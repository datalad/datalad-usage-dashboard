# Implementation Plan: Update-Workflow Reliability

Evidence: the 60 scheduled `update.yml` runs from 2026-07-17 to 2026-09-14, the
last 15 `update-gin.yml` runs, and the current `datalad-repos.json`.

## 1. What is failing

| Cause                                  | Runs                          | Evidence                                                |
|----------------------------------------|-------------------------------|---------------------------------------------------------|
| OSF `/v2/nodes/` 5xx on the first page | 6 (Sep 4, 7, 11, 12, 13, 14)  | 502 after exactly 60 s (Sep 13); 500 in 0.17 s (Sep 14) |
| 30-minute per-attempt timeout          | 3 (Aug 17, 29, 30)            | `Timeout of 1800000ms hit`                              |
| GitHub primary rate limit exhausted    | (Aug 29, above)               | `Primary rate limit exceeded` -> 755 s sleep -> timeout |
| GIN `/api/v1/repos/search` HTML 403    | 7 of 15 `update-gin.yml` runs | `Request forbidden by administrative rules.`            |

9/60 runs are red, but 7 more went green only on attempt 2, so **27% of runs had a
failed attempt**. Clean runs take 22-29 min against `timeout_minutes: 30`.

Two amplifiers turn small upstream faults into whole lost days:

- **No partial persistence.** `osf.py:73` calls `sys.exit(1)`; `__main__.py:76-90`
  writes `datalad-repos.json` only after every host succeeds. One OSF 502 discards
  the ~22-minute GitHub scan. No GitHub update has landed on master since Sep 10.
- **Whole-command retry.** `nick-fields/retry` re-runs the entire scan. A failed
  attempt costs ~1,900 core + ~100 search requests, so a retry spends ~3,800 core
  requests inside one 5,000/hr window — that *is* the Aug 29 rate-limit failure.
  It also sleeps `retry_wait_seconds` after the final attempt (900 s of dead
  runner time in every failing log).

## 2. PR A — stop losing work, stop double-scanning

**Implemented.** Order matters: the workflow changes are part of this commit, not a follow-up.
Without them the Python change is inert.

- `.github/workflows/update.yml`, `update-gin.yml`: **`if: always()` on the
  `Push changes` step.** It currently has no `if:`, so it defaults to `success()`
  and is skipped whenever the script exits non-zero — a partial commit would be
  made in the runner and discarded with it.
- `update.yml`: **drop the `nick-fields/retry` wrapper** for a plain `run:` with
  `timeout-minutes: 45`. Once failures are isolated it protects nothing and costs
  ~2,000 GitHub requests per failure. (Shortening `retry_wait_seconds` would make
  this *worse* — it lands the second attempt inside the same rate-limit hour.)
- `__main__.py`: run the five host updates in a loop of zero-arg callables, so the
  `os.environ[...]` / `get_ghtoken()` lookups happen inside the `try` and a missing
  token kills one host rather than all five. `except Exception` + `log.exception`,
  accumulate failed host names, **always** write the record and commit, then
  `sys.exit(1)` if anything failed. Name the skipped hosts in the commit message.
- `osf.py:73`: `raise RuntimeError(...)` instead of `sys.exit(1)`. No new exception
  type — every other host already fails with `ghreq.PrettyHTTPError` or
  `RuntimeError`, so a bespoke `HostError` would isolate only the one host that
  does not need it.
- `osf.py:70`: add `timeout=(10, 90)`. There is no timeout on the OSF session at
  all today; a hung connection burns the whole job budget.
- `osf.py`: **`page[size]` deliberately NOT set.** Measured 2026-09-14 by the
  maintainer against the live API: the query returns 502 after 60.198 s at
  `page[size]=100` and after 60.190 s at `page[size]=20`. Two conclusions. The
  60 s wall clock on both confirms the upstream backend timeout rather than a
  random fault, matching the 2026-09-13 CI log exactly. And a larger page is
  *more* server work per request, so raising it plausibly makes that timeout
  more likely, not less. The ~21-requests-per-run cost stays until OSF is
  healthy enough to measure; with per-host isolation an OSF failure is now
  cheap anyway.
- `github.py`: add `"per_page": "100"` to the org enumeration in
  `traverse_org_repositories` (github.py:312-372). `ghreq`'s
  `paginate()` sets no `per_page`, so this runs at GitHub's default of 30 —
  dandizarrs is ~197 pages, not 59, and the Aug 30 timeout hit at page 59, only
  ~30% through. One line, ~-220 requests and -3-5 min per run.

**The partial-failure invariant.** If a host raises, the
`collection[:] = ...` assignment at the end of `update_collection` never runs.
For OSF and GIN, which build a fresh list, the collection is genuinely
untouched. For GitHub it is *partially refreshed but never mass-GONE*:
`from_collection` stores the same `GitHubRepo` objects that live in
`record.github` and the refresh loop mutates them in place, so status/stars
stamps already made are banked. Every one of those is a real observation, so
this is benign — but it is not "untouched", and nothing should be built on
that assumption.

The invariant holds *only* while the `except` lives in `__main__`, outside
`update_collection()`. Catching inside it to "keep what OSF already found"
would mass-mark the un-fetched pages GONE — `osf.py` and `gin.py` mark
everything not in `seen` as gone. (`GitHubUpdater` has no such sweep; its
`seen` set is written and never read. It only marks gone what it re-checked.)

**Gone-flip ceiling.** Per-host isolation has one bad side effect that has to
be neutralised in the same change: today a GIN 403 aborts the process *before*
hub.datalad.org runs, which accidentally shields its 4,627 rows. Once hosts are
isolated, hub and ATRIS get swept on those runs too — and now the result is
committed. `update_collection` therefore refuses any update that would retire
more than `max(10, 5%)` of a host's active repos, raising so the host is
isolated and the rest of the run still commits. The floor of 10 sits above the
observed 5-9 GIN flap.

**Prerequisite in the same commit:** two separate paths in `gin.py` treat a
500 as evidence of absence and silently shrink `seen`, after which the sweep
marks those repos gone. This already flaps in the record (5-9 GIN repos
flipping to gone and back per commit, versus 1-2 genuine GitHub deletions), and
the per-repo path is the one that produces flaps of that size:

- the search-page handler skips the page — set `pages_skipped`;
- `has_datalad_config` folds 500 into 404 — make it return `bool | None` and
  record the repo id in `unresolved`.

`GINUpdater.get_new_collection` then keeps the previous status for anything
covered by either. Separately, the search-page handler had no bound: if every
page 500s the page counter climbs forever, and `update-gin.yml` had no timeout
at any level, so that was a six-hour hang rather than a failure. Bounded at
three consecutive failures, plus a job timeout.

**Test.** One, covering both invariants: seed a tmp record with 2 active OSF and 1
active GIN entry, make `OSFSearcher.get_datalad_repos` yield one then raise, run
`--hosts OSF,GIN`, assert exit code 1, both OSF entries still `active` (not
`gone`), the GIN update landed, and a commit exists. Enabling it needs
`tox.ini:49` fixed first — `addopts = --cov={{import_name}}` is an unrendered
cookiecutter placeholder, so `tox -e py3` fails instantly today.

## 3. PR B — stop re-fetching what we already downloaded

The refresh loop is the largest single consumer (1,000 of ~1,900 core requests per
run) and most of it is redundant *within the same run*.

`/orgs/{org}/repos` already returns `id`, `pushed_at` and `stargazers_count` for
every repo. `traverse_org_repositories` (`github.py:341-365`) discards all of it
and yields only `SearchHit(id, url, name)`; `register_repo` (`github.py:729-740`)
then deliberately keeps the *stale* values; and `get_new_collection`
(`github.py:747-763`) spends one `GET /repos/{name}` per repo re-fetching exactly
those two fields. Three orgs are configured `org_traverse` (OpenNeuroDatasets,
nemardatasets, dandisets = 3,489 repos); dandizarrs (5,439) reaches traversal
only through the auto-fallback, so the 8,928-of-12,470 figure below assumes the
`org_traverse` flip in this section also lands. These are the oldest entries, so
they dominate the oldest-first queue.

Carry `pushed_at`/`stars` from the enumeration through to `register_repo` and
stamp `last_checked`. ~15 lines. **Do not add fields to `SearchHit`** — it is
`frozen=True` and used both as a set member and as a dict key, so an extra field
makes the same repo hash differently depending on which search found it, and
`datasets | runcmds.keys()` then emits two `SearchResult`s for one repo. Put a
`dict[int, ExtraDetails]` on `GitHubSearcher` instead and read it in
`register_repo`, which already receives the searcher. The residual refresh set drops from 12,931 to
~3,542 — a full sweep every ~3.5 runs at the existing 1,000 cap, against today's
12.9-day sweep versus a 7-day promise (measured median staleness 11 days, max 19).
Gone-detection is unchanged: a repo that disappears from an org listing is not
stamped, ages past the cutoff, and gets its 404 in the refresh queue.

Also here, both one-liners:

- Skip `Status.GONE` in the refresh selection (`github.py:750-753`). 461 deleted
  repos are re-polled every sweep and 404 every time.
- Flip `dandizarrs` to `org_traverse` in `github-orgs.json`. It has 5,439 datasets
  and the Search API caps at 1,000 results, so `org:dandizarrs path:.datalad` can
  never enumerate it — the empty result and traversal fallback are the designed
  outcome, not a bug to investigate. One word removes two wasted search pages and
  an implicit failure mode.

## 4. Correctness bugs found during review

Not caused by this work; all verified against the code and the record.

1. **`nemardatasets` case mismatch.** `github-orgs.json` says `nemardatasets`, the
   repos are `nemarDatasets/*`, so `known_names` (`github.py:333-337`) is empty and
   `github.py:347` (`if known_names and ...`) falls through to yielding **every
   repo in the org as a DataLad dataset with no content check**. `nemarDatasets/.github`
   is in the record with `"dataset": true`. Fix the case comparison *and* drop the
   `known_names and` guard — as written, any org with an empty known set is
   ingested wholesale. Pair with commit 2: it adds ~790 content checks per run.
2. **Search queries exceed the documented length limit.** 14 excluded orgs produce
   a 269-char exclusion string; the code query is 308 chars and the commits query
   307, against GitHub's documented 256. `util.py:116` and
   `config.py:MAX_EXCLUSION_QUERY_LENGTH` both cap at 1,000 — the wrong number.
   This is a plausible cause of the intermittent search 404s that the
   `SEARCH_FLAKY_*` retry at `github.py:33-36` exists to paper over, and it grows
   with every new 30-repo org. Verify with one request before changing.
3. **~215 lines of unreachable code** (~350 including the `adhoc_tests/` files
   that exercise them). `needs_enumeration_fallback` returns `False` immediately
   unless `known_repos` is passed. Its caller `search_dataset_repos_in_org` does
   forward the parameter, but *its* only caller (`github.py:425`) never supplies
   it — so `process_enumerated_repos`,
   `enumerate_org_repositories` and `check_datalad_config` cannot run.
   `get_organizations_to_traverse`, `should_traverse` and
   `get_organizations_for_exclusion` have zero callers. The live path is the outer
   fallback at `github.py:428-443`. This dead twin is why the enumeration's
   throttling and `per_page` look correct at a glance — the copy that sets
   `per_page: 100` (`github.py:533`) is the one that never executes. Delete it.

## 5. Sequence

| PR  | Contents                                                                          | Risk   | Ship when                       |
|-----|-----------------------------------------------------------------------------------|--------|---------------------------------|
| A   | Section 2: isolation, workflows, gone-flip ceiling, GIN 500 handling, OSF, tests   | medium | now, alone                      |
| B   | Section 3: reuse the enumeration payload, skip GONE in refresh, dandizarrs flip    | medium | after A has run ~7 clean nights |
| C   | Section 4.1: `nemardatasets` case fix, plus a decision on the 788 existing rows    | high   | after B                         |
| D   | Section 4.2: search-query length — only after measuring, never speculatively       | high   | after evidence                  |
| E   | Section 4.3: delete the dead enumeration path and its adhoc tests                  | none   | any time, standalone            |
| F   | Section 7: GIN traffic reduction G0-G6                                             | —      | separate series                  |

B is held back from A deliberately: it changes the *provenance* of `stars` and
`updated` for ~8,900 rows, and landing that beside the first-ever partial-commit
behaviour would make any registry anomaly unattributable. C is not merely a code
fix — correcting the case stops new bad ingestion but does not retire the 788
rows already ingested unchecked, and GitHub has no sweep to retire them, so it
needs a data decision. D would *reduce* `MAX_EXCLUSION_QUERY_LENGTH`, pushing
orgs back into a global search that truncates at 1,000 results; that is a
coverage regression unless measured first.

### Deferred, with reasons

- **Log `total_count` per search.** Three log lines that turn the estimates here
  into measurements. Do it with B.
- **OSF retry adapter.** With A an OSF failure costs only the OSF table, and
  `page[size]=100` cuts exposure to 3 requests. Measure the real 5xx rate for a
  week, then size the backoff. When written: `raise_on_status=False` is required,
  or urllib3 raises `RetryError` and bypasses both the `not r.ok` diagnostic and
  the per-host handler; `respect_retry_after_header` is a no-op because
  `Retry.RETRY_AFTER_STATUS_CODES` is `{413, 429, 503}`; import `Retry` from
  `requests.adapters` since `urllib3` is not a declared dependency; and urllib3
  has no total-time budget, so the bound is `total x backoff_max` arithmetic.
- **Date-scoping the commits searches.** `"DATALAD RUNCMD" merge:false is:public`
  returns one hit per commit and is silently truncated at 1,000 today. An
  `author-date:>` window would cut most org searches to 1-2 pages *and* improve
  coverage. Needs the `total_count` logging first.
- **GraphQL batching.** Point cost is a non-issue (~130 points for the whole
  corpus), but it needs alias/index mapping for names containing `.` and `-`,
  correlation of `errors[]` against nulled `data`, and a retrier that understands
  HTTP 200 with an errors body. ~60-100 lines and a new failure surface to save
  requests that B removes outright. ETags on `get_extra_repo_details` are the
  ~10-line alternative if the residual still needs relief.
- **Surviving the step timeout.** A timeout SIGKILLs the process before the
  record is written, so 3 of the 9 historical failures stay total losses even
  after A. Fixing that means moving the write and commit inside the per-host
  loop, which changes commit granularity. Separate decision.
- **Failure notification.** Dropped. GitHub already emails the repo owner on a
  failed scheduled run.
- **`pydantic` v1 API migration.** `github_orgs.py` uses `@validator` and
  `github.py` uses `parse_obj`; `filterwarnings = error` in `tox.ini` needs a
  targeted ignore until these move to the v2 API.

## 6. Upstream

- `CenterForOpenScience/osf.io#11917` — follow-up ready at
  `doc/upstream/osf-11917-comment.md`. Corrects the "happened once" framing and
  adds the timing evidence that this is a query-performance problem.
- G-Node — draft at `doc/upstream/gin-403-issue.md`. **Do not file it yet**; see
  section 7.

## 7. Reduce our GIN/forgejo traffic before asking G-Node for anything

What the scan does today, per host, every run, for GIN + hub.datalad.org + ATRIS:

1. Paginate `/repos/search` over **every public repo on the instance**, not just
   the DataLad ones (`gin.py:90-117`).
2. For **every** repo returned, GET
   `/repos/{repo}/raw/{branch}/.datalad/config` (`gin.py:140-155`) — including
   repos we already know are datasets and repos we already know are not.

Nothing is cached between runs, and there is no throttling anywhere in `gin.py`
(contrast `github.py:25`, `INTER_SEARCH_DELAY = 10`).

Observed in the 2026-09-12 run (36 min of command time for the three hosts):

| Observation                                          | Evidence                                              |
|------------------------------------------------------|-------------------------------------------------------|
| No `limit` sent; server pages are small               | `/repos/search?page=N&private=false&is_private=false` |
| One content GET per enumerated repo, back to back     | ~130-450 ms apart, ~5-7 req/s sustained, no sleep     |
| `Range: 0-1` is ignored — full body returned         | responses are `200 63`, not `206` with 2 bytes        |
| A GIN 403 also kills hub.datalad.org and ATRIS        | `__main__.py:82-87` runs them in one process          |

The `Range` header is malformed: RFC 9110 requires a unit (`bytes=0-1`), and a
server must ignore a range unit it does not understand. The intent to keep the
request small is not being honoured.

Steady-state active records — a lower bound on the content GETs we issue per run,
since the scan also checks every *non*-DataLad public repo:

| Host            | Active | Gone |
|-----------------|-------:|-----:|
| hub.datalad.org |  4,627 |    3 |
| GIN             |    748 |  244 |
| ATRIS           |    113 |    3 |

### Order of work

| Step | Change                                                                     | Where                        | Size | Effect                                  |
|------|----------------------------------------------------------------------------|------------------------------|-----:|-----------------------------------------|
| G0   | Log per-host request and page counts                                       | `gin.py`                     |   ~3 | the numbers that go in the issue        |
| G1   | Send `limit=50` on `/repos/search`                                         | `gin.py:98`                  |    1 | up to ~5x fewer search pages            |
| G2   | Fix `Range: 0-1` -> `bytes=0-1`                                            | `gin.py:147`                 |    1 | 2-byte 206 instead of the whole file    |
| G3   | Throttle between requests                                                  | `gin.py`                     |   ~3 | bounds the burst rate the WAF sees      |
| G4   | Skip the content check for a known dataset whose `updated_at` is unchanged | `gin.py:119-138` + updater   |  ~15 | removes ~5,400 GETs/run at steady state |
| G5   | Persist a negative cache (repo id -> `updated_at` when last checked)       | `record.py` + `gin.py`       |  ~25 | removes most of what remains            |
| G6   | *Verify then use:* `sort=updated&order=desc` with early stop               | `gin.py:98`                  |  ~10 | full sweep becomes incremental          |

G4 is safe because removing `.datalad/config` bumps the repo's `updated_at`, so a
dataset that stops being one is still re-checked. G5 needs a staleness bound
(re-check anything not confirmed in ~90 days) in case `updated_at` does not move.
G6 is the structural win but depends on whether Gogs (GIN) and forgejo
(hub.datalad.org, ATRIS) both honour `sort`/`order` on `/repos/search` — test
against each instance before relying on it.

**Note:** the 2026-09-12 log shows ATRIS pagination terminating at page 6 with
~10 content checks while page 5 was consumed, which does not square with 113
active ATRIS records. That inconsistency is itself the argument for G0: we do not
currently know our own request volume, and we should not quote numbers at G-Node
that we have not measured.

### Then file

Run G0-G5 for two or three weekly cycles, fill the measured volume into the
"Client details" section of `doc/upstream/gin-403-issue.md`, and only then ask
G-Node whether the block is by user-agent, IP or rate. If the 403s stop once the
volume drops, there may be nothing to file at all.
