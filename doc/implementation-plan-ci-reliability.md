# Implementation Plan: Update-Workflow Reliability

Evidence base: the 60 scheduled `update.yml` runs from 2026-07-17 to 2026-09-14,
plus the last 15 `update-gin.yml` runs.

## 1. Measured failure modes

| # | Cause | Runs affected | Evidence |
|---|-------|---------------|----------|
| 1 | OSF `/v2/nodes/` returns 5xx on the first page | 6 (Sep 4, 7, 11, 12, 13, 14) | 502 after exactly 60 s (Sep 11-13); 500 in 0.17 s (Sep 14) |
| 2 | 30-minute per-attempt timeout | 3 (Aug 17, 29, 30) | `Final attempt failed. Timeout of 1800000ms hit` |
| 3 | GitHub primary rate limit exhausted | 1 (Aug 29, counted above) | `Primary rate limit exceeded; waiting for reset` -> 755 s sleep -> timeout |
| 4 | GIN `/api/v1/repos/search` returns HTML 403 | 7 of 15 `update-gin.yml` runs | `403 Forbidden ... Request forbidden by administrative rules.` |

Job-level failure rate is 9/60 (15%), but 7 further runs went green only on the
second attempt (`Command completed after 2 attempt(s).`), so **27% of runs had at
least one failed attempt**.

Two amplifiers make each of those failures much more expensive than it should be:

- **No partial persistence.** `OSFSearcher.paginate()` calls `sys.exit(1)`
  (`src/find_datalad_repos/osf.py:70-72`) and `__main__.py:76-86` writes
  `datalad-repos.json` only after every host finishes. A single OSF 502 discards
  the ~22-minute GitHub scan that just completed. `origin/master` has had no
  GitHub update since Sep 10 for exactly this reason.
- **Blind whole-command retry.** `nick-fields/retry` re-runs the entire scan,
  re-spending the GitHub rate-limit budget inside the same hour, and it sleeps
  `retry_wait_seconds` even after the final attempt (900 s of dead runner time in
  every failing log).

Clean runs take 22-29 min (Jul median 24 -> Sep median 26) against
`timeout_minutes: 30`.

## 2. Fixes

### A. Per-host failure isolation

Each host update runs independently; a failure records the host and lets the rest
of the run finish. The record file is written and committed with whatever
succeeded, then the process exits non-zero so CI still goes red.

- `osf.py`: replace `sys.exit(1)` with `raise` of an exception type shared by all
  searchers (e.g. `HostError` in `core.py`).
- `__main__.py`: drive the per-host calls through a loop; catch `HostError`,
  accumulate failed hosts, always write the record + commit, `sys.exit(1)` at the
  end if any host failed.
- Commit message must state what was skipped, so a partial commit is not mistaken
  for a complete run.

Hosts whose update failed must keep their previous collection untouched — in
particular the `Status.GONE` sweep in each updater's `get_new_collection()` must
not run on a partial result set, or a transient outage would mass-mark repos
gone.

### B. Retry OSF 5xx in-process

Mount a `requests.adapters.HTTPAdapter` on the OSF session with
`urllib3.util.Retry(total=5, status_forcelist=(500, 502, 503, 504),
allowed_methods=("GET",), backoff_factor=..., respect_retry_after_header=True)`.

The observed 502 arrives after a 60-second upstream timeout, so the backoff has to
be in the tens of seconds (~30/60/120), not the sub-second default. Cap the total
added wall time so this cannot push the job into the timeout.

### C. Larger OSF pages

Add `page[size]=100` to the `/v2/nodes/` query. OSF's default is 10, so 287 known
datasets cost ~29 requests per run today; at 100/page it is 3. Fewer requests is
both less exposure to their 5xx and less load on them.

**Unverified:** `api.osf.io` is unreachable from the dev sandbox. Confirm the
accepted maximum page size against the live API before relying on it, and keep the
`links.next` pagination loop so a smaller server-side cap degrades gracefully.

### D. Workflow knobs

- `timeout_minutes: 30` -> `50`. Current headroom is ~1 minute and shrinking.
- `retry_wait_seconds: 900` -> `300`; with A+B in place the whole-command retry is
  a backstop, not the primary recovery path.
- Once A+B are merged and observed for ~2 weeks, consider dropping
  `nick-fields/retry` entirely: retrying a 25-minute scan to recover from a
  3-second OSF error is the wrong granularity and re-burns the API budget.

## 3. Further work, not in this change

Ordered by expected value.

1. **GraphQL batching for the refresh loop.** 12,931 GitHub repos, refreshed at
   most 1000/run (`github.py:746-753`), one REST call each. 8,913 entries are
   already past the 7-day cutoff; median staleness 11 days, max 19 — the freshness
   target in the code is not being met, and a full sweep takes ~13 days. One
   GraphQL request can carry ~100 aliased `repository(owner:,name:)` lookups for
   `pushedAt`/`stargazerCount`. Measure the point cost before committing.
2. **`dandizarrs` enumeration fallback.** The org is configured `org_search`, but
   its search returns 0 hits, so `traverse_org_repositories()` enumerates ~5,900
   repos (59 pages) every run — the direct cause of the Aug 30 timeout. Find out
   why the search is empty rather than paying for the fallback daily.
3. **Split GitHub and OSF into separate jobs.** OSF is 3-30 cheap requests; GitHub
   is 25 minutes. Sharing a timeout and a failure domain buys nothing. (A makes
   this optional rather than urgent.)
4. **GIN 403.** `gin.py` tolerates 500 (skip page) and retries 501-599; the HTML
   403 propagates and kills the run in 1.5 s. Needs upstream clarification from
   G-Node on whether the block is by user-agent or by runner IP before we decide
   between a retry, a backoff, or a contact-and-allowlist. `update-gin.yml` has no
   retry wrapper at all.
5. **Failure notification.** Nothing reports a red scheduled run today. A
   `if: failure()` step that opens or updates a tracking issue is a cheap partial
   for issue #12.

## 4. Upstream reports

- `CenterForOpenScience/osf.io#11917` — needs a follow-up comment: the issue
  currently describes a single 500, but the same query also returned 502 after
  exactly 60 s on five other days. Draft: `doc/upstream/osf-11917-comment.md`.
- G-Node — new issue for the `repos/search` 403. Draft:
  `doc/upstream/gin-403-issue.md`.
