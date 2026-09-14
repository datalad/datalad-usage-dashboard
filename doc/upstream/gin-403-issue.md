<!--
Draft issue for G-Node about the GIN API 403.

Where to file: G-Node/gogs is the tracker we already use for GIN API problems
(gin.py cites G-Node/gogs#148), but this 403 comes from a proxy in front of the
application rather than from Gogs itself, so infrastructure contact
(gin@g-node.org) may be the better first stop. Suggest opening on G-Node/gogs and
cc'ing support.

Title: API: GET /api/v1/repos/search intermittently returns HTML 403
       "Request forbidden by administrative rules."
-->

## Summary

Roughly half of our weekly runs against the GIN API fail on the very first
request with an HTTP 403 whose body is an HTML error page, not a GIN/Gogs API
response:

```
GET https://gin.g-node.org/api/v1/repos/search?page=1&private=false&is_private=false
-> 403 Forbidden

<html><body><h1>403 Forbidden</h1>
Request forbidden by administrative rules.
</body></html>
```

The request is authenticated (`Authorization: token …`). "Request forbidden by
administrative rules." is a reverse-proxy/WAF message, which is why I suspect
this is infrastructure rather than application behaviour.

## Frequency

7 of our last 15 weekly runs failed. Confirmed as this exact 403 in the logs of:

- 2026-08-22 12:21 UTC — [run 32572754950](https://github.com/datalad/datalad-usage-dashboard/actions/runs/32572754950)
- 2026-09-05 14:18 UTC — [run 33971421826](https://github.com/datalad/datalad-usage-dashboard/actions/runs/33971421826)

Several other failed runs have the same sub-minute signature but I have not
re-read each log. On the runs that succeed, the identical request works fine and
the whole job completes, so it is not a permanent block on our token.

## Client details

- Project: <https://github.com/datalad/datalad-usage-dashboard>, which indexes
  public DataLad datasets on GIN for <https://registry.datalad.org/>.
- Schedule: **once a week**, Saturdays, single-threaded, no concurrency.
- Client: python-requests via [`ghreq`](https://github.com/jwodder/ghreq).
- User-Agent:
  `find_datalad_repos (https://github.com/datalad/datalad-usage-dashboard) requests/<version> CPython/3.14.x`
- Source IPs: GitHub Actions hosted runners, so they differ on every run.
- Access pattern: paginate `/api/v1/repos/search`, then one contents check per
  candidate repository.

## Questions

1. Is the block keyed on User-Agent, on source IP, or on request rate?
2. If it is the User-Agent, is there a form you would prefer? We are happy to
   identify the job however is most useful to you.
3. If it is IP-based, GitHub Actions runners are the source and their addresses
   rotate — is there something we can do short of moving the job to a fixed host?
4. Is there a request rate or an alternative endpoint you would rather we use for
   enumerating public repositories?

We would much rather be a well-behaved client than keep retrying into a block, so
any guidance on the intended limits is welcome.

## Not the same as G-Node/gogs#148

That issue covers 500s on individual `/repos/search` pages, which we already work
around by skipping the page. This one is a 403 on the first request that aborts
the whole run.
