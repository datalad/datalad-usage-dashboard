<!--
Draft follow-up comment for https://github.com/CenterForOpenScience/osf.io/issues/11917
Post as a comment on that issue. All timestamps UTC, all evidence from public CI logs.
-->

Follow-up with more data — and a correction to my original report, which said this
had only happened once.

Going back through our CI logs, the same request has broken our nightly job on
**six of the last eleven days**. Only one of those was the 500 I reported; the
other five were 502s:

| Date (UTC) | Status | Time to response | Run |
|---|---|---|---|
| 2026-09-04 11:47 | 502 | — | [33864333205](https://github.com/datalad/datalad-usage-dashboard/actions/runs/33864333205) |
| 2026-09-07 12:44 | 502 | — | [34117854220](https://github.com/datalad/datalad-usage-dashboard/actions/runs/34117854220) |
| 2026-09-11 11:51 | 502 | — | [34590370964](https://github.com/datalad/datalad-usage-dashboard/actions/runs/34590370964) |
| 2026-09-12 11:13 | 502 | — | [34687715781](https://github.com/datalad/datalad-usage-dashboard/actions/runs/34687715781) |
| 2026-09-13 12:08 | 502 | **60.1 s** | [34753683613](https://github.com/datalad/datalad-usage-dashboard/actions/runs/34753683613) |
| 2026-09-14 12:56 | 500 | 0.17 s | [34840633123](https://github.com/datalad/datalad-usage-dashboard/actions/runs/34840633123) |

The request is always the same, and always the *first* page:

```
GET /v2/nodes/?filter%5Btags%5D=DataLad+Dataset&filter%5Bpublic%5D=true
```

The two statuses look like they come from different layers. The 500 is an nginx
page; the 502 is the Google frontend page:

```
<html><head><title>500 Internal Server Error</title></head>
<body><center><h1>500 Internal Server Error</h1></center><hr><center>nginx</center>
```

```
<h1>Error: Server Error</h1>
<h2>The server encountered a temporary error and could not complete your request.
<p>Please try again in 30 seconds.</h2>
```

## Why I think this is a query-performance problem, not a random fault

On 2026-09-13 the 502 came back **exactly 60 seconds** after the connection was
opened — 12:07:36 to 12:08:36 — which is the signature of an upstream backend
timeout rather than a transient error.

That fits what the *successful* runs look like. On 2026-09-10 the same query
paginated over 21 pages, and each page took 7–11 seconds
([run 34467449175](https://github.com/datalad/datalad-usage-dashboard/actions/runs/34467449175)):

```
11:54:41  page 19  200
11:54:52  page 20  200   (11 s)
11:54:59  page 21  200   (7 s)
```

So a page of ~10 nodes filtered by tag costs the better part of ten seconds even
on a good day. It seems plausible that under load the first page occasionally
crosses a 60-second limit and the load balancer returns 502.

## Questions

1. Is `filter[tags]` on `/v2/nodes/` backed by an index? Seven to eleven seconds
   for a page of ten suggests it might not be.
2. Is there a cheaper endpoint for "all public nodes with tag X"? We would rather
   use whatever is efficient on your side.
3. What is the maximum accepted `page[size]` on `/v2/nodes/`? We currently issue
   ~21 requests per night at the default page size; at `page[size]=100` that would
   be 3, which is less work for you as well as fewer chances to hit this.
4. Is there a retry/`Retry-After` policy you would like clients to honour? We are
   about to add backoff on 5xx and would rather match your expectations than guess.

For context on the load we generate: this is a single-threaded nightly job for
<https://github.com/datalad/datalad-usage-dashboard>, which feeds
<https://registry.datalad.org/>. It is one pass over that one query per day and
nothing else. Happy to reduce the frequency or change the access pattern if that
helps.
