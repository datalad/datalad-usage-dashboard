from __future__ import annotations
from collections.abc import Iterator
from datetime import datetime, timedelta
import heapq
from operator import attrgetter
from time import sleep
from typing import Any, Dict, List, Optional, Set
from ghreq import Client, PrettyHTTPError
from pydantic import BaseModel, Field
from .core import Searcher, Updater
from .tables import GITHUB_COLUMNS, Column, TableRow
from .util import USER_AGENT, Status, check, is_container_run, log, nowutc

# Searching too fast can trigger abuse detection
INTER_SEARCH_DELAY = 10

# How long to wait after triggering abuse detection
POST_ABUSE_DELAY = 45


class GHSearchResult(BaseModel):
    id: int
    name: str
    url: str
    dataset: bool
    run: bool
    container_run: bool


class GHDataladRepo(BaseModel):
    id: int | None
    name: str
    url: str
    stars: int
    dataset: bool
    run: bool
    container_run: bool
    status: Status
    updated: datetime | None = None
    last_checked: datetime | None = None

    @property
    def owner(self) -> str:
        return self.name.partition("/")[0]

    @property
    def gone(self) -> bool:
        return self.status is Status.GONE

    def as_table_row(self) -> TableRow:
        cells = {
            Column.REPOSITORY: f"[{self.name}]({self.url})",
            Column.STARS: str(self.stars),
            Column.IS_DATASET: check(self.dataset),
            Column.IS_RUN: check(self.run),
            Column.IS_CONTAINERS_RUN: check(self.container_run),
            Column.LAST_MODIFIED: (
                str(self.updated) if self.updated is not None else "\u2014"
            ),
        }
        assert set(cells.keys()) == set(GITHUB_COLUMNS)
        qtys = {
            Column.REPOSITORY: 1,
            Column.STARS: self.stars,
            Column.IS_DATASET: int(self.dataset),
            Column.IS_RUN: int(self.run),
            Column.IS_CONTAINERS_RUN: int(self.container_run),
        }
        assert set(qtys.keys()) == {col for col in GITHUB_COLUMNS if col.countable}
        return TableRow(cells=cells, qtys=qtys)


class GHRepo(BaseModel, frozen=True):
    id: int
    url: str
    name: str

    @classmethod
    def from_repository(cls, data: dict[str, Any]) -> GHRepo:
        return cls(id=data["id"], url=data["html_url"], name=data["full_name"])


class ExtraDetails(BaseModel):
    id: int
    pushed_at: datetime
    stars: int = Field(alias="stargazers_count")


class GHDataladSearcher(Client, Searcher[GHSearchResult]):
    def __init__(self, token: str) -> None:
        super().__init__(token=token, user_agent=USER_AGENT)

    def search(self, resource_type: str, query: str) -> Iterator[Any]:
        url: str | None = f"/search/{resource_type}"
        params: Optional[dict[str, str]] = {"q": query}
        while url is not None:
            try:
                r = self.get(url, params=params, raw=True)
            except PrettyHTTPError as e:
                if (
                    e.response is not None
                    and e.response.status_code == 403
                    and "abuse detection" in e.response.text
                ):
                    log.warning(
                        "Abuse detection triggered; sleeping for %s seconds",
                        POST_ABUSE_DELAY,
                    )
                    sleep(POST_ABUSE_DELAY)
                    continue
                else:
                    raise
            data = r.json()
            if data["incomplete_results"]:
                log.warning("Search returned incomplete results due to timeout")
            yield from data["items"]
            url = r.links.get("next", {}).get("url")
            params = None
            if url is not None:
                sleep(INTER_SEARCH_DELAY)

    def search_dataset_repos(self) -> Iterator[GHRepo]:
        log.info("Searching for repositories with .datalad/config files")
        for hit in self.search("code", "path:.datalad filename:config fork:true"):
            repo = GHRepo.from_repository(hit["repository"])
            log.info("Found %s", repo.name)
            yield repo

    def search_runcmds(self) -> Iterator[tuple[GHRepo, bool]]:
        """Returns a generator of (ghrepo, is_container_run) pairs"""
        log.info('Searching for "DATALAD RUNCMD" commits')
        for hit in self.search("commits", '"DATALAD RUNCMD" merge:false is:public'):
            container_run = is_container_run(hit["commit"]["message"])
            repo = GHRepo.from_repository(hit["repository"])
            log.info(
                "Found commit %s in %s (container run: %s)",
                hit["sha"][:7],
                repo.name,
                container_run,
            )
            yield (repo, container_run)

    def get_extra_repo_details(self, repo_fullname: str) -> ExtraDetails | None:
        try:
            r = self.get(f"/repos/{repo_fullname}")
        except PrettyHTTPError as e:
            if e.response.status_code == 404:
                return None
            else:
                raise
        else:
            return ExtraDetails.parse_obj(r)

    def get_datalad_repos(self) -> list[GHSearchResult]:
        datasets = set(self.search_dataset_repos())
        runcmds: dict[GHRepo, bool] = {}
        for repo, container_run in self.search_runcmds():
            runcmds[repo] = container_run or runcmds.get(repo, False)
        results = []
        for repo in datasets | runcmds.keys():
            results.append(
                GHSearchResult(
                    id=repo.id,
                    url=repo.url,
                    name=repo.name,
                    dataset=repo in datasets,
                    run=repo in runcmds,
                    container_run=runcmds.get(repo, False),
                )
            )
        results.sort(key=attrgetter("name"))
        return results


class GHCollectionUpdater(
    BaseModel, Updater[GHDataladRepo, GHSearchResult, GHDataladSearcher]
):
    all_repos: Dict[int, GHDataladRepo]
    #: Repos that disappeared before we started tracking IDs
    noid_repos: List[GHDataladRepo]
    seen: Set[int] = Field(default_factory=set)
    new_hits: int = 0
    new_repos: int = 0
    new_runs: int = 0

    @classmethod
    def from_collection(cls, collection: list[GHDataladRepo]) -> GHCollectionUpdater:
        all_repos: dict[int, GHDataladRepo] = {}
        noid_repos: list[GHDataladRepo] = []
        for repo in collection:
            if repo.id is not None:
                all_repos[repo.id] = repo
            else:
                noid_repos.append(repo)
        return cls(all_repos=all_repos, noid_repos=noid_repos)

    def get_searcher(self, token: str | None) -> GHDataladSearcher:
        if token is None:
            raise TypeError("token required for GHNDataladSearcher")
        return GHDataladSearcher(token)

    def register_repo(self, sr: GHSearchResult, searcher: GHDataladSearcher) -> None:
        rid = sr.id
        assert rid is not None
        self.seen.add(rid)
        try:
            old_repo = self.all_repos[rid]
        except KeyError:
            self.new_hits += 1
            self.new_repos += 1
            if sr.run:
                self.new_runs += 1
            extra = searcher.get_extra_repo_details(sr.name)
            if extra is None or extra.id != sr.id:
                raise RuntimeError(
                    f"GitHub repository {sr.name} suddenly disappeared after"
                    " being returned in a search!"
                )
            else:
                repo = GHDataladRepo(
                    id=sr.id,
                    name=sr.name,
                    url=sr.url,
                    stars=extra.stars,
                    dataset=sr.dataset,
                    run=sr.run,
                    container_run=sr.container_run,
                    status=Status.ACTIVE,
                    updated=extra.pushed_at,
                    last_checked=nowutc(),
                )
        else:
            if not old_repo.run and sr.run:
                self.new_hits += 1
                self.new_runs += 1
            repo = GHDataladRepo(
                id=sr.id,
                name=sr.name,
                url=sr.url,
                stars=old_repo.stars,
                dataset=old_repo.dataset or sr.dataset,
                run=old_repo.run or sr.run,
                container_run=old_repo.container_run or sr.container_run,
                status=Status.ACTIVE,
                updated=old_repo.updated,
                last_checked=old_repo.last_checked,
            )
        self.all_repos[rid] = repo

    def get_new_collection(self, searcher: GHDataladSearcher) -> list[GHDataladRepo]:
        collection: list[GHDataladRepo] = list(self.noid_repos)
        replaced: set[int] = set()
        check_cutoff = nowutc() - timedelta(days=7)
        needs_check = heapq.nsmallest(
            1000,
            (
                r
                for r in self.all_repos.values()
                if r.last_checked is None or r.last_checked < check_cutoff
            ),
            key=lambda r: (r.last_checked is not None, r.last_checked),
        )
        for repo in needs_check:
            log.info(
                "Getting latest details for repository %s (last checked: %s)",
                repo.name,
                repo.last_checked,
            )
            try:
                extra = searcher.get_extra_repo_details(repo.name)
            except PrettyHTTPError as e:
                if e.response.status_code == 404 and "rate limit" in e.response.text:
                    log.info(
                        "Hit a GitHub rate limit; not checking any more repositories"
                    )
                    break
                else:
                    raise
            else:
                if extra is None:
                    log.info("Repository %s no longer exists", repo.name)
                    repo.status = Status.GONE
                elif repo.id is not None and extra.id != repo.id:
                    log.info(
                        "Repository %s with ID %d has been replaced; deleting",
                        repo.name,
                        repo.id,
                    )
                    replaced.add(repo.id)
                else:
                    repo.status = Status.ACTIVE
                    repo.stars = extra.stars
                    repo.updated = extra.pushed_at
                repo.last_checked = nowutc()
        collection.extend(
            repo
            for repo in self.all_repos.values()
            if not (repo.id is not None and repo.id in replaced)
        )
        collection.sort(key=attrgetter("name"))
        return collection

    def get_reports(self) -> list[str]:
        news = (
            f"{self.new_repos} new datasets",
            f"{self.new_runs} new `datalad run` users",
        )
        if self.new_hits:
            return [
                f"GitHub: {self.new_hits} new hits: "
                + " and ".join(n for n in news if not n.startswith("0 "))
            ]
        else:
            return []
