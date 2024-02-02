from __future__ import annotations
from collections.abc import Iterator
from datetime import datetime
from operator import attrgetter
from time import sleep
from typing import Any, Optional
from ghreq import Client, PrettyHTTPError
from pydantic import BaseModel, Field
from .tables import GITHUB_COLUMNS, Column, TableRow
from .util import USER_AGENT, Status, check, is_container_run, log

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


class GHDataladSearcher(Client):
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
