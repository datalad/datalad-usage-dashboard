from __future__ import annotations
from collections.abc import Iterator
from datetime import datetime
from operator import attrgetter
from typing import Any
from ghreq import Client, PrettyHTTPError, RetryConfig
from pydantic import BaseModel, Field
from .core import RepoHost, Searcher, Updater
from .tables import GIN_COLUMNS, Column, TableRow
from .util import USER_AGENT, Status, log


class GINRepo(BaseModel):
    id: int
    name: str
    url: str
    stars: int
    status: Status
    updated: datetime | None = None

    @classmethod
    def from_data(cls, data: dict[str, Any]) -> GINRepo:
        return cls(
            id=data["id"],
            name=data["full_name"],
            url=data["html_url"],
            stars=data["stars_count"],
            status=Status.ACTIVE,
            updated=data["updated_at"],
        )

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
            Column.LAST_MODIFIED: (
                str(self.updated) if self.updated is not None else "\u2014"
            ),
        }
        assert set(cells.keys()) == set(GIN_COLUMNS)
        qtys = {
            Column.REPOSITORY: 1,
            Column.STARS: self.stars,
        }
        assert set(qtys.keys()) == {col for col in GIN_COLUMNS if col.countable}
        return TableRow(cells=cells, qtys=qtys)


class GINSearcher(Client, Searcher[GINRepo]):
    def __init__(
        self, host: RepoHost, token: str | None = None, url: str | None = None
    ) -> None:
        if url is None:
            url = "https://gin.g-node.org"
        headers = {}
        if token is not None:
            # Passing `token` directly to ghreq results in an Authorization
            # header of "Bearer {token}", which GIN doesn't seem to support.
            headers["Authorization"] = f"token {token}"
        super().__init__(
            api_url=f"{url}/api/v1",
            user_agent=USER_AGENT,
            accept=None,
            api_version=None,
            headers=headers,
            # Don't retry on 500's until
            # <https://github.com/G-Node/gogs/issues/148> is resolved
            retry_config=RetryConfig(retry_statuses=range(501, 600)),
        )
        self.host = host

    def search_repositories(self) -> Iterator[dict[str, Any]]:
        # TODO: Switch back to this simpler implementation (and remove the
        # custom RetryConfig above) once
        # <https://github.com/G-Node/gogs/issues/148> is resolved:
        ###
        # for datum in self.paginate(
        #     "/repos/search", params={"private": "false", "is_private": "false"}
        # ):
        #     yield GINRepo.from_data(datum)
        ###
        page = 1
        while True:
            try:
                r = self.get(
                    "/repos/search",
                    # `private` and `is_private` are supported by the forgejo
                    # family (e.g., hub.datalad.org) but not GIN itself; for
                    # that, we filter on the "private" field below.
                    params={"page": page, "private": "false", "is_private": "false"},
                    raw=True,
                )
            except PrettyHTTPError as e:
                if e.response.status_code == 500:
                    log.warning(
                        "Request for page %d of %s repository search results"
                        " returned %d; skipping page",
                        page,
                        self.host.value,
                        e.response.status_code,
                    )
                else:
                    raise
            else:
                repos = r.json()["data"]
                if not repos:
                    break
                yield from repos
            page += 1

    def get_datalad_repos(self) -> Iterator[GINRepo]:
        for datum in self.search_repositories():
            if datum.get("private", False):
                continue
            repo = GINRepo.from_data(datum)
            if self.has_datalad_config(repo.name, datum["default_branch"]):
                log.info(
                    "Found DataLad repo on %s: %r (ID: %d)",
                    self.host.value,
                    repo.name,
                    repo.id,
                )
                yield repo
            else:
                log.debug(
                    "Found non-DataLad repo on %s: %r (ID: %d); ignoring",
                    self.host.value,
                    repo.name,
                    repo.id,
                )

    def has_datalad_config(self, repo: str, defbranch: str) -> bool:
        try:
            # forgejo instances like hub.datalad.org don't support HEAD
            # requests to this endpoint, so do a GET with a small range.
            self.get(
                f"/repos/{repo}/raw/{defbranch}/.datalad/config",
                raw=True,
                headers={"Range": "0-1"},
            )
        except PrettyHTTPError as e:
            if e.response.status_code in (404, 500):
                return False
            else:
                raise e
        else:
            return True


class GINUpdater(BaseModel, Updater[GINRepo, GINRepo, GINSearcher]):
    host: RepoHost
    all_repos: dict[int, GINRepo]
    seen: set[int] = Field(default_factory=set)
    new_repos: int = 0

    @classmethod
    def from_collection(cls, host: RepoHost, collection: list[GINRepo]) -> GINUpdater:
        return cls(host=host, all_repos={repo.id: repo for repo in collection})

    def get_searcher(self, **kwargs: Any) -> GINSearcher:
        return GINSearcher(host=self.host, **kwargs)

    def register_repo(self, repo: GINRepo, _searcher: GINSearcher) -> None:
        self.seen.add(repo.id)
        if repo.id not in self.all_repos:
            self.new_repos += 1
        self.all_repos[repo.id] = repo

    def get_new_collection(self, _searcher: GINSearcher) -> list[GINRepo]:
        collection: list[GINRepo] = []
        for repo in self.all_repos.values():
            if repo.id in self.seen:
                status = Status.ACTIVE
            else:
                status = Status.GONE
            collection.append(repo.model_copy(update={"status": status}))
        collection.sort(key=attrgetter("name"))
        return collection

    def get_reports(self) -> list[str]:
        if self.new_repos:
            return [f"{self.host.value}: {self.new_repos} new datasets"]
        else:
            return []
