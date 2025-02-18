from __future__ import annotations
from collections.abc import Iterator
from datetime import datetime
from operator import attrgetter
import sys
from types import TracebackType
from typing import Any
from pydantic import BaseModel, Field
import requests
from .core import Searcher, Updater
from .tables import OSF_COLUMNS, Column, TableRow
from .util import USER_AGENT, Status, log


class OSFRepo(BaseModel):
    url: str
    id: str
    name: str
    status: Status
    updated: datetime | None = None

    @classmethod
    def from_data(cls, data: dict[str, Any]) -> OSFRepo:
        return cls(
            url=data["links"]["html"],
            id=data["id"],
            name=data["attributes"]["title"],
            status=Status.ACTIVE,
            updated=data["attributes"]["date_modified"],
        )

    @property
    def gone(self) -> bool:
        return self.status is Status.GONE

    def as_table_row(self) -> TableRow:
        cells = {
            Column.REPOSITORY: f"[{self.name}]({self.url})",
            Column.LAST_MODIFIED: (
                str(self.updated) if self.updated is not None else "\u2014"
            ),
        }
        assert set(cells.keys()) == set(OSF_COLUMNS)
        qtys = {Column.REPOSITORY: 1}
        assert set(qtys.keys()) == {col for col in OSF_COLUMNS if col.countable}
        return TableRow(cells=cells, qtys=qtys)


class OSFSearcher(Searcher[OSFRepo]):
    API_URL = "https://api.osf.io/v2"

    def __init__(self) -> None:
        self.session = requests.Session()
        # self.session.headers["Authorization"] = f"token {token}"
        self.session.headers["User-Agent"] = USER_AGENT

    def __enter__(self) -> OSFSearcher:
        return self

    def __exit__(
        self,
        _exc_type: type[BaseException] | None,
        _exc_val: BaseException | None,
        _exc_tb: TracebackType | None,
    ) -> None:
        self.session.close()

    def paginate(self, url: str, params: dict[str, str] | None = None) -> Iterator:
        while url is not None:
            r = self.session.get(url, params=params)
            if not r.ok:
                log.error("Request to %s returned %d: %s", r.url, r.status_code, r.text)
                sys.exit(1)
            data = r.json()
            yield from data["data"]
            url = data.get("links", {}).get("next")
            params = None

    def get_datalad_repos(self) -> Iterator[OSFRepo]:
        for hit in self.paginate(
            f"{self.API_URL}/nodes/",
            params={"filter[tags]": "DataLad Dataset", "filter[public]": "true"},
        ):
            repo = OSFRepo.from_data(hit)
            log.info("Found OSF repo %r (ID: %s)", repo.name, repo.id)
            yield repo


class OSFUpdater(BaseModel, Updater[OSFRepo, OSFRepo, OSFSearcher]):
    all_repos: dict[str, OSFRepo]
    seen: set[str] = Field(default_factory=set)
    new_repos: int = 0

    @classmethod
    def from_collection(cls, collection: list[OSFRepo]) -> OSFUpdater:
        return cls(all_repos={repo.id: repo for repo in collection})

    def get_searcher(self, **_kwargs: Any) -> OSFSearcher:
        return OSFSearcher()

    def register_repo(self, repo: OSFRepo, _searcher: OSFSearcher) -> None:
        self.seen.add(repo.id)
        if repo.id not in self.all_repos:
            self.new_repos += 1
        self.all_repos[repo.id] = repo

    def get_new_collection(self, _searcher: OSFSearcher) -> list[OSFRepo]:
        collection: list[OSFRepo] = []
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
            return [f"OSF: {self.new_repos} new datasets"]
        else:
            return []
