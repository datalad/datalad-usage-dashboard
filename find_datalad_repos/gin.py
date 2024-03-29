from __future__ import annotations
from collections.abc import Iterator
from datetime import datetime
from typing import Any
from ghreq import Client, PrettyHTTPError, RetryConfig
from pydantic import BaseModel
from .tables import GIN_COLUMNS, Column, TableRow
from .util import USER_AGENT, Status, log


class GINDataladRepo(BaseModel):
    id: int
    name: str
    url: str
    stars: int
    status: Status
    updated: datetime | None = None

    @classmethod
    def from_data(cls, data: dict[str, Any]) -> GINDataladRepo:
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


class GINDataladSearcher(Client):
    def __init__(self, token: str) -> None:
        super().__init__(
            api_url="https://gin.g-node.org/api/v1",
            user_agent=USER_AGENT,
            accept=None,
            api_version=None,
            # Passing `token` directly to ghreq results in an Authorization
            # header of "Bearer {token}", which GIN doesn't seem to support.
            headers={"Authorization": f"token {token}"},
            # Don't retry on 500's until
            # <https://github.com/G-Node/gogs/issues/148> is resolved
            retry_config=RetryConfig(retry_statuses=range(501, 600)),
        )

    def search_repositories(self) -> Iterator[GINDataladRepo]:
        # TODO: Switch back to this simpler implementation (and remove the
        # custom RetryConfig above) once
        # <https://github.com/G-Node/gogs/issues/148> is resolved:
        ###
        # for datum in self.paginate("/repos/search"):
        #     yield GINDataladRepo.from_data(datum)
        ###
        page = 1
        while True:
            try:
                r = self.get("/repos/search", params={"page": page}, raw=True)
            except PrettyHTTPError as e:
                if e.response.status_code == 500:
                    log.warning(
                        "Request for page %d of GIN repository search results"
                        " returned %d; skipping page",
                        page,
                        e.response.status_code,
                    )
                else:
                    raise
            else:
                repos = [GINDataladRepo.from_data(datum) for datum in r.json()["data"]]
                if not repos:
                    break
                yield from repos
            page += 1

    def get_datalad_repos(self) -> Iterator[GINDataladRepo]:
        for repo in self.search_repositories():
            if self.has_datalad_config(repo.name):
                log.info("Found DataLad repo on GIN: %r (ID: %d)", repo.name, repo.id)
                yield repo
            else:
                log.debug(
                    "Found non-DataLad repo on GIN: %r (ID: %d); ignoring",
                    repo.name,
                    repo.id,
                )

    def has_datalad_config(self, repo: str) -> bool:
        try:
            self.request("HEAD", f"/repos/{repo}/raw/master/.datalad/config", raw=True)
        except PrettyHTTPError as e:
            if e.response.status_code == 404:
                return False
            else:
                raise e
        else:
            return True
