from __future__ import annotations
from collections.abc import Iterator
import sys
from types import TracebackType
from typing import Any, Optional
from pydantic import BaseModel
import requests
from .util import USER_AGENT, Status, log


class OSFDataladRepo(BaseModel):
    url: str
    id: str
    name: str
    status: Status

    @classmethod
    def from_data(cls, data: dict[str, Any]) -> "OSFDataladRepo":
        return cls(
            url=data["links"]["html"],
            id=data["id"],
            name=data["attributes"]["title"],
            status=Status.ACTIVE,
        )

    @property
    def gone(self) -> bool:
        return self.status is Status.GONE


class OSFDataladSearcher:
    API_URL = "https://api.osf.io/v2"

    def __init__(self) -> None:
        self.session = requests.Session()
        # self.session.headers["Authorization"] = f"token {token}"
        self.session.headers["User-Agent"] = USER_AGENT

    def __enter__(self) -> "OSFDataladSearcher":
        return self

    def __exit__(
        self,
        _exc_type: Optional[type[BaseException]],
        _exc_val: Optional[BaseException],
        _exc_tb: Optional[TracebackType],
    ) -> None:
        self.session.close()

    def paginate(self, url: str, params: Optional[dict[str, str]] = None) -> Iterator:
        while url is not None:
            r = self.session.get(url, params=params)
            if not r.ok:
                log.error("Request to %s returned %d: %s", r.url, r.status_code, r.text)
                sys.exit(1)
            data = r.json()
            yield from data["data"]
            url = data.get("links", {}).get("next")
            params = None

    def get_datalad_repos(self) -> Iterator[OSFDataladRepo]:
        for hit in self.paginate(
            f"{self.API_URL}/nodes/",
            params={"filter[tags]": "DataLad Dataset", "filter[public]": "true"},
        ):
            repo = OSFDataladRepo.from_data(hit)
            log.info("Found OSF repo %r (ID: %s)", repo.name, repo.id)
            yield repo
