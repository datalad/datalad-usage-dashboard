from datetime import datetime, timezone
from operator import attrgetter
import os
import subprocess
import sys
from time import sleep
from types import TracebackType
from typing import Any, Dict, Iterator, List, Optional, Tuple, Type, cast
from pydantic import BaseModel
import requests
from .config import OURSELVES
from .tables import TableRow
from .util import USER_AGENT, Statistics, Status, check, is_container_run, log

# Searching too fast can trigger abuse detection
INTER_SEARCH_DELAY = 10

# How long to wait after triggering abuse detection
POST_ABUSE_DELAY = 45


class GHDataladRepo(TableRow):
    name: str
    url: str
    stars: int
    dataset: bool
    run: bool
    container_run: bool
    status: Status

    @property
    def owner(self) -> str:
        return self.name.partition("/")[0]

    @property
    def ours(self) -> bool:
        return self.owner in OURSELVES

    @property
    def gone(self) -> bool:
        return self.status is Status.GONE

    def get_cells(self) -> List[str]:
        return [
            f"[{self.name}]({self.url})",
            str(self.stars),
            check(self.dataset),
            check(self.run),
            check(self.container_run),
        ]

    def get_qtys(self) -> Statistics:
        return Statistics(
            1, self.stars, int(self.dataset), int(self.run), int(self.container_run)
        )


class GHRepo(BaseModel):
    url: str
    name: str

    class Config:
        frozen = True

    @classmethod
    def from_repository(cls, data: Dict[str, Any]) -> "GHRepo":
        return cls(url=data["html_url"], name=data["full_name"])


class GHDataladSearcher:
    API_URL = "https://api.github.com"

    def __init__(self, token: str) -> None:
        self.session = requests.Session()
        self.session.headers["Accept"] = ",".join(
            [
                "application/vnd.github.cloak-preview+json",  # Commit search
                "application/vnd.github.v3+json",
            ]
        )
        self.session.headers["Authorization"] = f"token {token}"
        self.session.headers["User-Agent"] = USER_AGENT

    def __enter__(self) -> "GHDataladSearcher":
        return self

    def __exit__(
        self,
        _exc_type: Optional[Type[BaseException]],
        _exc_val: Optional[BaseException],
        _exc_tb: Optional[TracebackType],
    ) -> None:
        self.session.close()

    def search(self, resource_type: str, query: str) -> Iterator[Any]:
        url = f"{self.API_URL}/search/{resource_type}"
        params: Optional[Dict[str, str]] = {"q": query}
        while url is not None:
            r = self.session.get(url, params=params)
            data = r.json()
            if r.status_code == 403 and "API rate limit exceeded" in data.get(
                "message", ""
            ):
                reset_time = datetime.fromtimestamp(
                    int(r.headers["x-ratelimit-reset"]), tz=timezone.utc
                )
                # Take `max()` just in case we're right up against the reset
                # time, and add 1 because `sleep()` isn't always exactly
                # accurate
                delay = (
                    max((reset_time - datetime.now().astimezone()).total_seconds(), 0)
                    + 1
                )
                log.info("Search rate limit exceeded; sleeping for %s seconds", delay)
                sleep(delay)
                continue
            elif r.status_code == 403 and "abuse detection" in data.get("message", ""):
                log.warning(
                    "Abuse detection triggered; sleeping for %s seconds",
                    POST_ABUSE_DELAY,
                )
                sleep(POST_ABUSE_DELAY)
                continue
            if not r.ok:
                log.error("Request to %s returned %d: %s", r.url, r.status_code, r.text)
                sys.exit(1)
            # r.raise_for_status()
            if data["incomplete_results"]:
                log.warning("Search returned incomplete results due to timeout")
            yield from data["items"]
            url = r.links.get("next", {}).get("url")
            params = None
            if url is not None:
                sleep(INTER_SEARCH_DELAY)

    def search_dataset_repos(self) -> Iterator[GHRepo]:
        log.info("Searching for repositories with .datalad/config files")
        for hit in self.search("code", "path:.datalad filename:config"):
            repo = GHRepo.from_repository(hit["repository"])
            log.info("Found %s", repo.name)
            yield repo

    def search_runcmds(self) -> Iterator[Tuple[GHRepo, bool]]:
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

    def get_repo_stars(self, repo: GHRepo) -> int:
        r = self.session.get(f"{self.API_URL}/repos/{repo.name}")
        r.raise_for_status()
        return cast(int, r.json()["stargazers_count"])

    def get_datalad_repos(self) -> List[GHDataladRepo]:
        datasets = set(self.search_dataset_repos())
        runcmds: Dict[GHRepo, bool] = {}
        for repo, container_run in self.search_runcmds():
            runcmds[repo] = container_run or runcmds.get(repo, False)
        results = []
        for repo in datasets | runcmds.keys():
            results.append(
                GHDataladRepo(
                    url=repo.url,
                    name=repo.name,
                    stars=self.get_repo_stars(repo),
                    dataset=repo in datasets,
                    run=repo in runcmds,
                    container_run=runcmds.get(repo, False),
                    status=Status.ACTIVE,
                )
            )
        results.sort(key=attrgetter("name"))
        return results

    def repo_exists(self, repo_fullname: str) -> bool:
        r = self.session.get(f"{self.API_URL}/repos/{repo_fullname}")
        if r.ok:
            return True
        elif r.status_code in (403, 404):
            return False
        else:
            r.raise_for_status()
            raise AssertionError("Unreachable")


def get_github_token() -> str:
    token = os.environ.get("GITHUB_TOKEN")
    if not token:
        r = subprocess.run(
            ["git", "config", "hub.oauthtoken"],
            stdout=subprocess.PIPE,
            universal_newlines=True,
        )
        if r.returncode != 0 or not r.stdout.strip():
            raise RuntimeError(
                "GitHub OAuth token not set.  Set via GITHUB_TOKEN"
                " environment variable or hub.oauthtoken Git config option."
            )
        token = r.stdout.strip()
    return token
