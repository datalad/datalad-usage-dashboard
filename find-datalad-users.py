from datetime import datetime, timezone
import json
import logging
from operator import attrgetter
import os
import platform
import re
import shlex
import subprocess
import sys
from time import sleep
from types import TracebackType
from typing import Any, Dict, Iterator, List, Optional, Set, Tuple, Type, cast
import click
from click_loglevel import LogLevel
from pydantic import BaseModel, Field
import requests

# Searching too fast can trigger abuse detection
INTER_SEARCH_DELAY = 10

# How long to wait after triggering abuse detection
POST_ABUSE_DELAY = 45

OURSELVES = {"datalad"}

RECORD_FILE = "datalad-repos.json"

TABLE_HEADER = (
    "| Repository | Stars | Dataset | `run` | `containers-run`\n"
    "| ----- | ----- | ----- | ----- | ----- |"
)

USER_AGENT = "find-datalad-users ({}) requests/{} {}/{}".format(
    "https://github.com/datalad/datalad-usage-dashboard",
    requests.__version__,
    platform.python_implementation(),
    platform.python_version(),
)

log = logging.getLogger(__name__)


class DataladRepo(BaseModel):
    name: str
    url: str
    ours: bool
    stars: int
    dataset: bool
    run: bool
    container_run: bool

    def as_table_row(self) -> str:
        return (
            f"| [{self.name}](self.url) | {self.stars} | {check(self.dataset)} "
            f"| {check(self.run)} | {check(self.container_run)} |"
        )


class GHRepo(BaseModel):
    owner: str
    url: str
    name: str

    class Config:
        frozen = True

    @classmethod
    def from_repository(cls, data: Dict[str, Any]) -> "GHRepo":
        return cls(
            url=data["html_url"], owner=data["owner"]["login"], name=data["full_name"]
        )


class RepoCollection(BaseModel):
    active: List[DataladRepo] = Field(default_factory=list)
    gone: List[DataladRepo] = Field(default_factory=list)


class RepoRecord(BaseModel):
    github: RepoCollection = Field(default_factory=RepoCollection)


class CollectionUpdater(BaseModel):
    all_repos: Dict[str, DataladRepo]
    seen: Set[str] = Field(default_factory=set)
    new_hits: int = 0
    new_repos: int = 0
    new_runs: int = 0

    @classmethod
    def from_collection(cls, collection: RepoCollection) -> "CollectionUpdater":
        all_repos = {repo.name: repo for repo in collection.active}
        for repo in collection.gone:
            all_repos[repo.name] = repo
        return cls(all_repos=all_repos)

    def register_repo(self, repo: DataladRepo) -> None:
        self.seen.add(repo.name)
        try:
            old_repo = self.all_repos[repo.name]
        except KeyError:
            self.new_hits += 1
            self.new_repos += 1
            if repo.run:
                self.new_runs += 1
        else:
            if not old_repo.run and repo.run:
                self.new_hits += 1
                self.new_runs += 1
        self.all_repos[repo.name] = repo

    def get_new_collection(self) -> RepoCollection:
        active: List[DataladRepo] = []
        gone: List[DataladRepo] = []
        for repo in self.all_repos.values():
            if repo.name in self.seen:
                active.append(repo)
            else:
                gone.append(repo)
        active.sort(key=attrgetter("name"))
        gone.sort(key=attrgetter("name"))
        return RepoCollection(active=active, gone=gone)

    def get_report(self) -> str:
        return (
            f"Added {self.new_hits} new hits: {self.new_repos} new datasets"
            f" and {self.new_runs} new `datalad run` users"
        )


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
            repo = hit["repository"]
            log.info("Found %s", repo["full_name"])
            yield GHRepo.from_repository(repo)

    def search_runcmds(self) -> Iterator[Tuple[GHRepo, bool]]:
        """Returns a generator of (ghrepo, is_container_run) pairs"""
        log.info('Searching for "DATALAD RUNCMD" commits')
        for hit in self.search("commits", '"DATALAD RUNCMD" merge:false is:public'):
            container_run = is_container_run(hit["commit"]["message"])
            repo = hit["repository"]
            log.info("Found %s (container run: %s)", repo["full_name"], container_run)
            yield (GHRepo.from_repository(repo), container_run)

    def get_repo_stars(self, repo: GHRepo) -> int:
        r = self.session.get(f"{self.API_URL}/repos/{repo.name}")
        r.raise_for_status()
        return cast(int, r.json()["stargazers_count"])

    def get_datalad_repos(self) -> List[DataladRepo]:
        datasets = set(self.search_dataset_repos())
        runcmds: Dict[GHRepo, bool] = {}
        for repo, container_run in self.search_runcmds():
            runcmds[repo] = container_run or runcmds.get(repo, False)
        results = []
        for repo in datasets | runcmds.keys():
            results.append(
                DataladRepo(
                    url=repo.url,
                    name=repo.name,
                    ours=repo.owner in OURSELVES,
                    stars=self.get_repo_stars(repo),
                    dataset=repo in datasets,
                    run=repo in runcmds,
                    container_run=runcmds.get(repo, False),
                )
            )
        results.sort(key=attrgetter("name"))
        return results


@click.command()
@click.option(
    "-l",
    "--log-level",
    type=LogLevel(),
    default=logging.INFO,
    help="Set logging level  [default: INFO]",
)
@click.option(
    "-R",
    "--regen-readme",
    is_flag=True,
    help="Regenerate the README from the JSON file without querying GitHub",
)
def main(log_level, regen_readme):
    logging.basicConfig(
        format="%(asctime)s [%(levelname)-8s] %(name)s %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S%z",
        level=log_level,
    )

    try:
        record = RepoRecord.parse_file(RECORD_FILE)
    except FileNotFoundError:
        record = RepoRecord()

    if not regen_readme:
        updater = CollectionUpdater.from_collection(record.github)
        with GHDataladSearcher(get_github_token()) as searcher:
            for repo in searcher.get_datalad_repos():
                updater.register_repo(repo)
        record.github = updater.get_new_collection()
        with open(RECORD_FILE, "w") as fp:
            print(record.json(indent=4), file=fp)

    wild_repos: List[DataladRepo] = []
    our_repos: List[DataladRepo] = []
    for repo in record.github.active:
        if repo.ours:
            our_repos.append(repo)
        else:
            wild_repos.append(repo)

    with open("README.md", "w") as fp:
        for header, repo_list in [
            ("In the wild", wild_repos),
            ("Inner circle", our_repos),
            ("Gone", record.github.gone),
        ]:
            print("#", header, file=fp)
            if repo_list:
                print(TABLE_HEADER, file=fp)
                for repo in repo_list:
                    print(repo.as_table_row(), file=fp)
            else:
                print("No repositories found!", file=fp)
            print(file=fp)

    if not regen_readme:
        runcmd("git", "add", RECORD_FILE, "README.md")
        commit(updater.get_report())


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


def is_container_run(commit_msg: str) -> bool:
    m = re.search(
        r"^=== Do not change lines below ===$(.+)"
        r"^\^\^\^ Do not change lines above \^\^\^$",
        commit_msg,
        flags=re.M | re.S,
    )
    if m is None:
        return False
    try:
        metadata = json.loads(m[1])
    except ValueError:
        return False
    try:
        return bool(metadata.get("extra_inputs"))
    except AttributeError:
        # Apparently there are some commits with strings for the RUNCMD
        # metadata?
        return False


def check(yesno: bool) -> str:
    # return '\u2714\uFE0F' if yesno else ''
    return ":heavy_check_mark:" if yesno else ""


def runcmd(*args, **kwargs):
    log.debug("Running: %s", " ".join(shlex.quote(str(a)) for a in args))
    r = subprocess.run(args, **kwargs)
    if r.returncode != 0:
        sys.exit(r.returncode)


def commit(msg):
    if subprocess.run(["git", "diff", "--cached", "--quiet"]).returncode != 0:
        runcmd("git", "commit", "-m", msg)
    else:
        log.info("Nothing to commit")


if __name__ == "__main__":
    main()
