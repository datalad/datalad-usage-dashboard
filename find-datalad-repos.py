from abc import ABC, abstractmethod
from collections import defaultdict
from datetime import datetime, timezone
from enum import Enum
from functools import reduce
import json
import logging
from operator import add, attrgetter
import os
from pathlib import Path
import platform
import re
import shlex
import subprocess
import sys
from time import sleep
from types import TracebackType
from typing import (
    Any,
    ClassVar,
    Dict,
    Iterable,
    Iterator,
    List,
    Mapping,
    NamedTuple,
    Optional,
    Set,
    Tuple,
    Type,
    Union,
    cast,
)
import click
from click_loglevel import LogLevel
from pydantic import BaseModel, Field
import requests

# Searching too fast can trigger abuse detection
INTER_SEARCH_DELAY = 10

# How long to wait after triggering abuse detection
POST_ABUSE_DELAY = 45

OURSELVES = {
    "con",
    "dandi",
    "dandisets",
    "datalad",
    "datalad-collection-1",
    "datalad-datasets",
    "datalad-handbook",
    "datalad-tester",
    "dbic",
    "jsheunis",
    "jwodder",
    "loj",
    "mih",
    "myyoda",
    "proj-nuisance",
    "psychoinformatics-de",
    "yarikoptic",
}

RECORD_FILE = "datalad-repos.json"

README_FOLDER = "READMEs"

USER_AGENT = "find-datalad-repos.py ({}) requests/{} {}/{}".format(
    "https://github.com/datalad/datalad-usage-dashboard",
    requests.__version__,
    platform.python_implementation(),
    platform.python_version(),
)

log = logging.getLogger(__name__)


class Status(Enum):
    ACTIVE = "active"
    GONE = "gone"


class Statistics(NamedTuple):
    repo_qty: int
    star_qty: int
    dataset_qty: int
    run_qty: int
    container_run_qty: int

    @classmethod
    def sum(cls, stats: Iterable["Statistics"]) -> "Statistics":
        def plus(x: Statistics, y: Statistics) -> Statistics:
            return Statistics(*map(add, x, y))

        return reduce(plus, stats, Statistics(0, 0, 0, 0, 0))


class TableRow(ABC, BaseModel):
    @abstractmethod
    def get_cells(self) -> List[str]:
        ...

    @abstractmethod
    def get_qtys(self) -> Statistics:
        ...

    @property
    @abstractmethod
    def ours(self) -> bool:
        ...

    @property
    @abstractmethod
    def gone(self) -> bool:
        ...


class DataladRepo(TableRow):
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


class SubtableRow(TableRow):
    name: str
    qtys: Statistics
    status: Status

    @property
    def ours(self) -> bool:
        return self.name in OURSELVES

    @property
    def gone(self) -> bool:
        return self.status is Status.GONE

    @property
    def url(self) -> str:
        return f"https://github.com/{self.name}"

    def get_cells(self) -> List[str]:
        file_link = f"{README_FOLDER}/{self.name}.md"
        cells = [
            f"[{self.name}/*]({self.url}) [({self.qtys.repo_qty})]({file_link})",
            f"[{self.qtys.star_qty}]({file_link})",
        ]
        for qty in [
            self.qtys.dataset_qty,
            self.qtys.run_qty,
            self.qtys.container_run_qty,
        ]:
            if qty > 0:
                cells.append(f"[{check(True)} ({qty})]({file_link})")
            else:
                cells.append("")
        return cells

    def get_qtys(self) -> Statistics:
        return self.qtys


class RepoTable(BaseModel):
    HEADERS: ClassVar[List[str]] = [
        "Repository",
        "Stars",
        "Dataset",
        "`run`",
        "`containers-run`",
    ]

    title: str
    rows: List[TableRow] = Field(default_factory=list)

    def get_total_qtys(self) -> Statistics:
        return Statistics.sum(r.get_qtys() for r in self.rows)

    def render(self) -> str:
        s = f"# {self.title}\n"
        if self.rows:
            qtys = self.get_total_qtys()
            headers = []
            for h, q in zip(self.HEADERS, qtys):
                if q > 0:
                    headers.append(f"{h} ({q})")
                else:
                    headers.append(h)
            s += self.render_row(headers)
            s += self.render_row(["---"] * len(self.HEADERS))
            for r in self.rows:
                s += self.render_row(r.get_cells())
        else:
            s += "No repositories found!\n"
        return s

    @staticmethod
    def render_row(cells: Iterable[str]) -> str:
        return "| " + " | ".join(cells) + " |\n"


class GHRepo(BaseModel):
    url: str
    name: str

    class Config:
        frozen = True

    @classmethod
    def from_repository(cls, data: Dict[str, Any]) -> "GHRepo":
        return cls(url=data["html_url"], name=data["full_name"])


class RepoRecord(BaseModel):
    github: List[DataladRepo] = Field(default_factory=list)


class CollectionUpdater(BaseModel):
    all_repos: Dict[str, DataladRepo]
    seen: Set[str] = Field(default_factory=set)
    new_hits: int = 0
    new_repos: int = 0
    new_runs: int = 0

    @classmethod
    def from_collection(cls, collection: List[DataladRepo]) -> "CollectionUpdater":
        return cls(all_repos={repo.name: repo for repo in collection})

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

    def get_new_collection(self, searcher: "GHDataladSearcher") -> List[DataladRepo]:
        collection: List[DataladRepo] = []
        for repo in self.all_repos.values():
            if repo.name in self.seen or searcher.repo_exists(repo.name):
                status = Status.ACTIVE
            else:
                status = Status.GONE
            collection.append(repo.copy(update={"status": status}))
        collection.sort(key=attrgetter("name"))
        return collection

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
        elif r.status_code == 404:
            return False
        else:
            r.raise_for_status()
            raise AssertionError("Unreachable")


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
def main(log_level: int, regen_readme: bool) -> None:
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
            record.github = updater.get_new_collection(searcher)
        with open(RECORD_FILE, "w") as fp:
            print(record.json(indent=4), file=fp)

    Path(README_FOLDER).mkdir(parents=True, exist_ok=True)
    repos_by_org: Mapping[str, List[DataladRepo]] = defaultdict(list)
    for repo in record.github:
        repos_by_org[repo.owner].append(repo)
    main_rows: List[TableRow] = []
    for owner, repos in repos_by_org.items():
        if len(repos) > 1:
            main_rows.append(
                make_table_file(
                    Path(README_FOLDER, f"{owner}.md"),
                    owner,
                    list(repos),  # Copy to make mypy happy
                    show_ours=False,
                )
            )
        else:
            main_rows.extend(repos)
    make_table_file(Path("README.md"), "", main_rows, show_ours=True)

    if not regen_readme:
        runcmd("git", "add", RECORD_FILE, "README.md", README_FOLDER)
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


def make_table_file(
    path: Path, name: str, rows: List[TableRow], show_ours: bool = True
) -> SubtableRow:
    wild: List[TableRow] = []
    ours: List[TableRow] = []
    gone: List[TableRow] = []
    for r in rows:
        if r.gone:
            gone.append(r)
        elif r.ours and show_ours:
            ours.append(r)
        else:
            wild.append(r)
    if show_ours:
        tables = [
            RepoTable(title="In the wild", rows=wild),
            RepoTable(title="Inner circle", rows=ours),
            RepoTable(title="Gone", rows=gone),
        ]
    else:
        tables = [
            RepoTable(title="Active", rows=wild),
            RepoTable(title="Gone", rows=gone),
        ]
    stats: List[Statistics] = []
    with path.open("w") as fp:
        first = True
        for tbl in tables:
            if first:
                first = False
            else:
                print(file=fp)
            print(tbl.render(), end="", file=fp)
            stats.append(tbl.get_total_qtys())
    if all(r.gone for tbl in tables for r in tbl.rows):
        status = Status.GONE
    else:
        status = Status.ACTIVE
    return SubtableRow(name=name, qtys=Statistics.sum(stats), status=status)


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


def runcmd(*args: Union[str, Path], **kwargs: Any) -> None:
    log.debug("Running: %s", " ".join(shlex.quote(str(a)) for a in args))
    r = subprocess.run(args, **kwargs)
    if r.returncode != 0:
        sys.exit(r.returncode)


def commit(msg: str) -> None:
    if subprocess.run(["git", "diff", "--cached", "--quiet"]).returncode != 0:
        runcmd("git", "commit", "-m", msg)
    else:
        log.info("Nothing to commit")


if __name__ == "__main__":
    main()
