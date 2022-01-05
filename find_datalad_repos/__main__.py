import logging
from operator import attrgetter
from typing import Dict, List, Optional, Set
import click
from click_loglevel import LogLevel
from pydantic import BaseModel, Field
from .config import README_FOLDER, RECORD_FILE
from .core import RepoRecord, mkreadmes
from .github import GHDataladRepo, GHDataladSearcher, get_github_token
from .osf import OSFDataladRepo, OSFDataladSearcher
from .util import Status, commit, runcmd


class GHCollectionUpdater(BaseModel):
    all_repos: Dict[str, GHDataladRepo]
    seen: Set[str] = Field(default_factory=set)
    new_hits: int = 0
    new_repos: int = 0
    new_runs: int = 0

    @classmethod
    def from_collection(cls, collection: List[GHDataladRepo]) -> "GHCollectionUpdater":
        return cls(all_repos={repo.name: repo for repo in collection})

    def register_repo(self, repo: GHDataladRepo) -> None:
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
            repo = repo.copy(
                update={
                    "dataset": old_repo.dataset or repo.dataset,
                    "run": old_repo.run or repo.run,
                    "container_run": old_repo.container_run or repo.container_run,
                }
            )
        self.all_repos[repo.name] = repo

    def get_new_collection(self, searcher: "GHDataladSearcher") -> List[GHDataladRepo]:
        collection: List[GHDataladRepo] = []
        for repo in self.all_repos.values():
            if repo.name in self.seen or searcher.repo_exists(repo.name):
                status = Status.ACTIVE
            else:
                status = Status.GONE
            collection.append(repo.copy(update={"status": status}))
        collection.sort(key=attrgetter("name"))
        return collection

    def get_reports(self) -> List[str]:
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


class OSFCollectionUpdater(BaseModel):
    all_repos: Dict[str, OSFDataladRepo]
    seen: Set[str] = Field(default_factory=set)
    new_repos: int = 0

    @classmethod
    def from_collection(
        cls, collection: List[OSFDataladRepo]
    ) -> "OSFCollectionUpdater":
        return cls(all_repos={repo.id: repo for repo in collection})

    def register_repo(self, repo: OSFDataladRepo) -> None:
        self.seen.add(repo.id)
        if repo.id not in self.all_repos:
            self.new_repos += 1
        self.all_repos[repo.id] = repo

    def get_new_collection(self) -> List[OSFDataladRepo]:
        collection: List[OSFDataladRepo] = []
        for repo in self.all_repos.values():
            if repo.id in self.seen:
                status = Status.ACTIVE
            else:
                status = Status.GONE
            collection.append(repo.copy(update={"status": status}))
        collection.sort(key=attrgetter("name"))
        return collection

    def get_reports(self) -> List[str]:
        if self.new_repos:
            return [f"OSF: {self.new_repos} new datasets"]
        else:
            return []


@click.command()
@click.option(
    "-l",
    "--log-level",
    type=LogLevel(),
    default=logging.INFO,
    help="Set logging level  [default: INFO]",
)
@click.option("--github", "mode", flag_value="github", help="Only update GitHub data")
@click.option("--osf", "mode", flag_value="osf", help="Only update OSF data")
@click.option(
    "-R",
    "--regen-readme",
    "mode",
    flag_value="readme",
    help="Regenerate the README from the JSON file without querying",
)
def main(log_level: int, mode: Optional[str]) -> None:
    logging.basicConfig(
        format="%(asctime)s [%(levelname)-8s] %(name)s %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S%z",
        level=log_level,
    )

    try:
        record = RepoRecord.parse_file(RECORD_FILE)
    except FileNotFoundError:
        record = RepoRecord()

    reports: List[str] = []
    if mode != "readme":
        if mode is None or mode == "github":
            gh_updater = GHCollectionUpdater.from_collection(record.github)
            with GHDataladSearcher(get_github_token()) as gh_searcher:
                for ghrepo in gh_searcher.get_datalad_repos():
                    gh_updater.register_repo(ghrepo)
                record.github = gh_updater.get_new_collection(gh_searcher)
            reports.extend(gh_updater.get_reports())

        if mode is None or mode == "osf":
            osf_updater = OSFCollectionUpdater.from_collection(record.osf)
            with OSFDataladSearcher() as osf_searcher:
                for osfrepo in osf_searcher.get_datalad_repos():
                    osf_updater.register_repo(osfrepo)
                record.osf = osf_updater.get_new_collection()
            reports.extend(osf_updater.get_reports())

        with open(RECORD_FILE, "w") as fp:
            print(record.json(indent=4), file=fp)

    mkreadmes(record)

    if mode != "readme":
        runcmd("git", "add", RECORD_FILE, "README.md", README_FOLDER)
        if reports:
            msg = "; ".join(reports)
        else:
            msg = "Updated the state without any new hits added"
        commit(msg)


if __name__ == "__main__":
    main()
