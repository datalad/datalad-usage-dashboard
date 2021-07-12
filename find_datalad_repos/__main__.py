from collections import defaultdict
import logging
from operator import attrgetter
from pathlib import Path
from typing import Dict, List, Mapping, Set
import click
from click_loglevel import LogLevel
from pydantic import BaseModel, Field
from .config import README_FOLDER, RECORD_FILE
from .github import DataladRepo, GHDataladSearcher, get_github_token
from .tables import TableRow, make_table_file
from .util import Status, commit, runcmd


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
        news = (
            f"{self.new_repos} new datasets",
            f"{self.new_runs} new `datalad run` users",
        )
        if self.new_hits:
            return f"Added {self.new_hits} new hits: " + " and ".join(
                n for n in news if not n.startswith("0 ")
            )
        else:
            return "Updated the state without any new hits added"


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


if __name__ == "__main__":
    main()
