from __future__ import annotations
from datetime import timedelta
import heapq
import json
import logging
from operator import attrgetter
import os
from typing import Dict, List, Set
import click
from click_loglevel import LogLevel
from ghreq import PrettyHTTPError
from ghtoken import get_ghtoken
from pydantic import BaseModel, Field
from .config import README_FOLDER, RECORD_FILE
from .core import RepoRecord, mkreadmes
from .gin import GINDataladRepo, GINDataladSearcher
from .github import GHDataladRepo, GHDataladSearcher, GHSearchResult
from .osf import OSFDataladRepo, OSFDataladSearcher
from .util import Status, commit, log, nowutc, runcmd


class GHCollectionUpdater(BaseModel):
    all_repos: Dict[int, GHDataladRepo]
    #: Repos that disappeared before we started tracking IDs
    noid_repos: List[GHDataladRepo]
    seen: Set[int] = Field(default_factory=set)
    new_hits: int = 0
    new_repos: int = 0
    new_runs: int = 0

    @classmethod
    def from_collection(cls, collection: list[GHDataladRepo]) -> GHCollectionUpdater:
        all_repos: dict[int, GHDataladRepo] = {}
        noid_repos: list[GHDataladRepo] = []
        for repo in collection:
            if repo.id is not None:
                all_repos[repo.id] = repo
            else:
                noid_repos.append(repo)
        return cls(all_repos=all_repos, noid_repos=noid_repos)

    def register_repo(self, sr: GHSearchResult, searcher: GHDataladSearcher) -> None:
        rid = sr.id
        assert rid is not None
        self.seen.add(rid)
        try:
            old_repo = self.all_repos[rid]
        except KeyError:
            self.new_hits += 1
            self.new_repos += 1
            if sr.run:
                self.new_runs += 1
            extra = searcher.get_extra_repo_details(sr.name)
            if extra is None or extra.id != sr.id:
                raise RuntimeError(
                    f"GitHub repository {sr.name} suddenly disappeared after"
                    " being returned in a search!"
                )
            else:
                repo = GHDataladRepo(
                    id=sr.id,
                    name=sr.name,
                    url=sr.url,
                    stars=extra.stars,
                    dataset=sr.dataset,
                    run=sr.run,
                    container_run=sr.container_run,
                    status=Status.ACTIVE,
                    updated=extra.pushed_at,
                    last_checked=nowutc(),
                )
        else:
            if not old_repo.run and sr.run:
                self.new_hits += 1
                self.new_runs += 1
            repo = GHDataladRepo(
                id=sr.id,
                name=sr.name,
                url=sr.url,
                stars=old_repo.stars,
                dataset=old_repo.dataset or sr.dataset,
                run=old_repo.run or sr.run,
                container_run=old_repo.container_run or sr.container_run,
                status=Status.ACTIVE,
                updated=old_repo.updated,
                last_checked=old_repo.last_checked,
            )
        self.all_repos[rid] = repo

    def get_new_collection(self, searcher: GHDataladSearcher) -> list[GHDataladRepo]:
        collection: list[GHDataladRepo] = list(self.noid_repos)
        replaced: set[int] = set()
        check_cutoff = nowutc() - timedelta(days=7)
        needs_check = heapq.nsmallest(
            1000,
            (
                r
                for r in self.all_repos.values()
                if r.last_checked is None or r.last_checked < check_cutoff
            ),
            key=lambda r: (r.last_checked is not None, r.last_checked),
        )
        for repo in needs_check:
            log.info(
                "Getting latest details for repository %s (last checked: %s)",
                repo.name,
                repo.last_checked,
            )
            try:
                extra = searcher.get_extra_repo_details(repo.name)
            except PrettyHTTPError as e:
                if e.response.status_code == 404 and "rate limit" in e.response.text:
                    log.info(
                        "Hit a GitHub rate limit; not checking any more repositories"
                    )
                    break
                else:
                    raise
            else:
                if extra is None:
                    log.info("Repository %s no longer exists", repo.name)
                    repo.status = Status.GONE
                elif repo.id is not None and extra.id != repo.id:
                    log.info(
                        "Repository %s with ID %d has been replaced; deleting",
                        repo.name,
                        repo.id,
                    )
                    replaced.add(repo.id)
                else:
                    repo.status = Status.ACTIVE
                    repo.stars = extra.stars
                    repo.updated = extra.pushed_at
                repo.last_checked = nowutc()
        collection.extend(
            repo
            for repo in self.all_repos.values()
            if not (repo.id is not None and repo.id in replaced)
        )
        collection.sort(key=attrgetter("name"))
        return collection

    def get_reports(self) -> list[str]:
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
    def from_collection(cls, collection: list[OSFDataladRepo]) -> OSFCollectionUpdater:
        return cls(all_repos={repo.id: repo for repo in collection})

    def register_repo(self, repo: OSFDataladRepo) -> None:
        self.seen.add(repo.id)
        if repo.id not in self.all_repos:
            self.new_repos += 1
        self.all_repos[repo.id] = repo

    def get_new_collection(self) -> list[OSFDataladRepo]:
        collection: list[OSFDataladRepo] = []
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


class GINCollectionUpdater(BaseModel):
    all_repos: Dict[int, GINDataladRepo]
    seen: Set[int] = Field(default_factory=set)
    new_repos: int = 0

    @classmethod
    def from_collection(cls, collection: list[GINDataladRepo]) -> GINCollectionUpdater:
        return cls(all_repos={repo.id: repo for repo in collection})

    def register_repo(self, repo: GINDataladRepo) -> None:
        self.seen.add(repo.id)
        if repo.id not in self.all_repos:
            self.new_repos += 1
        self.all_repos[repo.id] = repo

    def get_new_collection(self) -> list[GINDataladRepo]:
        collection: list[GINDataladRepo] = []
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
            return [f"GIN: {self.new_repos} new datasets"]
        else:
            return []


def set_mode(
    ctx: click.Context, _param: click.Parameter, value: str | None
) -> str | None:
    if value is not None:
        ctx.params.setdefault("mode", set()).add(value)
    return value


@click.command()
@click.option(
    "-l",
    "--log-level",
    type=LogLevel(),
    default=logging.INFO,
    help="Set logging level  [default: INFO]",
)
@click.option(
    "--gin",
    flag_value="gin",
    callback=set_mode,
    expose_value=False,
    help="Update GIN data",
)
@click.option(
    "--github",
    flag_value="github",
    callback=set_mode,
    expose_value=False,
    help="Update GitHub data",
)
@click.option(
    "--osf",
    flag_value="osf",
    callback=set_mode,
    expose_value=False,
    help="Update OSF data",
)
@click.option(
    "-R",
    "--regen-readme",
    is_flag=True,
    help="Regenerate the README from the JSON file without querying",
)
def main(log_level: int, regen_readme: bool, mode: set[str] | None = None) -> None:
    if regen_readme and mode:
        raise click.UsageError("--regen-readme is mutually exclusive with mode options")

    logging.basicConfig(
        format="%(asctime)s [%(levelname)-8s] %(name)s %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S%z",
        level=log_level,
    )

    try:
        with open(RECORD_FILE, encoding="utf-8") as fp:
            record = RepoRecord.model_validate(json.load(fp))
    except FileNotFoundError:
        record = RepoRecord()

    reports: list[str] = []
    if not regen_readme:
        if mode is None or "github" in mode:
            gh_updater = GHCollectionUpdater.from_collection(record.github)
            with GHDataladSearcher(get_ghtoken()) as gh_searcher:
                for ghrepo in gh_searcher.get_datalad_repos():
                    gh_updater.register_repo(ghrepo, gh_searcher)
                record.github = gh_updater.get_new_collection(gh_searcher)
            reports.extend(gh_updater.get_reports())

        if mode is None or "osf" in mode:
            osf_updater = OSFCollectionUpdater.from_collection(record.osf)
            with OSFDataladSearcher() as osf_searcher:
                for osfrepo in osf_searcher.get_datalad_repos():
                    osf_updater.register_repo(osfrepo)
                record.osf = osf_updater.get_new_collection()
            reports.extend(osf_updater.get_reports())

        if mode is None or "gin" in mode:
            gin_updater = GINCollectionUpdater.from_collection(record.gin)
            with GINDataladSearcher(token=os.environ["GIN_TOKEN"]) as gin_searcher:
                for ginrepo in gin_searcher.get_datalad_repos():
                    gin_updater.register_repo(ginrepo)
                record.gin = gin_updater.get_new_collection()
            reports.extend(gin_updater.get_reports())

        with open(RECORD_FILE, "w") as fp:
            print(record.model_dump_json(indent=4), file=fp)

    mkreadmes(record)

    if not regen_readme:
        runcmd("git", "add", RECORD_FILE, "README.md", README_FOLDER)
        if reports:
            msg = "; ".join(reports)
        else:
            msg = "Updated the state without any new hits added"
        commit(msg)


if __name__ == "__main__":
    main()
