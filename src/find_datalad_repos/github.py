from __future__ import annotations
from collections.abc import Iterator
from datetime import datetime, timedelta
import heapq
from operator import attrgetter
from time import sleep
from typing import Any
from ghreq import Client, PrettyHTTPError
from pydantic import BaseModel, Field
from .core import RepoHost, Searcher, Updater
from .tables import GITHUB_COLUMNS, Column, TableRow
from .config import EXCLUSION_THRESHOLD
from .util import USER_AGENT, Status, check, is_container_run, log, nowutc, build_exclusion_query, get_organizations_for_exclusion

# Searching too fast can trigger abuse detection
INTER_SEARCH_DELAY = 10

# How long to wait after triggering abuse detection
POST_ABUSE_DELAY = 45


class SearchResult(BaseModel):
    id: int
    name: str
    url: str
    dataset: bool
    run: bool
    container_run: bool


class SearchHit(BaseModel, frozen=True):
    id: int
    url: str
    name: str

    @classmethod
    def from_repository(cls, data: dict[str, Any]) -> SearchHit:
        return cls(id=data["id"], url=data["html_url"], name=data["full_name"])


class ExtraDetails(BaseModel):
    id: int
    pushed_at: datetime
    stars: int = Field(alias="stargazers_count")


class GitHubRepo(BaseModel):
    id: int | None
    name: str
    url: str
    stars: int
    dataset: bool
    run: bool
    container_run: bool
    status: Status
    updated: datetime | None = None
    last_checked: datetime | None = None

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
            Column.IS_DATASET: check(self.dataset),
            Column.IS_RUN: check(self.run),
            Column.IS_CONTAINERS_RUN: check(self.container_run),
            Column.LAST_MODIFIED: (
                str(self.updated) if self.updated is not None else "\u2014"
            ),
        }
        assert set(cells.keys()) == set(GITHUB_COLUMNS)
        qtys = {
            Column.REPOSITORY: 1,
            Column.STARS: self.stars,
            Column.IS_DATASET: int(self.dataset),
            Column.IS_RUN: int(self.run),
            Column.IS_CONTAINERS_RUN: int(self.container_run),
        }
        assert set(qtys.keys()) == {col for col in GITHUB_COLUMNS if col.countable}
        return TableRow(cells=cells, qtys=qtys)


class GitHubSearcher(Client, Searcher[SearchResult]):
    def __init__(self, token: str, excluded_orgs: list[str] | None = None) -> None:
        super().__init__(token=token, user_agent=USER_AGENT)
        self.excluded_orgs = excluded_orgs or []

    def search(self, resource_type: str, query: str) -> Iterator[Any]:
        url: str | None = f"/search/{resource_type}"
        params: dict[str, str] | None = {
            "q": query,
            "per_page": "100",  # default is 30.
        }
        while url is not None:
            try:
                r = self.get(url, params=params, raw=True)
            except PrettyHTTPError as e:
                if (
                    e.response is not None
                    and e.response.status_code == 403
                    and "abuse detection" in e.response.text
                ):
                    log.warning(
                        "Abuse detection triggered; sleeping for %s seconds",
                        POST_ABUSE_DELAY,
                    )
                    sleep(POST_ABUSE_DELAY)
                    continue
                else:
                    raise
            data = r.json()
            if data["incomplete_results"]:
                log.warning("Search returned incomplete results due to timeout")
            yield from data["items"]
            url = r.links.get("next", {}).get("url")
            params = None
            if url is not None:
                sleep(INTER_SEARCH_DELAY)

    def search_dataset_repos(self) -> Iterator[SearchHit]:
        log.info("Searching for repositories with .datalad/config files")
        for hit in self.search("code", "path:.datalad filename:config fork:true"):
            repo = SearchHit.from_repository(hit["repository"])
            log.info("Found %s", repo.name)
            yield repo

    def search_runcmds(self) -> Iterator[tuple[SearchHit, bool]]:
        """Returns a generator of (ghrepo, is_container_run) pairs"""
        log.info('Searching for "DATALAD RUNCMD" commits')
        for hit in self.search("commits", '"DATALAD RUNCMD" merge:false is:public'):
            container_run = is_container_run(hit["commit"]["message"])
            repo = SearchHit.from_repository(hit["repository"])
            log.info(
                "Found commit %s in %s (container run: %s)",
                hit["sha"][:7],
                repo.name,
                container_run,
            )
            yield (repo, container_run)

    def search_dataset_repos_with_exclusions(self, exclusions: str) -> Iterator[SearchHit]:
        """Global search excluding busy organizations"""
        query = f"path:.datalad filename:config fork:true{exclusions}"
        log.info(f"Global search with exclusions: {query}")
        for hit in self.search("code", query):
            repo = SearchHit.from_repository(hit["repository"])
            log.info("Found %s", repo.name)
            yield repo

    def search_runcmds_with_exclusions(self, exclusions: str) -> Iterator[tuple[SearchHit, bool]]:
        """Global RUNCMD search excluding busy organizations"""
        query = f'"DATALAD RUNCMD" merge:false is:public{exclusions}'
        log.info(f"Global RUNCMD search with exclusions: {query}")
        for hit in self.search("commits", query):
            container_run = is_container_run(hit["commit"]["message"])
            repo = SearchHit.from_repository(hit["repository"])
            log.info("Found commit %s in %s (container run: %s)",
                     hit["sha"][:7], repo.name, container_run)
            yield (repo, container_run)

    def search_dataset_repos_in_org(self, org: str) -> Iterator[SearchHit]:
        """Search for datasets within specific organization"""
        query = f"org:{org} path:.datalad filename:config"
        log.info(f"Searching for .datalad datasets in {org}")
        for hit in self.search("code", query):
            repo = SearchHit.from_repository(hit["repository"])
            log.info("Found %s", repo.name)
            yield repo

    def search_runcmds_in_org(self, org: str) -> Iterator[tuple[SearchHit, bool]]:
        """Search for DATALAD RUNCMD within specific organization"""
        query = f'org:{org} "DATALAD RUNCMD" merge:false'
        log.info(f"Searching for RUNCMD commits in {org}")
        for hit in self.search("commits", query):
            container_run = is_container_run(hit["commit"]["message"])
            repo = SearchHit.from_repository(hit["repository"])
            log.info("Found commit %s in %s (container run: %s)",
                     hit["sha"][:7], repo.name, container_run)
            yield (repo, container_run)

    def get_extra_repo_details(self, repo_fullname: str) -> ExtraDetails | None:
        try:
            r = self.get(f"/repos/{repo_fullname}")
        except PrettyHTTPError as e:
            if e.response.status_code == 404:
                return None
            else:
                raise
        else:
            return ExtraDetails.parse_obj(r)

    def get_datalad_repos(self, excluded_orgs: list[str] | None = None) -> list[SearchResult]:
        """
        Hybrid search: global search with exclusions + targeted org searches

        Args:
            excluded_orgs: Organizations to exclude from global search and search individually
                          If None, uses self.excluded_orgs set during initialization
        """
        excluded_orgs = excluded_orgs or self.excluded_orgs
        exclusions = build_exclusion_query(excluded_orgs) if excluded_orgs else ""

        log.info(f"Using hybrid search strategy with {len(excluded_orgs)} excluded orgs")

        # Phase 1: Global search excluding busy orgs
        log.info("Phase 1: Global searches with exclusions")
        runcmds: dict[SearchHit, bool] = {}

        if excluded_orgs:
            datasets = set(self.search_dataset_repos_with_exclusions(exclusions))

            for repo, container_run in self.search_runcmds_with_exclusions(exclusions):
                runcmds[repo] = container_run or runcmds.get(repo, False)
        else:
            # Fallback to original search if no exclusions
            datasets = set(self.search_dataset_repos())

            for repo, container_run in self.search_runcmds():
                runcmds[repo] = container_run or runcmds.get(repo, False)

        log.info(f"Global search found: {len(datasets)} datasets, {len(runcmds)} runcmd repos")

        # Phase 2: Organization-specific searches
        if excluded_orgs:
            log.info("Phase 2: Organization-specific searches")
            for org in excluded_orgs:
                log.info(f"Searching within organization: {org}")

                # Add datasets from this org
                org_datasets = set(self.search_dataset_repos_in_org(org))
                datasets.update(org_datasets)

                # Add runcmds from this org
                for repo, container_run in self.search_runcmds_in_org(org):
                    runcmds[repo] = container_run or runcmds.get(repo, False)

                log.info(f"Organization {org} added: {len(org_datasets)} datasets")

        log.info(f"Total after hybrid search: {len(datasets)} datasets, {len(runcmds)} runcmd repos")

        # Convert to SearchResult objects
        results = []
        for repo in datasets | runcmds.keys():
            results.append(SearchResult(
                id=repo.id, url=repo.url, name=repo.name,
                dataset=repo in datasets,
                run=repo in runcmds,
                container_run=runcmds.get(repo, False),
            ))

        return sorted(results, key=attrgetter("name"))


class GitHubUpdater(BaseModel, Updater[GitHubRepo, SearchResult, GitHubSearcher]):
    all_repos: dict[int, GitHubRepo]
    #: Repos that disappeared before we started tracking IDs
    noid_repos: list[GitHubRepo]
    excluded_orgs: list[str] = Field(default_factory=list)
    seen: set[int] = Field(default_factory=set)
    new_hits: int = 0
    new_repos: int = 0
    new_runs: int = 0

    @classmethod
    def from_collection(
        cls, host: RepoHost, collection: list[GitHubRepo]  # noqa: U100
    ) -> GitHubUpdater:
        all_repos: dict[int, GitHubRepo] = {}
        noid_repos: list[GitHubRepo] = []
        for repo in collection:
            if repo.id is not None:
                all_repos[repo.id] = repo
            else:
                noid_repos.append(repo)

        # Calculate organizations to exclude based on current collection
        excluded_orgs = get_organizations_for_exclusion(
            current_repos=collection,
            threshold=EXCLUSION_THRESHOLD
        )

        return cls(
            all_repos=all_repos,
            noid_repos=noid_repos,
            excluded_orgs=excluded_orgs
        )

    def get_searcher(self, **kwargs: Any) -> GitHubSearcher:
        return GitHubSearcher(excluded_orgs=self.excluded_orgs, **kwargs)

    def register_repo(self, sr: SearchResult, searcher: GitHubSearcher) -> None:
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
                repo = GitHubRepo(
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
            repo = GitHubRepo(
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

    def get_new_collection(self, searcher: GitHubSearcher) -> list[GitHubRepo]:
        collection: list[GitHubRepo] = list(self.noid_repos)
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
