from __future__ import annotations
from collections.abc import Iterator
from datetime import datetime, timedelta
import heapq
from operator import attrgetter
from time import sleep
from typing import Any
from ghreq import Client, PrettyHTTPError
from pydantic import BaseModel, Field
import requests
from .core import RepoHost, Searcher, Updater
from .github_orgs import DiscoveryMethod, GitHubOrgsConfig, initialize_orgs_config
from .tables import GITHUB_COLUMNS, Column, TableRow
from .util import (
    USER_AGENT,
    Status,
    build_exclusion_query,
    check,
    is_container_run,
    log,
    nowutc,
)

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
    def __init__(
        self,
        token: str,
        excluded_orgs: list[str] | None = None,
        known_repos: list[dict] | None = None,
        orgs_config: GitHubOrgsConfig | None = None,
    ) -> None:
        super().__init__(token=token, user_agent=USER_AGENT)
        self.excluded_orgs = excluded_orgs or []
        self.known_repos = known_repos
        self.orgs_config = orgs_config or GitHubOrgsConfig()

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

    def search_dataset_repos_with_exclusions(
        self, exclusions: str
    ) -> Iterator[SearchHit]:
        """Global search excluding busy organizations"""
        query = f"path:.datalad filename:config fork:true{exclusions}"
        log.info(f"Global search with exclusions: {query}")
        for hit in self.search("code", query):
            repo = SearchHit.from_repository(hit["repository"])
            log.info("Found %s", repo.name)
            yield repo

    def search_runcmds_with_exclusions(
        self, exclusions: str
    ) -> Iterator[tuple[SearchHit, bool]]:
        """Global RUNCMD search excluding busy organizations"""
        query = f'"DATALAD RUNCMD" merge:false is:public{exclusions}'
        log.info(f"Global RUNCMD search with exclusions: {query}")
        for hit in self.search("commits", query):
            container_run = is_container_run(hit["commit"]["message"])
            repo = SearchHit.from_repository(hit["repository"])
            log.info(
                "Found commit %s in %s (container run: %s)",
                hit["sha"][:7],
                repo.name,
                container_run,
            )
            yield (repo, container_run)

    def search_dataset_repos_in_org(
        self, org: str, known_repos: list[dict] | None = None
    ) -> Iterator[SearchHit]:
        """
        Search for datasets within specific organization with enumeration fallback.

        Args:
            org: Organization name
            known_repos: Optional list of known repositories for fallback detection

        Yields:
            SearchHit objects for found repositories
        """
        query = f"org:{org} path:.datalad filename:config"
        log.info(f"Searching for .datalad datasets in {org}")

        # Collect search results first to check if fallback is needed
        search_results = []
        for hit in self.search("code", query):
            search_results.append(hit)

        # Check if we need enumeration fallback
        if self.needs_enumeration_fallback(org, search_results, known_repos):
            # Get known repo names for this org (active only)
            known_names = (
                {
                    r["name"]
                    for r in known_repos
                    if r["name"].startswith(f"{org}/") and r.get("status") != "gone"
                }
                if known_repos
                else set()
            )

            # Enumerate and check unknown repositories
            for result in self.process_enumerated_repos(org, known_names):
                # Convert SearchResult to dict format matching search API
                search_results.append(
                    {
                        "repository": {
                            "id": result.id,
                            "html_url": result.url,
                            "full_name": result.name,
                        }
                    }
                )

        # Yield results in consistent format
        for hit in search_results:
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
            log.info(
                "Found commit %s in %s (container run: %s)",
                hit["sha"][:7],
                repo.name,
                container_run,
            )
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

    def traverse_org_repositories(
        self, org: str, known_repos: list[dict] | None = None
    ) -> Iterator[SearchHit]:
        """
        Directly enumerate all repositories in an organization and check for DataLad.

        This bypasses GitHub's search API and directly checks each repository
        for the presence of .datalad/config file.

        Args:
            org: Organization name
            known_repos: Known repositories for optimization

        Yields:
            SearchHit objects for repositories with .datalad/config
        """
        log.info(f"Enumerating all repositories in organization {org}")

        # Get known repo names for this org for optimization
        known_names = set()
        if known_repos:
            known_names = {
                r["name"]
                for r in known_repos
                if r["name"].startswith(f"{org}/") and r.get("status") != "gone"
            }

        try:
            # Enumerate all repositories in the organization
            for repo_data in self.paginate(
                f"/orgs/{org}/repos", params={"type": "all"}
            ):
                repo_name = repo_data["full_name"]

                # Skip if we know this repo doesn't have DataLad
                if known_names and repo_name not in known_names:
                    # Check for .datalad/config
                    try:
                        self.get(f"/repos/{repo_name}/contents/.datalad/config")
                        # If we get here, the file exists
                        log.info(f"Found DataLad repository via traversal: {repo_name}")
                        yield SearchHit.from_repository(repo_data)
                    except PrettyHTTPError as e:
                        if e.response.status_code == 404:
                            # File doesn't exist, not a DataLad repo
                            continue
                        else:
                            # Some other error, log and continue
                            log.warning(f"Error checking {repo_name}: {e}")
                            continue
                else:
                    # We already know this is a DataLad repo from known_repos
                    log.debug(f"Skipping known DataLad repository: {repo_name}")
                    yield SearchHit.from_repository(repo_data)

        except PrettyHTTPError as e:
            if e.response.status_code == 404:
                log.warning(f"Organization {org} not found or not accessible")
            else:
                log.error(f"Error enumerating repositories for {org}: {e}")
                raise

    def get_datalad_repos(self) -> list[SearchResult]:
        """
        Discover DataLad repositories using configured discovery methods.
        """

        datasets: set[SearchHit] = set()
        runcmds: dict[SearchHit, bool] = {}

        # Get organizations by discovery method
        global_search_orgs = self.orgs_config.get_orgs_by_discovery_method(
            DiscoveryMethod.GLOBAL_SEARCH
        )
        org_search_orgs = self.orgs_config.get_orgs_by_discovery_method(
            DiscoveryMethod.ORG_SEARCH
        )
        org_traverse_orgs = self.orgs_config.get_orgs_by_discovery_method(
            DiscoveryMethod.ORG_TRAVERSE
        )

        # Build exclusion query for orgs not using global_search
        excluded_orgs = self.orgs_config.get_excluded_orgs()
        exclusions = build_exclusion_query(excluded_orgs) if excluded_orgs else ""

        log.info(
            f"Discovery methods - global_search: {len(global_search_orgs)}, "
            f"org_search: {len(org_search_orgs)}, "
            f"org_traverse: {len(org_traverse_orgs)}"
        )

        # Phase 1: Global search (with exclusions)
        log.info(f"Phase 1: Global search (excluding {len(excluded_orgs)} orgs)")
        if exclusions:
            datasets.update(self.search_dataset_repos_with_exclusions(exclusions))
            for repo, container_run in self.search_runcmds_with_exclusions(exclusions):
                runcmds[repo] = container_run or runcmds.get(repo, False)
        else:
            datasets.update(self.search_dataset_repos())
            for repo, container_run in self.search_runcmds():
                runcmds[repo] = container_run or runcmds.get(repo, False)

        log.info(
            f"Global search found: {len(datasets)} datasets, "
            f"{len(runcmds)} runcmd repos"
        )

        # Phase 2a: Organization-specific search (with auto-fallback)
        for org in org_search_orgs:
            log.info(f"Phase 2a: Searching organization {org}")
            org_datasets = list(self.search_dataset_repos_in_org(org))

            # Auto-fallback if search returns nothing but we have known repos
            if len(org_datasets) == 0:
                known_count = len(
                    [
                        r
                        for r in (self.known_repos or [])
                        if r["name"].startswith(f"{org}/") and r.get("status") != "gone"
                    ]
                )
                if known_count > 0:
                    log.warning(
                        f"Search returned 0 results for {org} but has {known_count} "
                        f"known repos - falling back to traversal"
                    )
                    org_datasets = list(
                        self.traverse_org_repositories(org, self.known_repos)
                    )

            datasets.update(org_datasets)

            # Also search for RUNCMD commits
            for repo, container_run in self.search_runcmds_in_org(org):
                runcmds[repo] = container_run or runcmds.get(repo, False)

            log.info(f"Organization {org} added: {len(org_datasets)} datasets")

        # Phase 2b: Organization traversal (always enumerate)
        for org in org_traverse_orgs:
            log.info(f"Phase 2b: Traversing organization {org}")
            org_datasets = list(self.traverse_org_repositories(org, self.known_repos))
            datasets.update(org_datasets)
            # Note: Can't detect RUNCMD via traversal without searching commits
            log.info(f"Organization {org} added: {len(org_datasets)} datasets")

        log.info(
            f"Total discovered: {len(datasets)} datasets, "
            f"{len(runcmds)} runcmd repos"
        )

        # Convert to SearchResult objects
        results = []
        for repo in datasets | runcmds.keys():
            results.append(
                SearchResult(
                    id=repo.id,
                    url=repo.url,
                    name=repo.name,
                    dataset=repo in datasets,
                    run=repo in runcmds,
                    container_run=runcmds.get(repo, False),
                )
            )

        return sorted(results, key=attrgetter("name"))

    def needs_enumeration_fallback(
        self, org: str, search_results: list, known_repos: list[dict] | None
    ) -> bool:
        """
        Determine if we need to enumerate repositories directly.

        Args:
            org: Organization name
            search_results: Results from organization search
            known_repos: List of known repositories with name and status

        Returns:
            True if fallback enumeration should be used
        """
        if not known_repos:
            return False

        # Count active known repos for this org
        active_known = [
            r
            for r in known_repos
            if r["name"].startswith(f"{org}/") and r.get("status") != "gone"
        ]

        # Use fallback if search returns empty but we have active repos
        if len(search_results) == 0 and len(active_known) > 0:
            log.warning(
                f"Organization {org} returned 0 search results but has "
                f"{len(active_known)} known active repos - using enumeration fallback"
            )
            return True

        return False

    def enumerate_org_repositories(self, org: str) -> Iterator[dict]:
        """
        List all repositories in an organization using GitHub API.

        Args:
            org: Organization name

        Yields:
            Repository data dictionaries from GitHub API
        """
        page = 1
        while True:
            try:
                repos = self.get(
                    f"/orgs/{org}/repos",
                    params={"per_page": "100", "page": str(page), "type": "all"},
                )
                if not repos:
                    break

                log.info(f"Enumerated page {page} of {org} repos ({len(repos)} items)")
                yield from repos

                # Rate limiting between pages
                sleep(2)
                page += 1

            except PrettyHTTPError as e:
                if e.response.status_code == 404:
                    log.error(f"Organization {org} not found")
                    break
                else:
                    raise

    def check_datalad_config(self, owner: str, repo: str, branch: str) -> bool:
        """
        Check if repository has .datalad/config file via raw GitHub content.

        Args:
            owner: Repository owner/organization
            repo: Repository name
            branch: Branch to check

        Returns:
            True if .datalad/config exists, False otherwise
        """
        url = (
            f"https://raw.githubusercontent.com/{owner}/{repo}/"
            f"refs/heads/{branch}/.datalad/config"
        )

        try:
            # Use HEAD request to check existence without downloading content
            response = requests.head(url, timeout=5, allow_redirects=True)
            exists = response.status_code == 200

            if exists:
                log.debug(f"Found .datalad/config in {owner}/{repo} on {branch}")

            return exists

        except requests.RequestException as e:
            log.debug(f"Error checking {owner}/{repo}: {e}")
            return False

    def process_enumerated_repos(
        self, org: str, known_repo_names: set[str]
    ) -> Iterator[SearchResult]:
        """
        Process repositories found via enumeration that aren't already known.

        Args:
            org: Organization name
            known_repo_names: Set of already known repository full names

        Yields:
            SearchResult objects for new DataLad repositories
        """
        checked_count = 0
        found_count = 0

        for repo_data in self.enumerate_org_repositories(org):
            repo_fullname = f"{org}/{repo_data['name']}"

            # Skip if already known
            if repo_fullname in known_repo_names:
                continue

            checked_count += 1

            # Get default branch
            default_branch = repo_data.get("default_branch", "main")

            # Check for .datalad/config
            if self.check_datalad_config(org, repo_data["name"], default_branch):
                found_count += 1
                log.info(f"Found new DataLad repo via enumeration: {repo_fullname}")

                yield SearchResult(
                    id=repo_data["id"],
                    url=repo_data["html_url"],
                    name=repo_fullname,
                    dataset=True,
                    run=False,  # Can't detect RUNCMD without searching commits
                    container_run=False,
                )

        log.info(
            f"Enumeration complete for {org}: checked {checked_count} unknown repos, "
            f"found {found_count} DataLad datasets"
        )


class GitHubUpdater(BaseModel, Updater[GitHubRepo, SearchResult, GitHubSearcher]):
    all_repos: dict[int, GitHubRepo]
    #: Repos that disappeared before we started tracking IDs
    noid_repos: list[GitHubRepo]
    excluded_orgs: list[str] = Field(default_factory=list)
    orgs_config: GitHubOrgsConfig = Field(default_factory=GitHubOrgsConfig)
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

        # Load or initialize organization configuration
        orgs_config = GitHubOrgsConfig.load()
        if not orgs_config.orgs:
            # Initialize from current data if empty
            orgs_config = initialize_orgs_config(collection)
            orgs_config.save()
            log.info("Initialized github-orgs.json configuration file")
        # Get organizations to exclude from the configuration
        excluded_orgs = orgs_config.get_excluded_orgs()

        return cls(
            all_repos=all_repos,
            noid_repos=noid_repos,
            excluded_orgs=excluded_orgs,
            orgs_config=orgs_config,
        )

    def get_searcher(self, **kwargs: Any) -> GitHubSearcher:
        # Prepare known repos for fallback detection
        known_repos = [
            {"name": r.name, "status": r.status.value} for r in self.all_repos.values()
        ]
        return GitHubSearcher(
            excluded_orgs=self.excluded_orgs,
            known_repos=known_repos,
            orgs_config=self.orgs_config,
            **kwargs,
        )

    def get_organizations_to_traverse(self) -> list[str]:
        """Get list of organizations that need timestamp updates after traversal."""
        orgs_to_traverse = []
        # Include organizations that use ORG_SEARCH or ORG_TRAVERSE methods
        for method in [DiscoveryMethod.ORG_SEARCH, DiscoveryMethod.ORG_TRAVERSE]:
            for org in self.orgs_config.get_orgs_by_discovery_method(method):
                if self.orgs_config.needs_traversal(org):
                    orgs_to_traverse.append(org)
        return orgs_to_traverse

    def update_org_timestamps(self, org: str, searcher: GitHubSearcher) -> None:
        """Update organization timestamps after traversal."""
        config = self.orgs_config.get_config(org)
        config.last_checked = nowutc()

        # Try to get latest push timestamp
        try:
            # Get the most recently pushed repository
            repos = searcher.get(
                f"/orgs/{org}/repos",
                params={"sort": "pushed", "direction": "desc", "per_page": 1},
            )
            if repos:
                config.updated = datetime.fromisoformat(
                    repos[0]["pushed_at"].replace("Z", "+00:00")
                )
        except Exception as e:
            log.warning(f"Failed to get last push for {org}: {e}")

        # Update repo count
        try:
            org_data = searcher.get(f"/orgs/{org}")
            config.repo_count = org_data.get("public_repos", 0)
        except Exception as e:
            log.warning(f"Failed to get repo count for {org}: {e}")

        self.orgs_config.save()

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
