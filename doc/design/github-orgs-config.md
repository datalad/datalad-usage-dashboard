# GitHub Organizations Configuration Management

**Issue**: [#64 - Do not exclude ReproBrainChart: EXCLUSION_THRESHOLD could be not the only criteria](https://github.com/datalad/datalad-usage-dashboard/issues/64)

## Problem Statement

The current implementation (gh-62) uses a simple threshold-based approach to determine which organizations to exclude from global search and traverse separately. Organizations with more than `EXCLUSION_THRESHOLD` (30) repositories are automatically excluded and searched individually. However, this approach has limitations:

1. **Inflexible control**: Some organizations (like ReproBrainChart) should be traversed explicitly even if they don't meet the threshold
2. **No manual override**: Cannot force inclusion/exclusion of specific organizations
3. **Inefficient re-traversal**: Organizations are re-traversed on every run even if they haven't changed
4. **No persistence**: Organization-specific settings aren't stored between runs

## Current Implementation Review

Based on the implemented hybrid search strategy (gh-62):
- Organizations with >30 repos are automatically excluded from global search
- Excluded organizations are searched individually using org-specific queries
- Fallback to repository enumeration when search returns empty results
- No configuration persistence or manual control

## Proposed Solution: github-orgs.json Configuration

### Configuration File Structure

Create a `github-orgs.json` file to store organization-specific configuration:

```json
{
  "ReproBrainChart": {
    "search_exclude": false,
    "traverse_repos": true,
    "last_checked": "2024-01-15T12:00:00Z",
    "updated": null,
    "repo_count": 25,
    "notes": "Explicitly traverse despite low repo count"
  },
  "dandisets": {
    "search_exclude": true,
    "traverse_repos": true,
    "use_enumeration_fallback": true,
    "last_checked": "2024-01-14T10:30:00Z",
    "updated": "2024-01-14T09:00:00Z",
    "repo_count": 217,
    "notes": "GitHub search returns empty results, use enumeration"
  },
  "OpenNeuroDatasets": {
    "search_exclude": true,
    "traverse_repos": true,
    "last_checked": "2024-01-13T15:45:00Z",
    "updated": "2024-01-13T14:30:00Z",
    "repo_count": 780,
    "notes": "High activity organization, auto-excluded"
  }
}
```

### Field Definitions

- **`search_exclude`** (bool): Whether to exclude from global GitHub search queries
  - `true`: Add `-org:name` to global search
  - `false`: Include in global search results

- **`traverse_repos`** (bool): Whether to explicitly traverse/search this organization's repos
  - `true`: Perform organization-specific search/enumeration
  - `false`: Only discover through global search

- **`use_enumeration_fallback`** (bool, optional): Force repository enumeration instead of search
  - Used for organizations where GitHub search fails (e.g., dandisets)

- **`last_checked`** (ISO 8601 timestamp): When the organization was last fully traversed
  - Consistent with repository's `last_checked` field

- **`updated`** (ISO 8601 timestamp, nullable): Most recent push across all repos
  - Used to detect if organization needs re-traversal
  - `null` if not yet determined
  - Consistent with repository's `updated` field

- **`repo_count`** (int): Number of repositories in the organization
  - Used for statistics and threshold calculations

- **`notes`** (string, optional): Human-readable explanation for configuration

## Implementation Plan

### Phase 1: Configuration Management

#### 1.1 Create OrgConfig Model
```python
# src/find_datalad_repos/github_orgs.py
from datetime import datetime
from pathlib import Path
from typing import Optional
import json
from pydantic import BaseModel, Field

class OrgConfig(BaseModel):
    """Configuration for a GitHub organization"""
    search_exclude: bool = False
    traverse_repos: bool = False
    use_enumeration_fallback: bool = False
    last_checked: Optional[datetime] = None
    updated: Optional[datetime] = None
    repo_count: int = 0
    notes: Optional[str] = None

class GitHubOrgsConfig(BaseModel):
    """Manages GitHub organization configurations"""
    orgs: dict[str, OrgConfig] = Field(default_factory=dict)

    @classmethod
    def load(cls, path: Path = Path("github-orgs.json")) -> "GitHubOrgsConfig":
        """Load configuration from JSON file"""
        if path.exists():
            with open(path) as f:
                data = json.load(f)
                return cls(orgs={
                    name: OrgConfig(**config)
                    for name, config in data.items()
                })
        return cls()

    def save(self, path: Path = Path("github-orgs.json")) -> None:
        """Save configuration to JSON file"""
        data = {
            name: config.model_dump(exclude_none=True)
            for name, config in self.orgs.items()
        }
        with open(path, "w") as f:
            json.dump(data, f, indent=2, default=str)

    def get_config(self, org: str) -> OrgConfig:
        """Get configuration for an organization (creates if not exists)"""
        if org not in self.orgs:
            self.orgs[org] = OrgConfig()
        return self.orgs[org]

    def should_exclude_from_search(self, org: str) -> bool:
        """Check if organization should be excluded from global search"""
        return self.get_config(org).search_exclude

    def should_traverse(self, org: str) -> bool:
        """Check if organization should be explicitly traversed"""
        return self.get_config(org).traverse_repos

    def needs_traversal(self, org: str) -> bool:
        """Check if organization needs re-traversal based on timestamps"""
        config = self.get_config(org)

        # Always traverse if never traversed
        if not config.last_checked:
            return True

        # If we don't have updated, we need to traverse to get it
        if not config.updated:
            return True

        # Re-traverse if pushed_at is newer than last traversal
        return config.updated > config.last_checked
```

#### 1.2 Auto-populate from Current Data
```python
def initialize_orgs_config(
    repos: list[GitHubRepo],
    threshold: int = EXCLUSION_THRESHOLD
) -> GitHubOrgsConfig:
    """Initialize organization config from current repository data"""
    from collections import Counter

    config = GitHubOrgsConfig()

    # Count repos per organization
    org_counts = Counter()
    for repo in repos:
        if not repo.gone:
            org = repo.owner
            org_counts[org] += 1

    # Configure organizations based on threshold
    for org, count in org_counts.items():
        org_config = OrgConfig(
            search_exclude=count >= threshold,
            traverse_repos=count >= threshold,
            repo_count=count,
            notes="Auto-configured based on repository count"
        )
        config.orgs[org] = org_config

    # Add special cases
    if "ReproBrainChart" not in config.orgs:
        config.orgs["ReproBrainChart"] = OrgConfig(
            search_exclude=False,
            traverse_repos=True,
            notes="Explicitly traverse despite low repo count (#64)"
        )

    # Mark organizations that need enumeration fallback
    for org in ["dandisets"]:
        if org in config.orgs:
            config.orgs[org].use_enumeration_fallback = True
            config.orgs[org].notes = "GitHub search returns empty, use enumeration"

    return config
```

### Phase 2: GitHub API Integration for Organization Updates

#### 2.1 Get Organization's Latest Activity
```python
class GitHubSearcher:
    def get_org_last_pushed(self, org: str) -> Optional[datetime]:
        """
        Get the most recent push timestamp across all repositories in an org.

        Note: This requires iterating through all repos as GitHub doesn't
        provide org-level activity timestamps.
        """
        try:
            # Get all repositories sorted by pushed_at
            repos = self.get(
                f"/orgs/{org}/repos",
                params={"sort": "pushed", "direction": "desc", "per_page": 1}
            )

            if repos:
                # Return the pushed_at of the most recently pushed repo
                return datetime.fromisoformat(
                    repos[0]["pushed_at"].replace("Z", "+00:00")
                )
        except Exception as e:
            log.warning(f"Failed to get last push for {org}: {e}")

        return None

    def get_org_repo_count(self, org: str) -> int:
        """Get total number of public repositories in an organization"""
        try:
            org_data = self.get(f"/orgs/{org}")
            return org_data.get("public_repos", 0)
        except Exception:
            return 0
```

### Phase 3: Integration with GitHubUpdater

#### 3.1 Update GitHubUpdater to Use Configuration
```python
class GitHubUpdater(BaseModel, Updater[GitHubRepo, SearchResult, GitHubSearcher]):
    all_repos: dict[int, GitHubRepo]
    noid_repos: list[GitHubRepo]
    orgs_config: GitHubOrgsConfig = Field(default_factory=GitHubOrgsConfig)
    seen: set[int] = Field(default_factory=set)
    new_hits: int = 0
    new_repos: int = 0
    new_runs: int = 0

    @classmethod
    def from_collection(
        cls, host: RepoHost, collection: list[GitHubRepo]
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

        return cls(
            all_repos=all_repos,
            noid_repos=noid_repos,
            orgs_config=orgs_config
        )

    def get_organizations_to_exclude(self) -> list[str]:
        """Get list of organizations to exclude from global search"""
        return [
            org for org, config in self.orgs_config.orgs.items()
            if config.search_exclude
        ]

    def get_organizations_to_traverse(self) -> list[str]:
        """Get list of organizations to explicitly traverse"""
        orgs_to_traverse = []
        for org, config in self.orgs_config.orgs.items():
            if config.traverse_repos and self.orgs_config.needs_traversal(org):
                orgs_to_traverse.append(org)
        return orgs_to_traverse

    def update_org_timestamps(self, org: str, searcher: GitHubSearcher) -> None:
        """Update organization timestamps after traversal"""
        config = self.orgs_config.get_config(org)
        config.last_checked = nowutc()

        # Try to get latest push timestamp
        last_pushed = searcher.get_org_last_pushed(org)
        if last_pushed:
            config.updated = last_pushed

        # Update repo count
        repo_count = searcher.get_org_repo_count(org)
        if repo_count:
            config.repo_count = repo_count

        self.orgs_config.save()

    def get_new_collection(self, searcher: GitHubSearcher) -> list[GitHubRepo]:
        # Get organizations to exclude and traverse
        excluded_orgs = self.get_organizations_to_exclude()
        traverse_orgs = self.get_organizations_to_traverse()

        log.info(f"Excluding {len(excluded_orgs)} orgs from global search")
        log.info(f"Will traverse {len(traverse_orgs)} organizations explicitly")

        # Pass configuration to searcher
        search_results = searcher.get_datalad_repos(
            excluded_orgs=excluded_orgs,
            traverse_orgs=traverse_orgs,
            orgs_config=self.orgs_config,
            known_repos=[
                {"name": r.name, "status": r.status.value}
                for r in self.all_repos.values()
            ]
        )

        # Process results...
        for sr in search_results:
            self.register_repo(sr, searcher)

        # Update timestamps for traversed organizations
        for org in traverse_orgs:
            self.update_org_timestamps(org, searcher)

        # Save configuration with updated timestamps
        self.orgs_config.save()

        # ... rest of implementation
```

#### 3.2 Update GitHubSearcher to Use Configuration
```python
class GitHubSearcher:
    def get_datalad_repos(
        self,
        excluded_orgs: list[str] = None,
        traverse_orgs: list[str] = None,
        orgs_config: GitHubOrgsConfig = None,
        known_repos: list[dict] = None
    ) -> list[SearchResult]:
        """
        Hybrid search with configuration-based control
        """
        excluded_orgs = excluded_orgs or []
        traverse_orgs = traverse_orgs or []
        orgs_config = orgs_config or GitHubOrgsConfig()

        # Build exclusion query
        exclusions = build_exclusion_query(excluded_orgs) if excluded_orgs else ""

        # Phase 1: Global search with exclusions
        log.info("Phase 1: Global searches with exclusions")
        datasets = set(self.search_dataset_repos_with_exclusions(exclusions))
        runcmds = {}

        for repo, container_run in self.search_runcmds_with_exclusions(exclusions):
            runcmds[repo] = container_run or runcmds.get(repo, False)

        # Phase 2: Organization-specific traversal
        log.info("Phase 2: Organization-specific traversal")
        for org in traverse_orgs:
            config = orgs_config.get_config(org)
            log.info(f"Traversing organization: {org}")

            if config.use_enumeration_fallback:
                # Use enumeration for organizations with broken search
                log.info(f"Using enumeration fallback for {org}")
                org_datasets = set(self.enumerate_org_repositories(org, known_repos))
            else:
                # Use search API
                org_datasets = set(self.search_dataset_repos_in_org(org))

            datasets.update(org_datasets)

            # Search for RUNCMD (search API usually works for this)
            if not config.use_enumeration_fallback:
                for repo, container_run in self.search_runcmds_in_org(org):
                    runcmds[repo] = container_run or runcmds.get(repo, False)

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
```

## Benefits

### Immediate Benefits
1. **Manual control**: Can force specific organizations to be traversed (ReproBrainChart)
2. **Configuration persistence**: Settings preserved across runs
3. **Transparent operation**: JSON file documents why organizations are handled differently

### Optimization Benefits
1. **Skip unchanged organizations**: Only re-traverse when updated > last_checked
2. **Reduced API calls**: Avoid unnecessary organization traversals
3. **Faster runs**: Skip organizations that haven't changed

### Future Benefits
1. **Easy maintenance**: Add/remove organizations without code changes
2. **Debugging**: Clear record of organization handling decisions
3. **Extensibility**: Can add more per-organization settings as needed

## Migration Strategy

1. **Initial deployment**:
   - Generate github-orgs.json from current data
   - Add ReproBrainChart with traverse_repos=true
   - Mark dandisets for enumeration fallback

2. **First run**:
   - All organizations will be traversed (no timestamps)
   - Timestamps will be recorded for future optimization

3. **Subsequent runs**:
   - Only traverse organizations with changes
   - Monitor API usage reduction

## Performance Considerations

### API Usage
- Initial run: Same as current implementation
- Subsequent runs: Significantly reduced (skip unchanged orgs)
- Timestamp checks: 1 API call per org (lightweight)

### Storage
- github-orgs.json: ~200 bytes per organization
- For 100 organizations: ~20KB (negligible)

### Processing
- Loading/saving JSON: Minimal overhead
- Timestamp comparisons: O(n) where n = number of orgs

## Future Enhancements

1. **Auto-discovery of problem organizations**:
   - Automatically set use_enumeration_fallback when search returns empty

2. **Smart threshold adjustment**:
   - Dynamically adjust EXCLUSION_THRESHOLD based on total repo count

3. **Per-organization rate limits**:
   - Configure different delays for different organizations

4. **Historical tracking**:
   - Track organization growth over time
   - Predict when organizations will need different handling

## Testing Strategy

1. **Unit tests**: Test configuration load/save/update
2. **Integration tests**: Verify organization traversal logic
3. **Validation**: Compare results with/without optimization
4. **Monitoring**: Track API usage reduction over multiple runs
