# Implementation Plan: Repository Enumeration Fallback for GitHub Search

## Overview
Implement a three-tier search strategy to handle GitHub search API limitations where certain organizations (e.g., "dandisets") return empty search results despite having DataLad repositories.

## Implementation Steps

### Step 1: Add Helper Methods to GitHubSearcher
**File**: `src/find_datalad_repos/github.py`

#### 1.1 Add method to detect when fallback is needed
```python
def needs_enumeration_fallback(
    self, org: str, search_results: list, known_repos: list[dict]
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
    # Count active known repos for this org
    active_known = [
        r for r in known_repos
        if r['name'].startswith(f"{org}/") and r.get('status') != 'gone'
    ]

    # Use fallback if search returns empty but we have active repos
    if len(search_results) == 0 and len(active_known) > 0:
        log.warning(
            f"Organization {org} returned 0 search results but has "
            f"{len(active_known)} known active repos - using enumeration fallback"
        )
        return True

    return False
```

#### 1.2 Add repository enumeration method
```python
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
                params={"per_page": "100", "page": str(page), "type": "all"}
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
```

#### 1.3 Add DataLad config checking method
```python
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
```

#### 1.4 Add method to process enumerated repositories
```python
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
        default_branch = repo_data.get('default_branch', 'main')

        # Check for .datalad/config
        if self.check_datalad_config(org, repo_data['name'], default_branch):
            found_count += 1
            log.info(f"Found new DataLad repo via enumeration: {repo_fullname}")

            yield SearchResult(
                id=repo_data['id'],
                url=repo_data['html_url'],
                name=repo_fullname,
                dataset=True,
                run=False,  # Can't detect RUNCMD without searching commits
                container_run=False
            )

    log.info(
        f"Enumeration complete for {org}: checked {checked_count} unknown repos, "
        f"found {found_count} DataLad datasets"
    )
```

### Step 2: Update Organization Search Method
**File**: `src/find_datalad_repos/github.py`

Update the `search_dataset_repos_in_org` method to support fallback:

```python
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

    # Collect search results
    search_results = []
    for hit in self.search("code", query):
        search_results.append(hit)

    # Check if we need enumeration fallback
    if known_repos and self.needs_enumeration_fallback(org, search_results, known_repos):
        # Get known repo names for this org (active only)
        known_names = {
            r['name'] for r in known_repos
            if r['name'].startswith(f"{org}/") and r.get('status') != 'gone'
        }

        # Enumerate and check unknown repositories
        for result in self.process_enumerated_repos(org, known_names):
            # Convert SearchResult to dict format matching search API
            search_results.append({
                "repository": {
                    "id": result.id,
                    "html_url": result.url,
                    "full_name": result.name
                }
            })

    # Yield results in consistent format
    for hit in search_results:
        repo = SearchHit.from_repository(hit["repository"])
        log.info("Found %s", repo.name)
        yield repo
```

### Step 3: Update get_datalad_repos Method
**File**: `src/find_datalad_repos/github.py`

Add known_repos parameter:

```python
def get_datalad_repos(
    self,
    excluded_orgs: list[str] | None = None,
    known_repos: list[dict] | None = None  # NEW PARAMETER
) -> list[SearchResult]:
    """
    Hybrid search: global search with exclusions + targeted org searches.

    Args:
        excluded_orgs: Organizations to exclude from global search
        known_repos: Known repositories for fallback detection
    """
    # ... existing code ...

    # Phase 2: Organization-specific searches
    if excluded_orgs:
        log.info("Phase 2: Organization-specific searches")
        for org in excluded_orgs:
            log.info(f"Searching within organization: {org}")

            # Add datasets from this org WITH FALLBACK SUPPORT
            org_datasets = set(
                self.search_dataset_repos_in_org(org, known_repos=known_repos)
            )
            datasets.update(org_datasets)

            # ... rest of existing code ...
```

### Step 4: Update GitHubUpdater to Pass Known Repos
**File**: `src/find_datalad_repos/github.py`

Modify GitHubUpdater class:

```python
class GitHubUpdater(BaseModel, Updater[GitHubRepo, SearchResult, GitHubSearcher]):
    # ... existing fields ...

    def get_searcher(self, **kwargs: Any) -> GitHubSearcher:
        # Store reference to known repos for later use
        self._known_repos = [
            {'name': r.name, 'status': r.status.value}
            for r in self.all_repos.values()
        ]
        return GitHubSearcher(excluded_orgs=self.excluded_orgs, **kwargs)

    def register_repo(self, sr: SearchResult, searcher: GitHubSearcher) -> None:
        # ... existing implementation ...
        pass

    def get_new_collection(self, searcher: GitHubSearcher) -> list[GitHubRepo]:
        # Monkey-patch the searcher with known repos
        # (Alternative: modify get_datalad_repos call signature)
        original_get_datalad_repos = searcher.get_datalad_repos

        def get_datalad_repos_with_known(excluded_orgs=None):
            return original_get_datalad_repos(
                excluded_orgs=excluded_orgs,
                known_repos=self._known_repos
            )

        searcher.get_datalad_repos = get_datalad_repos_with_known

        # ... rest of existing implementation ...
```

### Step 5: Add Configuration for Fallback
**File**: `src/find_datalad_repos/config.py`

Add configuration options:

```python
# Organizations known to have GitHub search issues
ENUMERATION_FALLBACK_ORGS = {
    "dandisets",  # Known to return 0 results despite having 200+ repos
}

# Minimum active repos to trigger fallback (prevents unnecessary enumeration)
ENUMERATION_FALLBACK_MIN_REPOS = 10

# Rate limiting for enumeration operations
ENUMERATION_PAGE_DELAY = 2  # Seconds between pagination requests
ENUMERATION_CHECK_DELAY = 0.1  # Seconds between config checks
```

### Step 6: Add Import for requests
**File**: `src/find_datalad_repos/github.py`

Add at the top of the file:
```python
import requests
```

## Testing Plan

### Unit Tests
Create `test_enumeration_fallback.py`:

```python
def test_needs_enumeration_fallback():
    """Test fallback detection logic"""
    searcher = GitHubSearcher(token="test")

    # Should trigger fallback: empty results but known repos
    assert searcher.needs_enumeration_fallback(
        "dandisets",
        [],
        [{"name": "dandisets/000003", "status": "active"}]
    )

    # Should not trigger: has search results
    assert not searcher.needs_enumeration_fallback(
        "dandisets",
        [{"repository": {"full_name": "dandisets/000003"}}],
        [{"name": "dandisets/000003", "status": "active"}]
    )

    # Should not trigger: no known active repos
    assert not searcher.needs_enumeration_fallback(
        "dandisets",
        [],
        [{"name": "dandisets/000003", "status": "gone"}]
    )
```

### Integration Test
Create `test_dandisets_enumeration.py`:

```python
def test_dandisets_enumeration():
    """Test that enumeration finds dandisets repos"""
    searcher = GitHubSearcher(token=os.environ["GITHUB_TOKEN"])

    # Known repos to simulate existing data
    known_repos = [
        {"name": "dandisets/000003", "status": "active"},
        {"name": "dandisets/000004", "status": "active"},
    ]

    # Search with fallback
    results = list(searcher.search_dataset_repos_in_org("dandisets", known_repos))

    # Should find repos via enumeration
    assert len(results) > 0

    # Check that we found some new repos not in known_repos
    result_names = {r.name for r in results}
    known_names = {r["name"] for r in known_repos}
    new_repos = result_names - known_names

    print(f"Found {len(new_repos)} new repos via enumeration")
    assert len(new_repos) > 0
```

## Rollout Plan

### Phase 1: Limited Testing (Week 1)
- Implement core enumeration functionality
- Test with "dandisets" organization only
- Monitor API usage and performance

### Phase 2: Gradual Expansion (Week 2)
- Add more organizations to ENUMERATION_FALLBACK_ORGS as needed
- Implement caching for enumeration results
- Add metrics collection

### Phase 3: Full Deployment (Week 3)
- Enable automatic fallback detection for all organizations
- Add RUNCMD detection for enumerated repos
- Documentation and monitoring

## Performance Metrics to Track

1. **API Usage**:
   - Number of enumeration API calls per organization
   - Rate limit consumption

2. **Discovery Metrics**:
   - New repositories found via enumeration
   - False positives (non-DataLad repos checked)
   - Time spent on enumeration vs search

3. **Error Rates**:
   - Failed enumeration attempts
   - Timeout errors on config checks

## Risk Mitigation

1. **Rate Limiting**:
   - Add configurable delays between requests
   - Implement exponential backoff on 429 errors

2. **Performance Impact**:
   - Only use fallback for known problematic organizations initially
   - Add timeout limits for enumeration operations

3. **Data Quality**:
   - Log all enumeration activities for audit
   - Validate discovered repos match expected patterns

## Success Criteria

- Successfully discover 50+ new repositories in "dandisets" organization
- No increase in API rate limit violations
- Total runtime increase less than 20%
- Zero false positives (all discovered repos are valid DataLad datasets)
