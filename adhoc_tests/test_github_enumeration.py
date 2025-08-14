#!/usr/bin/env python3
"""
Test GitHub repository enumeration fallback functionality.

Run with: pytest test_github_enumeration.py -v
Requires: GITHUB_TOKEN environment variable to be set
"""

import os
from unittest.mock import Mock, patch
import pytest
from src.find_datalad_repos.github import GitHubSearcher


class TestEnumerationFallback:
    """Test the enumeration fallback logic"""

    def test_needs_enumeration_fallback_no_known_repos(self):
        """Test fallback detection when no known repos"""
        searcher = GitHubSearcher(token="test")

        # Should not trigger fallback when known_repos is None
        assert not searcher.needs_enumeration_fallback("test-org", [], None)

        # Should not trigger fallback when known_repos is empty
        assert not searcher.needs_enumeration_fallback("test-org", [], [])

    def test_needs_enumeration_fallback_with_results(self):
        """Test fallback detection when search has results"""
        searcher = GitHubSearcher(token="test")

        known_repos = [
            {"name": "test-org/repo1", "status": "active"},
            {"name": "test-org/repo2", "status": "active"},
        ]

        search_results = [{"repository": {"full_name": "test-org/repo1"}}]

        # Should not trigger fallback when search has results
        assert not searcher.needs_enumeration_fallback(
            "test-org", search_results, known_repos
        )

    def test_needs_enumeration_fallback_empty_search(self):
        """Test fallback triggers when search is empty but repos are known"""
        searcher = GitHubSearcher(token="test")

        known_repos = [
            {"name": "test-org/repo1", "status": "active"},
            {"name": "test-org/repo2", "status": "active"},
            {"name": "other-org/repo3", "status": "active"},
        ]

        # Should trigger fallback: empty results but known active repos
        assert searcher.needs_enumeration_fallback("test-org", [], known_repos)

        # Should not trigger for org with no known repos
        assert not searcher.needs_enumeration_fallback("unknown-org", [], known_repos)

    def test_needs_enumeration_fallback_only_gone_repos(self):
        """Test fallback doesn't trigger when all known repos are gone"""
        searcher = GitHubSearcher(token="test")

        known_repos = [
            {"name": "test-org/repo1", "status": "gone"},
            {"name": "test-org/repo2", "status": "gone"},
        ]

        # Should not trigger when all repos are gone
        assert not searcher.needs_enumeration_fallback("test-org", [], known_repos)

    @patch("requests.head")
    def test_check_datalad_config(self, mock_head):
        """Test checking for .datalad/config file"""
        searcher = GitHubSearcher(token="test")

        # Test successful check
        mock_response = Mock()
        mock_response.status_code = 200
        mock_head.return_value = mock_response

        assert searcher.check_datalad_config("owner", "repo", "main")
        mock_head.assert_called_with(
            "https://raw.githubusercontent.com/owner/repo/refs/heads/main/.datalad/config",
            timeout=5,
            allow_redirects=True,
        )

        # Test file not found
        mock_response.status_code = 404
        assert not searcher.check_datalad_config("owner", "repo", "main")

        # Test request exception
        mock_head.side_effect = Exception("Network error")
        assert not searcher.check_datalad_config("owner", "repo", "main")


@pytest.mark.skipif(not os.environ.get("GITHUB_TOKEN"), reason="GITHUB_TOKEN not set")
class TestRealGitHubAPI:
    """Integration tests with real GitHub API"""

    def test_search_datalad_org(self):
        """Test that we can find repos in datalad organization"""
        token = os.environ["GITHUB_TOKEN"]
        searcher = GitHubSearcher(token=token)

        # Search for datalad organization repos
        results = list(searcher.search_dataset_repos_in_org("datalad"))

        # Should find at least some repos
        assert len(results) > 0

        # Check that datalad/datalad is found
        repo_names = {r.name for r in results}
        assert "datalad/datalad" in repo_names or "datalad/datalad-next" in repo_names

    def test_search_dandisets_org_with_fallback(self):
        """Test that fallback works for dandisets organization"""
        token = os.environ["GITHUB_TOKEN"]

        # Simulate known repos
        known_repos = [
            {"name": "dandisets/000003", "status": "active"},
            {"name": "dandisets/000004", "status": "active"},
            {"name": "dandisets/000005", "status": "active"},
        ]

        searcher = GitHubSearcher(token=token, known_repos=known_repos)

        # Search for dandisets organization repos
        # This should trigger fallback since search returns empty
        results = list(searcher.search_dataset_repos_in_org("dandisets", known_repos))

        # Should find repos via enumeration
        assert len(results) > 0

        # Should find some new repos not in our known list
        result_names = {r.name for r in results}
        known_names = {r["name"] for r in known_repos}
        new_repos = result_names - known_names

        print(f"Found {len(new_repos)} new repos via enumeration")
        # We expect to find new repos
        assert len(new_repos) > 0

    def test_check_real_datalad_config(self):
        """Test checking a real .datalad/config file"""
        token = os.environ["GITHUB_TOKEN"]
        searcher = GitHubSearcher(token=token)

        # Check a known DataLad repository
        # Using dandisets/000003 which we know exists
        assert searcher.check_datalad_config("dandisets", "000003", "draft")

        # Check a repo that shouldn't have .datalad/config
        assert not searcher.check_datalad_config("torvalds", "linux", "master")

    def test_enumerate_small_org(self):
        """Test enumerating repositories in a small organization"""
        token = os.environ["GITHUB_TOKEN"]
        searcher = GitHubSearcher(token=token)

        # Use a small org to avoid rate limiting
        # Note: This might need adjustment based on available test orgs
        repos = list(searcher.enumerate_org_repositories("datalad"))

        # Should find multiple repositories
        assert len(repos) > 5

        # Each repo should have expected fields
        for repo in repos[:3]:  # Check first 3
            assert "name" in repo
            assert "id" in repo
            assert "html_url" in repo
            assert "default_branch" in repo


if __name__ == "__main__":
    # Run tests
    import sys

    pytest.main([__file__, "-v"] + sys.argv[1:])
