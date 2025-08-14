#!/usr/bin/env python3
"""
Test GitHub repository enumeration fallback functionality.

Run with: pytest test_github_fallback.py -v
Requires: GITHUB_TOKEN environment variable for integration tests
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
    def test_check_datalad_config_success(self, mock_head):
        """Test successful checking for .datalad/config file"""
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

    @patch("requests.head")
    def test_check_datalad_config_not_found(self, mock_head):
        """Test when .datalad/config file is not found"""
        searcher = GitHubSearcher(token="test")

        # Test file not found
        mock_response = Mock()
        mock_response.status_code = 404
        mock_head.return_value = mock_response

        assert not searcher.check_datalad_config("owner", "repo", "main")

    @patch("requests.head")
    def test_check_datalad_config_exception(self, mock_head):
        """Test handling of request exceptions"""
        searcher = GitHubSearcher(token="test")

        # Test request exception
        mock_head.side_effect = Exception("Network error")
        assert not searcher.check_datalad_config("owner", "repo", "main")


@pytest.mark.skipif(
    not os.environ.get("GITHUB_TOKEN"),
    reason="GITHUB_TOKEN not set - skipping integration tests",
)
class TestRealGitHubAPI:
    """Integration tests with real GitHub API"""

    @pytest.fixture
    def github_token(self):
        """Get GitHub token from environment or ghtoken"""
        token = os.environ.get("GITHUB_TOKEN")
        if not token:
            try:
                import subprocess

                token = subprocess.check_output(["ghtoken"], text=True).strip()
            except Exception:
                pytest.skip("No GitHub token available")
        return token

    def test_search_datalad_org(self, github_token):
        """Test that we can find repos in datalad organization"""
        searcher = GitHubSearcher(token=github_token)

        # Search for datalad organization repos
        results = list(searcher.search_dataset_repos_in_org("datalad"))

        # Should find at least some repos
        assert len(results) > 0

        # Check that known repos are found
        repo_names = {r.name for r in results}
        assert any("datalad" in name for name in repo_names)

    def test_search_dandisets_org_with_fallback(self, github_token):
        """Test that fallback works for dandisets organization"""
        # Simulate known repos
        known_repos = [
            {"name": "dandisets/000003", "status": "active"},
            {"name": "dandisets/000004", "status": "active"},
            {"name": "dandisets/000005", "status": "active"},
        ]

        searcher = GitHubSearcher(token=github_token, known_repos=known_repos)

        # Search for dandisets organization repos (limiting enumeration for test)
        results = []
        for i, repo in enumerate(
            searcher.search_dataset_repos_in_org("dandisets", known_repos)
        ):
            results.append(repo)
            # Limit to first 10 results for testing
            if i >= 9:
                break

        # Should find repos via enumeration
        assert len(results) > 0

        # Should find some repos (known or new)
        result_names = {r.name for r in results}
        assert any("dandisets/" in name for name in result_names)

    def test_check_real_datalad_config(self, github_token):
        """Test checking a real .datalad/config file"""
        searcher = GitHubSearcher(token=github_token)

        # Check a known DataLad repository
        # Using dandisets/000003 which we know exists
        assert searcher.check_datalad_config("dandisets", "000003", "draft")

        # Check a repo that shouldn't have .datalad/config
        assert not searcher.check_datalad_config("torvalds", "linux", "master")


if __name__ == "__main__":
    # Run tests
    import sys

    pytest.main([__file__, "-v"] + sys.argv[1:])
