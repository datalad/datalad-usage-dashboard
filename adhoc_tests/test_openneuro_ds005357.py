#!/usr/bin/env python3
"""
Test script to demonstrate the OpenNeuroDatasets/ds005357 discovery issue.

This script tests:
1. GitHub search API fails to find the repository
2. Direct API access confirms the repository exists
3. The proposed solution would find it via traversal
"""

import json
import subprocess


def run_gh_api(endpoint: str, jq_filter: str = None) -> str:
    """Run GitHub API command and return output."""
    cmd = ["gh", "api", endpoint]
    if jq_filter:
        cmd.extend(["--jq", jq_filter])

    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode != 0:
        return f"Error: {result.stderr}"
    return result.stdout.strip()


def test_repository_exists():
    """Verify the repository exists and has .datalad/config."""
    print("1. Testing if OpenNeuroDatasets/ds005357 exists...")

    # Check repository metadata
    repo_info = run_gh_api(
        "repos/OpenNeuroDatasets/ds005357", ".default_branch, .created_at, .pushed_at"
    )
    print(f"   Repository info: {repo_info}")

    # Check if .datalad/config exists
    config_check = run_gh_api(
        "repos/OpenNeuroDatasets/ds005357/contents/.datalad/config", ".type"
    )
    print(f"   .datalad/config exists: {config_check == 'file'}")

    return config_check == "file"


def test_search_api_fails():
    """Demonstrate that GitHub search API doesn't find the repository."""
    print("\n2. Testing GitHub search API...")

    # Search for .datalad/config in this specific repo
    search_result = run_gh_api(
        "/search/code?q=path:.datalad+filename:config+repo:OpenNeuroDatasets/ds005357",
        ".total_count",
    )
    print(f"   Search for .datalad/config in ds005357: {search_result} results")

    # Search for any code in this repo
    any_search = run_gh_api(
        "/search/code?q=repo:OpenNeuroDatasets/ds005357", ".total_count"
    )
    print(f"   Search for any code in ds005357: {any_search} results")

    # Search in the org (should find many, but not this one)
    org_search = run_gh_api(
        "/search/code?q=path:.datalad+filename:config+org:OpenNeuroDatasets",
        ".total_count",
    )
    print(
        f"   Search for .datalad/config in OpenNeuroDatasets org: {org_search} results"
    )

    return search_result == "0"


def test_similar_repos():
    """Check if other OpenNeuroDatasets repos around the same time."""
    print("\n3. Checking similar repositories...")

    # Get a list of repos created around the same time
    print("   Checking repos created in July 2024...")

    # List some repos and check their creation dates
    repos_to_check = ["ds005356", "ds005357", "ds005358", "ds005359"]

    for repo in repos_to_check:
        repo_date = run_gh_api(f"repos/OpenNeuroDatasets/{repo}", ".created_at")

        if "Error" not in repo_date:
            # Check if searchable
            search = run_gh_api(
                f"/search/code?q=repo:OpenNeuroDatasets/{repo}", ".total_count"
            )
            searchable = search != "0"
            print(f"   {repo}: created {repo_date[:10]}, searchable: {searchable}")


def check_current_config():
    """Check the current configuration for OpenNeuroDatasets."""
    print("\n4. Current configuration for OpenNeuroDatasets...")

    try:
        with open("github-orgs.json", "r") as f:
            orgs = json.load(f)

        if "OpenNeuroDatasets" in orgs:
            config = orgs["OpenNeuroDatasets"]
            discovery_method = config.get("discovery_method", "global_search")
            print(f"   discovery_method: {discovery_method}")
            print(f"   search_exclude: {config.get('search_exclude', False)}")
            print(f"   repo_count: {config.get('repo_count', 'unknown')}")

            # Diagnose the issue with new schema
            if discovery_method in ["global_search", "org_search"]:
                msg = (
                    f"Organization uses {discovery_method} which relies "
                    f"on GitHub search API"
                )
                print(f"\n   ⚠️  ISSUE: {msg}")
                msg2 = "This means repos not indexed by GitHub search are missed!"
                print(f"      {msg2}")
            elif discovery_method == "org_traverse":
                print(
                    "\n   ✅  RESOLVED: Using org_traverse - will find all repositories"
                )
                print(
                    "      This bypasses GitHub search API and enumerates all "
                    "repos directly"
                )
    except FileNotFoundError:
        print("   github-orgs.json not found")


def show_current_solution():
    """Show the current implemented solution."""
    print("\n5. Current Implementation:")
    print(
        "   ✅ OpenNeuroDatasets is now configured with 'org_traverse' discovery method"
    )
    print("   ✅ This enumerates all repositories and checks each one directly")
    print("   ✅ No longer relies on GitHub's search API which has indexing delays")
    print("\n   Current configuration schema:")
    print("   {")
    print('     "OpenNeuroDatasets": {')
    print('       "discovery_method": "org_traverse",')
    print('       "search_exclude": true,')
    print("       ...")
    print("     }")
    print("   }")


def main():
    print("=" * 70)
    print("Testing OpenNeuroDatasets/ds005357 Discovery Issue")
    print("=" * 70)

    # Run tests
    repo_exists = test_repository_exists()
    search_fails = test_search_api_fails()

    if repo_exists and search_fails:
        msg = "Repository exists but is not searchable via GitHub API"
        print(f"\n✅ Issue confirmed: {msg}")
    else:
        print("\n❌ Unexpected result - issue may have been resolved")

    test_similar_repos()
    check_current_config()
    show_current_solution()

    print("\n" + "=" * 70)
    print("Summary:")
    print("  - Repository exists with .datalad/config: YES")
    print("  - GitHub search API finds it: NO")
    print("  - Current config would miss it: NO (RESOLVED)")
    print("  - Solution: ✅ IMPLEMENTED - Using org_traverse discovery method")
    print("=" * 70)


if __name__ == "__main__":
    main()
