#!/usr/bin/env python3
from collections import Counter
import json
import os
from time import sleep
import requests

# Load current repository data
with open("datalad-repos.json") as f:
    data = json.load(f)

# Count current repos per organization
current_repos = Counter()
for repo in data["github"]:
    org = repo["name"].split("/")[0]
    current_repos[org] += 1

# Organizations to test
orgs_to_test = [
    "dandisets",
    "OpenNeuroDatasets",
    "OpenNeuroDatasets-JSONLD",
    "datasets-mila",
    "OpenNeuroDerivatives",
]

# GitHub API setup
token = os.environ.get("GITHUB_TOKEN")
if not token:
    print("GITHUB_TOKEN environment variable not set")
    exit(1)

headers = {
    "Authorization": f"token {token}",
    "Accept": "application/vnd.github.v3+json",
    "User-Agent": "find-datalad-repos",
}


def search_github(query, search_type="code"):
    url = f"https://api.github.com/search/{search_type}"
    params = {"q": query, "per_page": 100}
    results = []

    while True:
        response = requests.get(url, headers=headers, params=params)
        if response.status_code != 200:
            print(f"API error: {response.status_code} - {response.text}")
            break

        data = response.json()
        results.extend(data["items"])

        # Check for next page
        if "next" in response.links:
            url = response.links["next"]["url"]
            params = None
        else:
            break

        sleep(2)  # Rate limiting

    return results


for org in orgs_to_test:
    print(f"\n=== Testing {org} organization ===")
    print(f"Currently indexed: {current_repos[org]} repositories")

    print("Searching for .datalad/config files...")
    try:
        datasets = search_github(f"org:{org} path:.datalad filename:config", "code")
        print(f"Found {len(datasets)} .datalad/config files")
    except Exception as e:
        print(f"Error searching for datasets: {e}")
        datasets = []

    print("Searching for DATALAD RUNCMD commits...")
    try:
        runs = search_github(f'org:{org} "DATALAD RUNCMD" merge:false', "commits")
        print(f"Found {len(runs)} DATALAD RUNCMD commits")
    except Exception as e:
        print(f"Error searching for runs: {e}")
        runs = []

    # Get unique repositories from both searches
    dataset_repos = set()
    for hit in datasets:
        dataset_repos.add(hit["repository"]["full_name"])

    run_repos = set()
    for hit in runs:
        run_repos.add(hit["repository"]["full_name"])

    total_unique = len(dataset_repos | run_repos)
    print(f"Total unique repositories found: {total_unique}")
    print(
        f"Gap: {total_unique - current_repos[org]} additional repositories discoverable"
    )
