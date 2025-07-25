from __future__ import annotations
from collections import Counter
from datetime import datetime, timezone
from enum import Enum
import json
import logging
from pathlib import Path
import platform
import re
import shlex
import subprocess
import sys
from typing import Any, Union, Sequence
import requests

USER_AGENT = "find_datalad_repos ({}) requests/{} {}/{}".format(
    "https://github.com/datalad/datalad-usage-dashboard",
    requests.__version__,
    platform.python_implementation(),
    platform.python_version(),
)

log = logging.getLogger(__package__)


class Status(Enum):
    ACTIVE = "active"
    GONE = "gone"


def is_container_run(commit_msg: str) -> bool:
    m = re.search(
        r"^=== Do not change lines below ===$(.+)"
        r"^\^\^\^ Do not change lines above \^\^\^$",
        commit_msg,
        flags=re.M | re.S,
    )
    if m is None:
        return False
    try:
        metadata = json.loads(m[1])
    except ValueError:
        return False
    try:
        return bool(metadata.get("extra_inputs"))
    except AttributeError:
        # Apparently there are some commits with strings for the RUNCMD
        # metadata?
        return False


def check(yesno: bool) -> str:
    # return '\u2714\uFE0F' if yesno else ''
    return ":heavy_check_mark:" if yesno else ""


def runcmd(*args: str | Path, **kwargs: Any) -> None:
    log.debug("Running: %s", " ".join(shlex.quote(str(a)) for a in args))
    r = subprocess.run(args, **kwargs)
    if r.returncode != 0:
        sys.exit(r.returncode)


def commit(msg: str) -> None:
    if subprocess.run(["git", "diff", "--cached", "--quiet"]).returncode != 0:
        runcmd("git", "commit", "-m", msg)
    else:
        log.info("Nothing to commit")


def nowutc() -> datetime:
    return datetime.now(timezone.utc)


def get_organizations_for_exclusion(
    current_repos: Sequence[Union[dict, Any]], threshold: int = 30
) -> list[str]:
    """
    Get organizations with >threshold repos to exclude from global search.

    Args:
        current_repos: List of repository objects (dict or GitHubRepo objects)
        threshold: Minimum number of repositories for exclusion

    Returns:
        List of organization names to exclude
    """
    org_counts: Counter[str] = Counter()

    for repo in current_repos:
        if isinstance(repo, dict):
            # Handle dict format from JSON
            org = repo['name'].split('/')[0]
        else:
            # Handle GitHubRepo objects
            org = repo.name.split('/')[0]
        org_counts[org] += 1

    excluded_orgs = [org for org, count in org_counts.items() if count >= threshold]
    log.info(f"Found {len(excluded_orgs)} organizations with >={threshold} repos for exclusion")

    return excluded_orgs


def build_exclusion_query(orgs: list[str], max_length: int = 1000) -> str:
    """
    Build -org:name1 -org:name2... string within GitHub query limits.

    Args:
        orgs: List of organization names to exclude
        max_length: Maximum query string length

    Returns:
        Exclusion query string
    """
    if not orgs:
        return ""

    exclusions: list[str] = []
    query_length = 0

    # Sort by name for consistent ordering
    for org in sorted(orgs):
        addition = f" -org:{org}"
        if query_length + len(addition) > max_length:
            log.warning(f"Exclusion query length limit reached, excluding {len(orgs) - len(exclusions)} organizations")
            break
        exclusions.append(addition)
        query_length += len(addition)

    exclusion_str = "".join(exclusions)
    log.info(f"Built exclusion query with {len(exclusions)} organizations ({len(exclusion_str)} chars)")

    return exclusion_str
