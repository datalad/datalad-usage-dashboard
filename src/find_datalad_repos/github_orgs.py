"""GitHub organization configuration management."""
from __future__ import annotations
from datetime import datetime
import json
from pathlib import Path
from typing import Optional
from pydantic import BaseModel, Field
from .config import EXCLUSION_THRESHOLD, GITHUB_ORGS_FILE
from .util import log


class OrgConfig(BaseModel):
    """Configuration for a GitHub organization."""

    search_exclude: bool = False
    traverse_repos: bool = False
    use_enumeration_fallback: bool = False
    # Most recent push across all repos (like repo.updated)
    updated: Optional[datetime] = None
    # When we last traversed this org (like repo.last_checked)
    last_checked: Optional[datetime] = None
    repo_count: int = 0
    notes: Optional[str] = None


class GitHubOrgsConfig(BaseModel):
    """Manages GitHub organization configurations."""

    orgs: dict[str, OrgConfig] = Field(default_factory=dict)

    @classmethod
    def load(cls, path: Optional[Path] = None) -> GitHubOrgsConfig:
        """Load configuration from JSON file."""
        if path is None:
            path = Path(GITHUB_ORGS_FILE)
        if path.exists():
            with open(path) as f:
                data = json.load(f)
                return cls(
                    orgs={name: OrgConfig(**config) for name, config in data.items()}
                )
        return cls()

    def save(self, path: Optional[Path] = None) -> None:
        """Save configuration to JSON file."""
        if path is None:
            path = Path(GITHUB_ORGS_FILE)

        data = {}
        for name, config in self.orgs.items():
            config_dict = config.model_dump(exclude_none=True)
            # Convert datetime objects to ISO format strings
            if "updated" in config_dict and config_dict["updated"]:
                config_dict["updated"] = config_dict["updated"].isoformat()
            if "last_checked" in config_dict and config_dict["last_checked"]:
                config_dict["last_checked"] = config_dict["last_checked"].isoformat()
            data[name] = config_dict

        with open(path, "w") as f:
            json.dump(data, f, indent=2)

    def get_config(self, org: str) -> OrgConfig:
        """Get configuration for an organization (creates if not exists)."""
        if org not in self.orgs:
            self.orgs[org] = OrgConfig()
        return self.orgs[org]

    def should_exclude_from_search(self, org: str) -> bool:
        """Check if organization should be excluded from global search."""
        return self.get_config(org).search_exclude

    def should_traverse(self, org: str) -> bool:
        """Check if organization should be explicitly traversed."""
        return self.get_config(org).traverse_repos

    def needs_traversal(self, org: str) -> bool:
        """Check if organization needs re-traversal based on timestamps."""
        config = self.get_config(org)

        # Always traverse if explicitly configured but never traversed
        if config.traverse_repos and not config.last_checked:
            log.info(f"Organization {org} needs traversal: never traversed before")
            return True

        # Don't traverse if not configured to
        if not config.traverse_repos:
            return False

        # If we don't have updated timestamp, we need to traverse to get it
        if not config.updated:
            log.info(f"Organization {org} needs traversal: no updated timestamp")
            return True

        # Re-traverse if updated is newer than last_checked
        if config.last_checked and config.updated > config.last_checked:
            log.info(
                f"Organization {org} needs traversal: "
                f"updated {config.updated} > "
                f"last_checked {config.last_checked}"
            )
            return True

        return False


def initialize_orgs_config(
    repos: list, threshold: int = EXCLUSION_THRESHOLD  # GitHubRepo objects
) -> GitHubOrgsConfig:
    """Initialize organization config from current repository data."""
    from collections import Counter

    config = GitHubOrgsConfig()

    # Count repos per organization
    org_counts: Counter[str] = Counter()
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
            notes="Auto-configured based on repository count",
        )
        config.orgs[org] = org_config

    # Add special case for ReproBrainChart (issue #64)
    if "ReproBrainChart" not in config.orgs:
        config.orgs["ReproBrainChart"] = OrgConfig(
            search_exclude=False,
            traverse_repos=True,
            notes="Explicitly traverse despite low repo count (#64)",
        )
    elif not config.orgs["ReproBrainChart"].traverse_repos:
        # Update existing config to ensure traversal
        config.orgs["ReproBrainChart"].traverse_repos = True
        config.orgs[
            "ReproBrainChart"
        ].notes = "Explicitly traverse despite low repo count (#64)"

    # Mark organizations that need enumeration fallback
    for org in ["dandisets"]:
        if org in config.orgs:
            config.orgs[org].use_enumeration_fallback = True
            if not config.orgs[org].notes:
                config.orgs[org].notes = "GitHub search returns empty, use enumeration"

    log.info(
        f"Initialized configuration for {len(config.orgs)} organizations "
        f"({sum(1 for c in config.orgs.values() if c.traverse_repos)} "
        f"to traverse)"
    )

    return config
