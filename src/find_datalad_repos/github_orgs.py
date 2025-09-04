"""GitHub organization configuration management."""
from __future__ import annotations
from enum import Enum
import json
from pathlib import Path
from typing import Optional
from pydantic import BaseModel, Field, validator
from .config import EXCLUSION_THRESHOLD, GITHUB_ORGS_FILE
from .util import log


class DiscoveryMethod(str, Enum):
    """How to discover DataLad repositories in an organization."""

    GLOBAL_SEARCH = "global_search"  # Default: included in global search queries
    ORG_SEARCH = "org_search"  # Org-specific search with auto-fallback
    ORG_TRAVERSE = "org_traverse"  # Always enumerate all repos (no search)


class OrgConfig(BaseModel):
    """Configuration for a GitHub organization with new discovery method system."""

    # Discovery method configuration
    discovery_method: Optional[DiscoveryMethod] = None
    search_exclude: Optional[bool] = None

    @validator("search_exclude")
    @classmethod
    def validate_search_exclude(cls, v: bool | None, values: dict) -> bool | None:
        """Validate search_exclude compatibility with discovery method."""
        discovery = values.get("discovery_method")
        if v is True and discovery == DiscoveryMethod.GLOBAL_SEARCH:
            raise ValueError(
                "Cannot set search_exclude=True with "
                "discovery_method='global_search'. "
                "Use 'org_search' or 'org_traverse' instead."
            )
        return v

    @property
    def effective_discovery_method(self) -> DiscoveryMethod:
        """Get the effective discovery method."""
        return self.discovery_method or DiscoveryMethod.GLOBAL_SEARCH

    @property
    def effective_search_exclude(self) -> bool:
        """Get the effective search_exclude value."""
        return self.search_exclude or False


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

            # Skip organizations that only have default values
            is_default = config_dict.get("discovery_method") in [
                None,
                "global_search",
            ] and config_dict.get("search_exclude") in [None, False]

            if is_default:
                # Skip completely default organizations
                continue

            # Remove default values to minimize file size
            if config_dict.get("discovery_method") == "global_search":
                config_dict.pop("discovery_method", None)
            if config_dict.get("search_exclude") is False:
                config_dict.pop("search_exclude", None)

            # Only save if there's something meaningful left
            if config_dict:
                data[name] = config_dict

        with open(path, "w") as f:
            json.dump(data, f, indent=2)
            f.write("\n")  # Add trailing newline

    def get_config(self, org: str) -> OrgConfig:
        """Get configuration for an organization (creates if not exists)."""
        if org not in self.orgs:
            self.orgs[org] = OrgConfig()
        return self.orgs[org]

    def get_orgs_by_discovery_method(self, method: DiscoveryMethod) -> list[str]:
        """Get organizations using a specific discovery method."""
        return [
            org
            for org, config in self.orgs.items()
            if config.effective_discovery_method == method
        ]

    def get_excluded_orgs(self) -> list[str]:
        """Get organizations excluded from global search."""
        return [
            org for org, config in self.orgs.items() if config.effective_search_exclude
        ]

    def should_exclude_from_search(self, org: str) -> bool:
        """Check if organization should be excluded from global search."""
        return self.get_config(org).effective_search_exclude

    def should_traverse(self, org: str) -> bool:
        """Check if organization should be explicitly processed (legacy method)."""
        config = self.get_config(org)
        return config.effective_discovery_method in [
            DiscoveryMethod.ORG_SEARCH,
            DiscoveryMethod.ORG_TRAVERSE,
        ]

    def needs_traversal(self, org: str) -> bool:
        """Check if organization needs traversal.

        Note: Timestamp-based checks have been removed as timestamps
        are tracked in individual repository records.
        """
        config = self.get_config(org)

        # Only applies to orgs that are actually traversed
        return config.effective_discovery_method in [
            DiscoveryMethod.ORG_SEARCH,
            DiscoveryMethod.ORG_TRAVERSE,
        ]


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

    # Configure organizations based on threshold using new discovery method system
    for org, count in org_counts.items():
        if count >= threshold:
            # Large orgs use org_search (with auto-fallback)
            org_config = OrgConfig(
                discovery_method=DiscoveryMethod.ORG_SEARCH,
                search_exclude=True,
            )
        else:
            # Small orgs use default (global_search) - no config needed
            org_config = OrgConfig()
        config.orgs[org] = org_config

    # Add special cases with new system
    special_cases: dict[str, dict] = {
        "ReproBrainChart": {
            "discovery_method": DiscoveryMethod.ORG_SEARCH,
        },
        "dandisets": {
            "discovery_method": DiscoveryMethod.ORG_TRAVERSE,
            "search_exclude": True,
        },
        "OpenNeuroDatasets": {
            "discovery_method": DiscoveryMethod.ORG_TRAVERSE,
            "search_exclude": True,
        },
    }

    for org, special_config in special_cases.items():
        if org in config.orgs:
            # Update existing config
            for key, value in special_config.items():
                setattr(config.orgs[org], key, value)
        else:
            # Create new config
            config.orgs[org] = OrgConfig(**special_config)

    log.info(
        f"Initialized configuration for {len(config.orgs)} organizations "
        f"(global_search: "
        f"{len(config.get_orgs_by_discovery_method(DiscoveryMethod.GLOBAL_SEARCH))}, "
        f"org_search: "
        f"{len(config.get_orgs_by_discovery_method(DiscoveryMethod.ORG_SEARCH))}, "
        f"org_traverse: "
        f"{len(config.get_orgs_by_discovery_method(DiscoveryMethod.ORG_TRAVERSE))})"
    )

    return config
