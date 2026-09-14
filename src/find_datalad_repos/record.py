from __future__ import annotations
from collections.abc import Sequence
from typing import Any
from pydantic import BaseModel, Field
from .core import RepoHost, S, T, U, Updater
from .gin import GINRepo, GINUpdater
from .github import GitHubRepo, GitHubUpdater
from .osf import OSFRepo, OSFUpdater

# A run may legitimately retire a few repos, but a jump much larger than
# that means the host was unhealthy rather than emptied.  Refuse to write
# such a result: the caller isolates the host and the rest of the run
# still commits.
GONE_FLIP_FLOOR = 10
GONE_FLIP_FRACTION = 0.05


class RepoRecord(BaseModel):
    github: list[GitHubRepo] = Field(default_factory=list)
    osf: list[OSFRepo] = Field(default_factory=list)
    gin: list[GINRepo] = Field(default_factory=list)
    hub_datalad_org: list[GINRepo] = Field(default_factory=list)
    atris: list[GINRepo] = Field(default_factory=list)

    def update_github(self, token: str) -> list[str]:
        return update_collection(
            self.github, RepoHost.GITHUB, GitHubUpdater, token=token
        )

    def update_osf(self) -> list[str]:
        return update_collection(self.osf, RepoHost.OSF, OSFUpdater)

    def update_gin(self, token: str) -> list[str]:
        return update_collection(self.gin, RepoHost.GIN, GINUpdater, token=token)

    def update_hub_datalad_org(self, token: str) -> list[str]:
        return update_collection(
            self.hub_datalad_org,
            RepoHost.HUB_DATALAD_ORG,
            GINUpdater,
            token=token,
            url="https://hub.datalad.org",
        )

    def update_atris(self) -> list[str]:
        return update_collection(
            self.atris, RepoHost.ATRIS, GINUpdater, url="https://atris.fz-juelich.de"
        )


def update_collection(
    collection: list[T],
    host: RepoHost,
    updater_cls: type[Updater[T, U, S]],
    **searcher_kwargs: Any,
) -> list[str]:
    updater = updater_cls.from_collection(host, collection)
    with updater.get_searcher(**searcher_kwargs) as searcher:
        # For GitHub, pass organization configuration
        if host == RepoHost.GITHUB:
            from .github import GitHubSearcher, GitHubUpdater

            if isinstance(updater, GitHubUpdater) and isinstance(
                searcher, GitHubSearcher
            ):
                search_results = searcher.get_datalad_repos()
                for sr in search_results:
                    updater.register_repo(sr, searcher)
                # Save configuration changes
                updater.orgs_config.save()
            else:
                for search_result in searcher.get_datalad_repos():
                    updater.register_repo(search_result, searcher)
        else:
            for search_result in searcher.get_datalad_repos():
                updater.register_repo(search_result, searcher)
        new_collection = updater.get_new_collection(searcher)
        check_gone_flip(host, collection, new_collection)
        collection[:] = new_collection
    return updater.get_reports()


def count_gone(repos: Sequence[Any]) -> int:
    return sum(1 for r in repos if r.gone)


def check_gone_flip(host: RepoHost, old: Sequence[Any], new: Sequence[Any]) -> None:
    """Refuse an update that retires an implausible number of repositories."""
    was_active = len(old) - count_gone(old)
    newly_gone = count_gone(new) - count_gone(old)
    limit = max(GONE_FLIP_FLOOR, int(GONE_FLIP_FRACTION * was_active))
    if was_active and newly_gone > limit:
        raise RuntimeError(
            f"{host.value}: would mark {newly_gone} of {was_active} active"
            f" repositories as gone (limit {limit}); refusing to write."
            " The host is probably unhealthy."
        )
