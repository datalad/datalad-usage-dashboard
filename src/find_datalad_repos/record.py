from __future__ import annotations
from typing import Any
from pydantic import BaseModel, Field
from .core import RepoHost, S, T, U, Updater
from .gin import GINRepo, GINUpdater
from .github import GitHubRepo, GitHubUpdater
from .osf import OSFRepo, OSFUpdater


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
        collection[:] = updater.get_new_collection(searcher)
    return updater.get_reports()
