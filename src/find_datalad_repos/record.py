from __future__ import annotations
from typing import Any
from pydantic import BaseModel, Field
from .core import S, T, U, Updater
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
        return update_collection(self.github, GitHubUpdater, token=token)

    def update_osf(self) -> list[str]:
        return update_collection(self.osf, OSFUpdater)

    def update_gin(self, token: str) -> list[str]:
        return update_collection(self.gin, GINUpdater, token=token)

    def update_hub_datalad_org(self, token: str) -> list[str]:
        return update_collection(
            self.hub_datalad_org, GINUpdater, token=token, url="https://hub.datalad.org"
        )

    def update_atris(self) -> list[str]:
        return update_collection(
            self.atris, GINUpdater, url="https://atris.fz-juelich.de"
        )


def update_collection(
    collection: list[T], updater_cls: type[Updater[T, U, S]], **searcher_kwargs: Any
) -> list[str]:
    updater = updater_cls.from_collection(collection)
    with updater.get_searcher(**searcher_kwargs) as searcher:
        for search_result in searcher.get_datalad_repos():
            updater.register_repo(search_result, searcher)
        collection[:] = updater.get_new_collection(searcher)
    return updater.get_reports()
