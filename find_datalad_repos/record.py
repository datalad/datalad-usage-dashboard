from __future__ import annotations
from typing import List
from pydantic import BaseModel, Field
from .core import S, T, U, Updater
from .gin import GINRepo, GINUpdater
from .github import GitHubRepo, GitHubUpdater
from .osf import OSFRepo, OSFUpdater


class RepoRecord(BaseModel):
    github: List[GitHubRepo] = Field(default_factory=list)
    osf: List[OSFRepo] = Field(default_factory=list)
    gin: List[GINRepo] = Field(default_factory=list)

    def update_github(self, token: str) -> list[str]:
        return update_collection(self.github, GitHubUpdater, token)

    def update_osf(self) -> list[str]:
        return update_collection(self.osf, OSFUpdater, None)

    def update_gin(self, token: str) -> list[str]:
        return update_collection(self.gin, GINUpdater, token)


def update_collection(
    collection: list[T], updater_cls: type[Updater[T, U, S]], token: str | None
) -> list[str]:
    updater = updater_cls.from_collection(collection)
    with updater.get_searcher(token) as searcher:
        for search_result in searcher.get_datalad_repos():
            updater.register_repo(search_result, searcher)
        collection[:] = updater.get_new_collection(searcher)
    return updater.get_reports()
