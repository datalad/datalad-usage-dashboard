from __future__ import annotations
from abc import ABC, abstractmethod
from collections.abc import Iterable
from enum import Enum
from types import TracebackType
from typing import Any, Generic, Self, TypeVar

T = TypeVar("T")
U = TypeVar("U")

# cf. <https://github.com/python/typing/issues/548>
# S = TypeVar("S", bound="Searcher[U]")
S = TypeVar("S", bound="Searcher")


class RepoHost(Enum):
    GITHUB = "GitHub"
    GIN = "GIN"
    OSF = "OSF"
    HUB_DATALAD_ORG = "hub.datalad.org"
    ATRIS = "ATRIS"


class Updater(ABC, Generic[T, U, S]):
    @classmethod
    @abstractmethod
    def from_collection(cls, host: RepoHost, collection: list[T]) -> Self:
        ...

    @abstractmethod
    def get_searcher(self, **kwargs: Any) -> S:
        ...

    @abstractmethod
    def register_repo(self, search_result: U, searcher: S) -> None:
        ...

    @abstractmethod
    def get_new_collection(self, searcher: S) -> list[T]:
        ...

    @abstractmethod
    def get_reports(self) -> list[str]:
        ...


class Searcher(ABC, Generic[U]):
    @abstractmethod
    def __enter__(self) -> Self:
        ...

    @abstractmethod
    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        ...

    @abstractmethod
    def get_datalad_repos(self) -> Iterable[U]:
        ...
