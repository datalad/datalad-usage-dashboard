from __future__ import annotations
from abc import ABC, abstractmethod
from collections.abc import Iterable
from types import TracebackType
from typing import Generic, Self, TypeVar

T = TypeVar("T")
U = TypeVar("U")


class Updater(ABC, Generic[T, U]):
    @classmethod
    @abstractmethod
    def from_collection(cls, collection: list[T]) -> Self:
        ...

    @abstractmethod
    def get_searcher(self, token: str | None) -> Searcher[U]:
        ...

    @abstractmethod
    def register_repo(self, search_result: U, searcher: Searcher[U]) -> None:
        ...

    @abstractmethod
    def get_new_collection(self, searcher: Searcher[U]) -> list[T]:
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
