from __future__ import annotations
from abc import ABC, abstractmethod
from collections.abc import Iterable
from pathlib import Path
from typing import IO, ClassVar, List
from pydantic import BaseModel, Field
from .config import OURSELVES, README_FOLDER
from .util import Statistics, Status, check


class TableRow(ABC, BaseModel):
    @abstractmethod
    def get_cells(self, directory: str | Path) -> list[str]:
        ...

    @abstractmethod
    def get_qtys(self) -> Statistics:
        ...

    @property
    @abstractmethod
    def ours(self) -> bool:
        ...

    @property
    @abstractmethod
    def gone(self) -> bool:
        ...


class SubtableRow(TableRow):
    name: str
    qtys: Statistics
    status: Status

    @property
    def ours(self) -> bool:
        return self.name in OURSELVES

    @property
    def gone(self) -> bool:
        return self.status is Status.GONE

    @property
    def url(self) -> str:
        return f"https://github.com/{self.name}"

    def get_cells(self, directory: str | Path) -> list[str]:
        file_link = f"{directory}/{self.name}.md"
        cells = [
            f"[{self.name}/*]({self.url}) [({self.qtys.repo_qty})]({file_link})",
            f"[{self.qtys.star_qty}]({file_link})",
        ]
        for qty in [
            self.qtys.dataset_qty,
            self.qtys.run_qty,
            self.qtys.container_run_qty,
        ]:
            if qty > 0:
                cells.append(f"[{check(True)} ({qty})]({file_link})")
            else:
                cells.append("")
        return cells

    def get_qtys(self) -> Statistics:
        return self.qtys


class RepoTable(BaseModel):
    HEADERS: ClassVar[List[str]] = [
        "#",
        "Repository",
        "Stars",
        "Dataset",
        "`run`",
        "`containers-run`",
    ]

    title: str
    rows: List[TableRow] = Field(default_factory=list)

    def get_total_qtys(self) -> Statistics:
        return Statistics.sum(r.get_qtys() for r in self.rows)

    def render(self, directory: str | Path) -> str:
        s = f"## {self.title}\n"
        if self.rows:
            qtys = self.get_total_qtys()
            headers = [self.HEADERS[0]]
            for h, q in zip(self.HEADERS[1:], qtys):
                if q > 0:
                    headers.append(f"{h} ({q})")
                else:
                    headers.append(h)
            s += self.render_row(headers)
            s += self.render_row(["---"] * len(self.HEADERS))
            for i, r in enumerate(self.rows, start=1):
                s += self.render_row([str(i)] + r.get_cells(directory))
        else:
            s += "No repositories found!\n"
        return s

    @staticmethod
    def render_row(cells: Iterable[str]) -> str:
        return "| " + " | ".join(cells) + " |\n"


def make_table_file(
    fp: IO[str],
    name: str,
    rows: list[TableRow],
    show_ours: bool = True,
    directory: str | Path = README_FOLDER,
) -> SubtableRow:
    wild: list[TableRow] = []
    ours: list[TableRow] = []
    gone: list[TableRow] = []
    for r in rows:
        if r.gone:
            gone.append(r)
        elif r.ours and show_ours:
            ours.append(r)
        else:
            wild.append(r)
    if show_ours:
        tables = [
            RepoTable(title="In the wild", rows=wild),
            RepoTable(title="Inner circle", rows=ours),
            RepoTable(title="Gone", rows=gone),
        ]
    else:
        tables = [
            RepoTable(title="Active", rows=wild),
            RepoTable(title="Gone", rows=gone),
        ]
    stats: list[Statistics] = []
    first = True
    for tbl in tables:
        if first:
            first = False
        else:
            print(file=fp)
        print(tbl.render(directory=directory), end="", file=fp)
        stats.append(tbl.get_total_qtys())
    if all(r.gone for tbl in tables for r in tbl.rows):
        status = Status.GONE
    else:
        status = Status.ACTIVE
    return SubtableRow(name=name, qtys=Statistics.sum(stats), status=status)
