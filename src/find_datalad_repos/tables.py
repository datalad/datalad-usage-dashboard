from __future__ import annotations
from collections.abc import Iterable
from dataclasses import dataclass
from enum import Enum


class Column(Enum):
    REPOSITORY = "Repository"
    STARS = "Stars"
    IS_DATASET = "Dataset"
    IS_RUN = "`run`"
    IS_CONTAINERS_RUN = "`containers-run`"
    LAST_MODIFIED = "Last Modified"

    @property
    def countable(self) -> bool:
        return self in {
            Column.REPOSITORY,
            Column.STARS,
            Column.IS_DATASET,
            Column.IS_RUN,
            Column.IS_CONTAINERS_RUN,
        }


GITHUB_COLUMNS = [
    Column.REPOSITORY,
    Column.STARS,
    Column.IS_DATASET,
    Column.IS_RUN,
    Column.IS_CONTAINERS_RUN,
    Column.LAST_MODIFIED,
]

GIN_COLUMNS = [
    Column.REPOSITORY,
    Column.STARS,
    Column.LAST_MODIFIED,
]

OSF_COLUMNS = [
    Column.REPOSITORY,
    Column.LAST_MODIFIED,
]


@dataclass
class RepoTable:
    title: str
    columns: list[Column]
    rows: list[TableRow]

    def get_total_qtys(self) -> dict[Column, int]:
        keys = [col for col in Column if col.countable and col in self.columns]
        qtys = dict.fromkeys(keys, 0)
        for r in self.rows:
            for c in keys:
                qtys[c] += r.qtys[c]
        return qtys

    def render(self) -> str:
        s = f"## {self.title}\n"
        if self.rows:
            qtys = self.get_total_qtys()
            headers = ["#"]
            for col in self.columns:
                q = qtys.get(col, 0)
                if q > 0:
                    headers.append(f"{col.value} ({q})")
                else:
                    headers.append(col.value)
            s += render_row(headers)
            s += render_row(["---"] * len(headers))
            for i, r in enumerate(self.rows, start=1):
                s += render_row([str(i)] + [r.cells[col] for col in self.columns])
        else:
            s += "No repositories found!\n"
        return s


@dataclass
class TableRow:
    cells: dict[Column, str]
    qtys: dict[Column, int]


def render_row(cells: Iterable[str]) -> str:
    return "| " + " | ".join(cells) + " |\n"
