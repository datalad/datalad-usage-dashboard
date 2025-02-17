from __future__ import annotations
from collections import Counter, defaultdict
from datetime import datetime
from pathlib import Path
from .config import OURSELVES, README_FOLDER
from .gin import GINRepo
from .github import GitHubRepo
from .osf import OSFRepo
from .record import RepoRecord
from .tables import (
    GIN_COLUMNS,
    GITHUB_COLUMNS,
    OSF_COLUMNS,
    Column,
    RepoTable,
    TableRow,
)
from .util import check


def mkreadmes(
    record: RepoRecord,
    filename: str | Path = "README.md",
    directory: str | Path = README_FOLDER,
) -> None:
    Path(directory).mkdir(parents=True, exist_ok=True)
    Path(directory, "gin").mkdir(parents=True, exist_ok=True)
    Path(filename).parent.mkdir(parents=True, exist_ok=True)
    (github_block, github_wild, github_ours, github_gone) = make_github_tables(
        record.github, directory
    )
    (gin_block, gin_active, gin_gone) = make_gin_tables(
        record.gin, Path(directory, "gin")
    )
    (osf_block, osf_active, osf_gone) = make_osf_tables(record.osf)
    with open(filename, "w") as fp:
        print("# Introduction", file=fp)
        print(file=fp)
        print(
            "This file is automatically updated using GitHub Actions workflows.  It lists "
            "Git repositories discovered on GitHub and other hosts that "
            "either are [DataLad](https://www.datalad.org) datasets or else had "
            "`datalad run` used on them. "
            "Furthermore, the [`datalad-repos.json`](./datalad-repos.json) file in "
            "this repository is used by https://registry.datalad.org/ to provide "
            "up-to-date metadata for these repositories and support metadata-based "
            "searches.",
            file=fp,
        )
        print(file=fp)
        print("# Summary", file=fp)
        print(
            f"- [GitHub](#github): [{github_wild}](#in-the-wild) in the wild +"
            f" [{github_ours}](#inner-circle) inner-circle +"
            f" [{github_gone}](#gone) gone",
            file=fp,
        )
        print(
            f"- [OSF](#osf): [{osf_active}](#active) active +"
            f" [{osf_gone}](#gone-1) gone",
            file=fp,
        )
        print(
            f"- [GIN](#gin): [{gin_active}](#active-1) active +"
            f" [{gin_gone}](#gone-2) gone",
            file=fp,
        )
        print(file=fp)
        print(github_block, file=fp)
        print(osf_block, file=fp)
        print(gin_block, end="", file=fp)


def make_github_tables(
    repolist: list[GitHubRepo], directory: str | Path
) -> tuple[str, int, int, int]:
    base_url = "https://github.com"
    repos_by_org: dict[str, list[GitHubRepo]] = defaultdict(list)
    for repo in repolist:
        repos_by_org[repo.owner].append(repo)
    main_wild: list[TableRow] = []
    main_ours: list[TableRow] = []
    main_gone: list[TableRow] = []
    for owner, repos in repos_by_org.items():
        if len(repos) > 1:
            active: list[TableRow] = []
            gone: list[TableRow] = []
            for r in repos:
                if r.gone:
                    gone.append(r.as_table_row())
                else:
                    active.append(r.as_table_row())
            tables = [
                RepoTable(title="Active", columns=GITHUB_COLUMNS, rows=active),
                RepoTable(title="Gone", columns=GITHUB_COLUMNS, rows=gone),
            ]
            stats: Counter[Column] = Counter()
            with Path(directory, f"{owner}.md").open("w") as fp:
                first = True
                for tbl in tables:
                    if first:
                        first = False
                    else:
                        print(file=fp)
                    print(tbl.render(), end="", file=fp)
                    for k, v in tbl.get_total_qtys().items():
                        stats[k] += v
            if active:
                if owner in OURSELVES:
                    section = main_ours
                else:
                    section = main_wild
            else:
                section = main_gone
            file_link = f"{directory}/{owner}.md"
            repo_qty = stats[Column.REPOSITORY]
            star_qty = stats[Column.STARS]
            last_modified: datetime | None = max(
                [lm for r in repos if (lm := r.updated) is not None],
                default=None,
            )
            cells = {
                Column.REPOSITORY: (
                    f"[{owner}/*]({base_url}/{owner})" f" [({repo_qty})]({file_link})"
                ),
                Column.STARS: f"[{star_qty}]({file_link})",
                Column.LAST_MODIFIED: (
                    str(last_modified) if last_modified is not None else "\u2014"
                ),
            }
            for col in [
                Column.IS_DATASET,
                Column.IS_RUN,
                Column.IS_CONTAINERS_RUN,
            ]:
                qty = stats[col]
                if qty > 0:
                    cells[col] = f"[{check(True)} ({qty})]({file_link})"
                else:
                    cells[col] = ""
            section.append(TableRow(cells=cells, qtys=stats))
        else:
            r = repos[0]
            if r.gone:
                section = main_gone
            elif owner in OURSELVES:
                section = main_ours
            else:
                section = main_wild
            section.append(r.as_table_row())
    outer_tables = "# GitHub\n"
    final_qtys = []
    first = True
    for title, rows in [
        ("In the wild", main_wild),
        ("Inner circle", main_ours),
        ("Gone", main_gone),
    ]:
        tbl = RepoTable(title=title, columns=GITHUB_COLUMNS, rows=rows)
        if first:
            first = False
        else:
            outer_tables += "\n"
        outer_tables += tbl.render()
        final_qtys.append(tbl.get_total_qtys()[Column.REPOSITORY])
    # (outer_tables, wild_qty, ours_qty, gone_qty)
    return (outer_tables, *final_qtys)  # type: ignore[return-value]


def make_gin_tables(
    repolist: list[GINRepo], directory: str | Path
) -> tuple[str, int, int]:
    base_url = "https://gin.g-node.org"
    repos_by_org: dict[str, list[GINRepo]] = defaultdict(list)
    for repo in repolist:
        repos_by_org[repo.owner].append(repo)
    main_active: list[TableRow] = []
    main_gone: list[TableRow] = []
    for owner, repos in repos_by_org.items():
        if len(repos) > 1:
            active: list[TableRow] = []
            gone: list[TableRow] = []
            for r in repos:
                if r.gone:
                    gone.append(r.as_table_row())
                else:
                    active.append(r.as_table_row())
            tables = [
                RepoTable(title="Active", columns=GIN_COLUMNS, rows=active),
                RepoTable(title="Gone", columns=GIN_COLUMNS, rows=gone),
            ]
            stats: Counter[Column] = Counter()
            with Path(directory, f"{owner}.md").open("w") as fp:
                first = True
                for tbl in tables:
                    if first:
                        first = False
                    else:
                        print(file=fp)
                    print(tbl.render(), end="", file=fp)
                    for k, v in tbl.get_total_qtys().items():
                        stats[k] += v
            section = main_active if active else main_gone
            file_link = f"{directory}/{owner}.md"
            repo_qty = stats[Column.REPOSITORY]
            star_qty = stats[Column.STARS]
            last_modified: datetime | None = max(
                [lm for r in repos if (lm := r.updated) is not None],
                default=None,
            )
            cells = {
                Column.REPOSITORY: (
                    f"[{owner}/*]({base_url}/{owner}) [({repo_qty})]({file_link})"
                ),
                Column.STARS: f"[{star_qty}]({file_link})",
                Column.LAST_MODIFIED: (
                    str(last_modified) if last_modified is not None else "\u2014"
                ),
            }
            section.append(TableRow(cells=cells, qtys=stats))
        else:
            r = repos[0]
            section = main_gone if r.gone else main_active
            section.append(r.as_table_row())
    outer_tables = "# GIN\n"
    final_qtys = []
    first = True
    for title, rows in [("Active", main_active), ("Gone", main_gone)]:
        tbl = RepoTable(title=title, columns=GIN_COLUMNS, rows=rows)
        if first:
            first = False
        else:
            outer_tables += "\n"
        outer_tables += tbl.render()
        final_qtys.append(tbl.get_total_qtys()[Column.REPOSITORY])
    # (outer_tables, active_qty, gone_qty)
    return (outer_tables, *final_qtys)  # type: ignore[return-value]


def make_osf_tables(repolist: list[OSFRepo]) -> tuple[str, int, int]:
    active: list[TableRow] = []
    gone: list[TableRow] = []
    for r in repolist:
        if r.gone:
            gone.append(r.as_table_row())
        else:
            active.append(r.as_table_row())
    s = "# OSF\n"
    final_qtys = []
    first = True
    for title, rows in [("Active", active), ("Gone", gone)]:
        tbl = RepoTable(title=title, columns=OSF_COLUMNS, rows=rows)
        if first:
            first = False
        else:
            s += "\n"
        s += tbl.render()
        final_qtys.append(tbl.get_total_qtys()[Column.REPOSITORY])
    # (outer_tables, active_qty, gone_qty)
    return (s, *final_qtys)  # type: ignore[return-value]
