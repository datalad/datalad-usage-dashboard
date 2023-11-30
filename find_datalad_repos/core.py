from __future__ import annotations
from collections import defaultdict
from pathlib import Path
from typing import List
from pydantic import BaseModel, Field
from .config import README_FOLDER
from .gin import GINDataladRepo
from .github import GHDataladRepo
from .osf import OSFDataladRepo
from .tables import GIN_HEADERS, GITHUB_HEADERS, TableRow, make_table_file


class RepoRecord(BaseModel):
    github: List[GHDataladRepo] = Field(default_factory=list)
    osf: List[OSFDataladRepo] = Field(default_factory=list)
    gin: List[GINDataladRepo] = Field(default_factory=list)


def mkreadmes(
    record: RepoRecord,
    filename: str | Path = "README.md",
    directory: str | Path = README_FOLDER,
) -> None:
    Path(directory).mkdir(parents=True, exist_ok=True)
    Path(directory, "gin").mkdir(parents=True, exist_ok=True)
    Path(filename).parent.mkdir(parents=True, exist_ok=True)
    main_rows: dict[str, list[TableRow]] = {}
    repolist: list[TableRow]
    for key, subdir, repolist, headers, base_url in [  # type: ignore[assignment]
        ("github", (), record.github, GITHUB_HEADERS, "https://github.com/"),
        ("gin", ("gin",), record.gin, GIN_HEADERS, "https://gin.g-node.org/"),
    ]:
        repos_by_org: dict[str, list[TableRow]] = defaultdict(list)
        for repo in repolist:
            repos_by_org[repo.owner].append(repo)  # type: ignore[attr-defined]
        one_offs: list[TableRow] = []
        for owner, repos in repos_by_org.items():
            if len(repos) > 1:
                with Path(directory, *subdir, f"{owner}.md").open("w") as fp:
                    one_offs.append(
                        make_table_file(
                            fp, owner, headers, repos, base_url, show_ours=False
                        )
                    )
            else:
                one_offs.extend(repos)
        main_rows[key] = one_offs
    osfactive: list[OSFDataladRepo] = []
    osfgone: list[OSFDataladRepo] = []
    for osfrepo in record.osf:
        if osfrepo.gone:
            osfgone.append(osfrepo)
        else:
            osfactive.append(osfrepo)
    with open(filename, "w") as fp:
        print("# Summary", file=fp)
        wild, ours, gone = count_wild_ours_gone(main_rows["github"])
        print(
            f"- [GitHub](#github): [{wild}](#in-the-wild) in the wild +"
            f" [{ours}](#inner-circle) inner-circle + [{gone}](#gone) gone",
            file=fp,
        )
        active = len(osfactive)
        gone = len(osfgone)
        print(
            f"- [OSF](#osf): [{active}](#active) active + [{gone}](#gone-1) gone",
            file=fp,
        )
        active, gone = count_active_gone(main_rows["gin"])
        print(
            f"- [GIN](#gin): [{active}](#active-1) active + [{gone}](#gone-2) gone",
            file=fp,
        )
        print("# GitHub", file=fp)
        make_table_file(
            fp,
            "",
            GITHUB_HEADERS,
            main_rows["github"],
            "https://github.com/",
            show_ours=True,
            directory=directory,
        )
        print(file=fp)
        print("# OSF", file=fp)
        for title, osfrepolist in [("Active", osfactive), ("Gone", osfgone)]:
            print(f"## {title}", file=fp)
            if osfrepolist:
                for i, osfrepo in enumerate(osfrepolist, start=1):
                    print(f"{i}. [{osfrepo.name}]({osfrepo.url})", file=fp)
            else:
                print("No repositories found!", file=fp)
        print(file=fp)
        print("# GIN", file=fp)
        make_table_file(
            fp,
            "",
            GIN_HEADERS,
            main_rows["gin"],
            "https://gin.g-node.org/",
            show_ours=False,
            directory=Path(directory, "gin"),
        )


def count_wild_ours_gone(rows: list[TableRow]) -> tuple[int, int, int]:
    wild = 0
    ours = 0
    gone = 0
    for r in rows:
        qty = r.get_qtys().repo_qty
        if r.gone:
            gone += qty
        elif r.ours:
            ours += qty
        else:
            wild += qty
    return (wild, ours, gone)


def count_active_gone(rows: list[TableRow]) -> tuple[int, int]:
    active = 0
    gone = 0
    for r in rows:
        qty = r.get_qtys().repo_qty
        if r.gone:
            gone += qty
        else:
            active += qty
    return (active, gone)
