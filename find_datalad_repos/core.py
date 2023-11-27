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
    with open(filename, "w") as fp:
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
        active: list[OSFDataladRepo] = []
        gone: list[OSFDataladRepo] = []
        for osfrepo in record.osf:
            if osfrepo.gone:
                gone.append(osfrepo)
            else:
                active.append(osfrepo)
        for title, ginrepolist in [("Active", active), ("Gone", gone)]:
            print(f"## {title}", file=fp)
            if ginrepolist:
                for i, osfrepo in enumerate(ginrepolist, start=1):
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
