from collections import defaultdict
from pathlib import Path
from typing import List, Mapping, Union
from pydantic import BaseModel, Field
from .config import README_FOLDER
from .github import GHDataladRepo
from .osf import OSFDataladRepo
from .tables import TableRow, make_table_file


class RepoRecord(BaseModel):
    github: List[GHDataladRepo] = Field(default_factory=list)
    osf: List[OSFDataladRepo] = Field(default_factory=list)


def mkreadmes(
    record: RepoRecord,
    filename: Union[str, Path] = "README.md",
    directory: Union[str, Path] = README_FOLDER,
) -> None:
    Path(directory).mkdir(parents=True, exist_ok=True)
    Path(filename).parent.mkdir(parents=True, exist_ok=True)
    repos_by_org: Mapping[str, List[GHDataladRepo]] = defaultdict(list)
    for repo in record.github:
        repos_by_org[repo.owner].append(repo)
    main_rows: List[TableRow] = []
    for owner, repos in repos_by_org.items():
        if len(repos) > 1:
            with Path(directory, f"{owner}.md").open("w") as fp:
                main_rows.append(
                    make_table_file(
                        fp,
                        owner,
                        list(repos),  # Copy to make mypy happy
                        show_ours=False,
                    )
                )
        else:
            main_rows.extend(repos)
    with open(filename, "w") as fp:
        print("# GitHub", file=fp)
        make_table_file(fp, "", main_rows, show_ours=True, directory=directory)
        print(file=fp)
        print("# OSF", file=fp)
        active: List[OSFDataladRepo] = []
        gone: List[OSFDataladRepo] = []
        for osfrepo in record.osf:
            if osfrepo.gone:
                gone.append(osfrepo)
            else:
                active.append(osfrepo)
        for title, repolist in [("Active", active), ("Gone", gone)]:
            print(f"## {title}", file=fp)
            if repolist:
                for i, osfrepo in enumerate(repolist, start=1):
                    print(f"{i}. [{osfrepo.name}]({osfrepo.url})", file=fp)
            else:
                print("No repositories found!", file=fp)
