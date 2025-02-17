from __future__ import annotations
from datetime import date
import os
from pathlib import Path
import subprocess
import click
from .config import RECORD_FILE
from .readmes import mkreadmes
from .record import RepoRecord


@click.command()
@click.option(
    "-R",
    "--repo",
    type=click.Path(exists=True, file_okay=False, path_type=Path),
    default=os.curdir,
    help="Path to the datalad-usage-dashboard repository",
    show_default=True,
)
@click.option(
    "-f",
    "--readme-file",
    type=click.Path(dir_okay=False, writable=True, path_type=Path),
    default="README.md",
    help="Name of the root README file to generate",
    show_default=True,
)
@click.option(
    "-d",
    "--readme-dir",
    type=click.Path(file_okay=False, writable=True, path_type=Path),
    default="READMEs",
    help="Directory in which to place sub-READMEs",
    show_default=True,
)
@click.argument("from_point", metavar="FROM")
@click.argument("to_point", metavar="TO", required=False)
def main(
    from_point: str,
    to_point: str | None,
    repo: Path,
    readme_file: Path,
    readme_dir: Path,
) -> None:
    """
    Generate READMEs listing datasets added between two points in time.

    This script examines the history of the datalad-repos.json file in a local
    clone of the datalad/datalad-usage-dashboard repository and creates a
    README and sub-READMEs for all datasets added to the file between two given
    points in time.

    The FROM and TO arguments can be either a date in the form "YYYY-MM-DD"
    (indicating midnight at that date in the local timezone) or a Git
    commitish.  If the TO argument is not given, it defaults to "HEAD".
    """
    from_commit = dateish2commit(repo, from_point)
    to_commit = "HEAD" if to_point is None else dateish2commit(repo, to_point)
    from_record = RepoRecord.model_validate_json(read_record(from_commit, repo))
    to_record = RepoRecord.model_validate_json(read_record(to_commit, repo))
    old_github_repos = {r.name for r in from_record.github}
    old_osf_repos = {r.id for r in from_record.osf}
    old_gin_repos = {r.id for r in from_record.gin}
    new_record = RepoRecord()
    for ghr in to_record.github:
        if ghr.name not in old_github_repos:
            new_record.github.append(ghr)
    for osfr in to_record.osf:
        if osfr.id not in old_osf_repos:
            new_record.osf.append(osfr)
    for ginr in to_record.gin:
        if ginr.id not in old_gin_repos:
            new_record.gin.append(ginr)
    mkreadmes(new_record, filename=readme_file, directory=readme_dir)


def dateish2commit(repo: Path, dateish: str) -> str:
    try:
        dt = date.fromisoformat(dateish)
    except ValueError:
        return dateish
    return readgit(
        "rev-list",
        "-n1",
        "--first-parent",
        f"--before={dt}T00:00:00",
        "HEAD",
        repo=repo,
    )


def readgit(*args: str, repo: Path) -> str:
    r = subprocess.run(
        ["git", *args], cwd=repo, stdout=subprocess.PIPE, text=True, check=True
    )
    assert isinstance(r.stdout, str)
    return r.stdout.strip()


def read_record(commit: str, repo: Path) -> str:
    if commit == "":
        # rev-list returned nothing, so date must be before repo was created;
        # simulate by returning an empty JSON file
        return "{}"
    else:
        return readgit("show", f"{commit}:{RECORD_FILE}", repo=repo)


if __name__ == "__main__":
    main()
