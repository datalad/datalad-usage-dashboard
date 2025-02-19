from __future__ import annotations
import json
import logging
import os
import click
from click_loglevel import LogLevel
from ghtoken import get_ghtoken
from .config import README_FOLDER, RECORD_FILE
from .core import RepoHost
from .readmes import mkreadmes
from .record import RepoRecord
from .util import commit, runcmd


def set_mode(
    ctx: click.Context, _param: click.Parameter, value: RepoHost | None
) -> RepoHost | None:
    if value is not None:
        ctx.params.setdefault("mode", set()).add(value)
    return value


@click.command()
@click.option(
    "-l",
    "--log-level",
    type=LogLevel(),
    default=logging.INFO,
    help="Set logging level  [default: INFO]",
)
@click.option(
    "--gin",
    flag_value=RepoHost.GIN,
    type=click.UNPROCESSED,
    callback=set_mode,
    expose_value=False,
    help="Update GIN data",
)
@click.option(
    "--github",
    flag_value=RepoHost.GITHUB,
    type=click.UNPROCESSED,
    callback=set_mode,
    expose_value=False,
    help="Update GitHub data",
)
@click.option(
    "--hub-datalad-org",
    flag_value=RepoHost.HUB_DATALAD_ORG,
    type=click.UNPROCESSED,
    callback=set_mode,
    expose_value=False,
    help="Update hub.datalad.org data",
)
@click.option(
    "--osf",
    flag_value=RepoHost.OSF,
    type=click.UNPROCESSED,
    callback=set_mode,
    expose_value=False,
    help="Update OSF data",
)
@click.option(
    "-R",
    "--regen-readme",
    is_flag=True,
    help="Regenerate the README from the JSON file without querying",
)
def main(log_level: int, regen_readme: bool, mode: set[RepoHost] | None = None) -> None:
    if regen_readme and mode:
        raise click.UsageError("--regen-readme is mutually exclusive with mode options")

    logging.basicConfig(
        format="%(asctime)s [%(levelname)-8s] %(name)s %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S%z",
        level=log_level,
    )

    try:
        with open(RECORD_FILE, encoding="utf-8") as fp:
            record = RepoRecord.model_validate(json.load(fp))
    except FileNotFoundError:
        record = RepoRecord()

    reports: list[str] = []
    if mode is None:
        mode = set(RepoHost)
    if not regen_readme:
        if RepoHost.GITHUB in mode:
            reports.extend(record.update_github(get_ghtoken()))
        if RepoHost.OSF in mode:
            reports.extend(record.update_osf())
        if RepoHost.GIN in mode:
            reports.extend(record.update_gin(os.environ["GIN_TOKEN"]))
        if RepoHost.HUB_DATALAD_ORG in mode:
            reports.extend(
                record.update_hub_datalad_org(os.environ["HUB_DATALAD_ORG_TOKEN"])
            )
        with open(RECORD_FILE, "w") as fp:
            print(record.model_dump_json(indent=4), file=fp)

    mkreadmes(record)

    if not regen_readme:
        runcmd("git", "add", RECORD_FILE, "README.md", README_FOLDER)
        if reports:
            msg = "; ".join(reports)
        else:
            msg = "Updated the state without any new hits added"
        commit(msg)


if __name__ == "__main__":
    main()
