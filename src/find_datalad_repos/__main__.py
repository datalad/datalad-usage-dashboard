from __future__ import annotations
import json
import logging
import os
import click
from click_loglevel import LogLevel
from ghtoken import get_ghtoken
from .config import README_FOLDER, RECORD_FILE
from .readmes import mkreadmes
from .record import RepoRecord
from .util import commit, runcmd


def set_mode(
    ctx: click.Context, _param: click.Parameter, value: str | None
) -> str | None:
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
    flag_value="gin",
    callback=set_mode,
    expose_value=False,
    help="Update GIN data",
)
@click.option(
    "--github",
    flag_value="github",
    callback=set_mode,
    expose_value=False,
    help="Update GitHub data",
)
@click.option(
    "--hub-datalad-org",
    flag_value="hub.datalad.org",
    callback=set_mode,
    expose_value=False,
    help="Update hub.datalad.org data",
)
@click.option(
    "--osf",
    flag_value="osf",
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
def main(log_level: int, regen_readme: bool, mode: set[str] | None = None) -> None:
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
    if not regen_readme:
        if mode is None or "github" in mode:
            reports.extend(record.update_github(get_ghtoken()))
        if mode is None or "osf" in mode:
            reports.extend(record.update_osf())
        if mode is None or "gin" in mode:
            reports.extend(record.update_gin(os.environ["GIN_TOKEN"]))
        if mode is None or "hub.datalad.org" in mode:
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
