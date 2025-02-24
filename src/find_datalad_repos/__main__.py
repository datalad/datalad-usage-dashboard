from __future__ import annotations
import json
import logging
import os
import re
import click
from click_loglevel import LogLevel
from ghtoken import get_ghtoken
from .config import README_FOLDER, RECORD_FILE
from .core import RepoHost
from .readmes import mkreadmes
from .record import RepoRecord
from .util import commit, runcmd


class RepoHostSet(click.ParamType):
    name = "hostset"

    def convert(
        self,
        value: str | set[RepoHost],
        param: click.Parameter | None,
        ctx: click.Context | None,
    ) -> set[RepoHost]:
        if not isinstance(value, str):
            return value
        selected: set[RepoHost] = set()
        for v in re.split(r"\s*,\s*", value):
            if v == "all":
                selected.update(RepoHost)
            else:
                try:
                    selected.add(RepoHost(v))
                except ValueError:
                    self.fail(f"{value!r}: invalid item {v!r}", param, ctx)
        return selected

    def get_metavar(self, _param: click.Parameter) -> str:
        return "[all," + ",".join(v.value for v in RepoHost) + "]"


@click.command()
@click.option(
    "--hosts",
    type=RepoHostSet(),
    default="all",
    help="Set which repository hosts to query",
)
@click.option(
    "-l",
    "--log-level",
    type=LogLevel(),
    default=logging.INFO,
    help="Set logging level  [default: INFO]",
)
@click.option(
    "-R",
    "--regen-readme",
    is_flag=True,
    help="Regenerate the README from the JSON file without querying",
)
def main(log_level: int, regen_readme: bool, hosts: set[RepoHost]) -> None:
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
        if RepoHost.GITHUB in hosts:
            reports.extend(record.update_github(get_ghtoken()))
        if RepoHost.OSF in hosts:
            reports.extend(record.update_osf())
        if RepoHost.GIN in hosts:
            reports.extend(record.update_gin(os.environ["GIN_TOKEN"]))
        if RepoHost.HUB_DATALAD_ORG in hosts:
            reports.extend(
                record.update_hub_datalad_org(os.environ["HUB_DATALAD_ORG_TOKEN"])
            )
        if RepoHost.ATRIS in hosts:
            reports.extend(record.update_atris())
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
