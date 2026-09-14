from __future__ import annotations
from collections.abc import Callable
import json
import logging
import os
import re
import sys
import click
from click_loglevel import LogLevel
from ghtoken import get_ghtoken
from .config import README_FOLDER, RECORD_FILE, GITHUB_ORGS_FILE
from .core import RepoHost
from .readmes import mkreadmes
from .record import RepoRecord
from .util import commit, in_git_head, log, runcmd


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

    def get_metavar(self, _param: click.Parameter, _ctx: click.Context) -> str:
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
        if in_git_head(RECORD_FILE):
            # Starting from an empty record here would rebuild the whole
            # dashboard from a single run's results and commit that over the
            # real one.  Every host would also skip the gone-flip ceiling,
            # since it has nothing to compare against.
            raise RuntimeError(
                f"{RECORD_FILE} is missing from the working tree but present"
                " in HEAD; refusing to start from an empty record."
            ) from None
        record = RepoRecord()

    reports: list[str] = []
    failed: list[str] = []
    if not regen_readme:
        # Zero-argument callables so that a missing token raises inside the
        # try below, taking down one host instead of the whole run.  Ordered,
        # unlike `hosts`, which is a set.
        updates: list[tuple[RepoHost, Callable[[], list[str]]]] = [
            (RepoHost.GITHUB, lambda: record.update_github(get_ghtoken())),
            (RepoHost.OSF, record.update_osf),
            (RepoHost.GIN, lambda: record.update_gin(os.environ["GIN_TOKEN"])),
            (
                RepoHost.HUB_DATALAD_ORG,
                lambda: record.update_hub_datalad_org(
                    os.environ["HUB_DATALAD_ORG_TOKEN"]
                ),
            ),
            (RepoHost.ATRIS, record.update_atris),
        ]
        for host, update in updates:
            if host not in hosts:
                continue
            try:
                reports.extend(update())
            except Exception:
                log.exception("Updating %s failed; continuing", host.value)
                failed.append(host.value)
        # Serialise to a sibling and rename, so a failure partway through
        # cannot leave a truncated record behind for `git add` to pick up.
        tmpfile = RECORD_FILE + ".tmp"
        with open(tmpfile, "w", encoding="utf-8") as fp:
            print(record.model_dump_json(indent=4), file=fp)
        os.replace(tmpfile, RECORD_FILE)

    mkreadmes(record)

    if not regen_readme:
        runcmd("git", "add", RECORD_FILE, GITHUB_ORGS_FILE, "README.md", README_FOLDER)
        if reports:
            msg = "; ".join(reports)
        else:
            msg = "Updated the state without any new hits added"
        if failed:
            msg += " [failed: " + ", ".join(failed) + "]"
        commit(msg)
        if failed:
            sys.exit(1)


if __name__ == "__main__":
    main()
