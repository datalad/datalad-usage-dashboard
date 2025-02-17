from __future__ import annotations
from datetime import datetime, timezone
from enum import Enum
import json
import logging
from pathlib import Path
import platform
import re
import shlex
import subprocess
import sys
from typing import Any
import requests

USER_AGENT = "find_datalad_repos ({}) requests/{} {}/{}".format(
    "https://github.com/datalad/datalad-usage-dashboard",
    requests.__version__,
    platform.python_implementation(),
    platform.python_version(),
)

log = logging.getLogger(__package__)


class Status(Enum):
    ACTIVE = "active"
    GONE = "gone"


def is_container_run(commit_msg: str) -> bool:
    m = re.search(
        r"^=== Do not change lines below ===$(.+)"
        r"^\^\^\^ Do not change lines above \^\^\^$",
        commit_msg,
        flags=re.M | re.S,
    )
    if m is None:
        return False
    try:
        metadata = json.loads(m[1])
    except ValueError:
        return False
    try:
        return bool(metadata.get("extra_inputs"))
    except AttributeError:
        # Apparently there are some commits with strings for the RUNCMD
        # metadata?
        return False


def check(yesno: bool) -> str:
    # return '\u2714\uFE0F' if yesno else ''
    return ":heavy_check_mark:" if yesno else ""


def runcmd(*args: str | Path, **kwargs: Any) -> None:
    log.debug("Running: %s", " ".join(shlex.quote(str(a)) for a in args))
    r = subprocess.run(args, **kwargs)
    if r.returncode != 0:
        sys.exit(r.returncode)


def commit(msg: str) -> None:
    if subprocess.run(["git", "diff", "--cached", "--quiet"]).returncode != 0:
        runcmd("git", "commit", "-m", msg)
    else:
        log.info("Nothing to commit")


def nowutc() -> datetime:
    return datetime.now(timezone.utc)
