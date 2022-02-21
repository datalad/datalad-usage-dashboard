import logging
import click
import requests
from .config import RECORD_FILE
from .core import RepoRecord
from .github import get_github_token
from .util import USER_AGENT, log


@click.command()
def main() -> None:
    """Populate missing repository IDs for GitHub repositories"""
    logging.basicConfig(
        format="%(asctime)s [%(levelname)-8s] %(name)s %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S%z",
        level=logging.INFO,
    )
    record = RepoRecord.parse_file(RECORD_FILE)
    token = get_github_token()
    with requests.Session() as s:
        s.headers["Authorization"] = f"token {token}"
        s.headers["User-Agent"] = USER_AGENT
        for repo in record.github:
            if repo.id is None:
                r = s.get(f"https://api.github.com/repos/{repo.name}")
                if r.status_code == 404:
                    log.info("Repository %s 404'd; ignoring", repo.name)
                else:
                    r.raise_for_status()
                    repo.id = r.json()["id"]
                    log.info("Set ID of %s to %d", repo.name, repo.id)
    with open(RECORD_FILE, "w") as fp:
        print(record.json(indent=4), file=fp)


if __name__ == "__main__":
    main()
