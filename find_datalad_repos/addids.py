import logging
import click
from ghreq import Client, PrettyHTTPError
from ghtoken import get_ghtoken
from .config import RECORD_FILE
from .core import RepoRecord
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
    with Client(token=get_ghtoken(), user_agent=USER_AGENT) as client:
        for repo in record.github:
            if repo.id is None:
                try:
                    data = client.get(f"/repos/{repo.name}")
                except PrettyHTTPError as e:
                    if e.response is not None and e.response.status_code == 404:
                        log.info("Repository %s 404'd; ignoring", repo.name)
                        continue
                    else:
                        raise
                else:
                    repo.id = data["id"]
                    log.info("Set ID of %s to %d", repo.name, repo.id)
    with open(RECORD_FILE, "w") as fp:
        print(record.json(indent=4), file=fp)


if __name__ == "__main__":
    main()
