"""One host failing must not discard or retire the other hosts' data."""
import json
import subprocess
from click.testing import CliRunner
import pytest
from find_datalad_repos.__main__ import main
from find_datalad_repos.core import RepoHost
from find_datalad_repos.gin import GINRepo, GINSearcher
from find_datalad_repos.osf import OSFRepo, OSFSearcher
from find_datalad_repos.record import check_gone_flip
from find_datalad_repos.util import Status

SEED = {
    "osf": [
        {
            "url": "https://osf.io/aaaaa/",
            "id": "aaaaa",
            "name": "A",
            "status": "active",
        },
        {
            "url": "https://osf.io/bbbbb/",
            "id": "bbbbb",
            "name": "B",
            "status": "active",
        },
    ],
    "gin": [
        {
            "id": 1,
            "name": "owner/repo",
            "url": "https://gin.g-node.org/owner/repo",
            "stars": 0,
            "status": "active",
        }
    ],
}


def git(*args):
    return subprocess.run(
        ["git", *args], check=True, capture_output=True, text=True
    ).stdout


def test_failed_host_leaves_its_collection_intact(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    git("init", "-q", ".")
    git("config", "user.email", "test@example.com")
    git("config", "user.name", "Test")
    (tmp_path / "datalad-repos.json").write_text(json.dumps(SEED))
    # `git add` exits 128 on a missing pathspec, which would mask the exit
    # code under test.
    (tmp_path / "github-orgs.json").write_text("{}")
    git("add", "-A")
    git("commit", "-qm", "seed")

    def osf_boom(self):
        # Yield one repo before failing, so a naive "keep what we got"
        # implementation would retire the other seeded OSF repo.
        yield OSFRepo(
            url="https://osf.io/aaaaa/", id="aaaaa", name="A", status=Status.ACTIVE
        )
        raise RuntimeError("simulated OSF 502")

    def gin_ok(self):
        yield GINRepo(
            id=1,
            name="owner/repo",
            url="https://gin.g-node.org/owner/repo",
            stars=7,
            status=Status.ACTIVE,
        )

    monkeypatch.setattr(OSFSearcher, "get_datalad_repos", osf_boom)
    monkeypatch.setattr(GINSearcher, "get_datalad_repos", gin_ok)
    monkeypatch.setenv("GIN_TOKEN", "unused")

    # OSF runs before GIN, so this also proves the run continues past a
    # failed host rather than merely tolerating a failure at the end.
    result = CliRunner().invoke(main, ["--hosts", "OSF,GIN"])

    assert result.exit_code == 1, result.output
    record = json.loads((tmp_path / "datalad-repos.json").read_text())
    # The failed host keeps every repo it had, none retired.
    assert [r["status"] for r in record["osf"]] == ["active", "active"]
    # The healthy host's update landed.
    assert record["gin"][0]["stars"] == 7
    # ...and was committed, not just written to the working tree.
    assert git("rev-list", "--count", "HEAD").strip() == "2"
    assert "failed: OSF" in git("log", "-1", "--pretty=%s")


def osf(ident, status):
    return OSFRepo(url=f"https://osf.io/{ident}/", id=ident, name=ident, status=status)


def test_gone_flip_ceiling():
    """A mass retirement is refused; ordinary churn is not."""
    before = [osf(f"r{i:03d}", Status.ACTIVE) for i in range(200)]

    def retire(n):
        return [
            osf(r.id, Status.GONE if i < n else Status.ACTIVE)
            for i, r in enumerate(before)
        ]

    # 5% of 200 == 10, so ten retirements are still plausible churn.
    check_gone_flip(RepoHost.OSF, before, retire(10))
    with pytest.raises(RuntimeError, match="refusing to write"):
        check_gone_flip(RepoHost.OSF, before, retire(11))
    with pytest.raises(RuntimeError, match="refusing to write"):
        check_gone_flip(RepoHost.OSF, before, retire(200))


def test_gone_flip_ceiling_floor_protects_small_hosts():
    """A tiny host must not have a ceiling of nearly zero."""
    before = [osf(f"r{i}", Status.ACTIVE) for i in range(12)]
    after = [
        osf(r.id, Status.GONE if i < 9 else Status.ACTIVE) for i, r in enumerate(before)
    ]
    check_gone_flip(RepoHost.OSF, before, after)


def test_missing_record_does_not_rebuild_over_a_tracked_one(tmp_path, monkeypatch):
    """A vanished record file must abort, not rebuild the dashboard from one run.

    Every host would otherwise see an empty prior collection, which also
    disables the gone-flip ceiling, so nothing else would catch it.
    """
    monkeypatch.chdir(tmp_path)
    git("init", "-q", ".")
    git("config", "user.email", "test@example.com")
    git("config", "user.name", "Test")
    (tmp_path / "datalad-repos.json").write_text(json.dumps(SEED))
    (tmp_path / "github-orgs.json").write_text("{}")
    git("add", "-A")
    git("commit", "-qm", "seed")
    (tmp_path / "datalad-repos.json").unlink()

    def gin_ok(self):
        yield GINRepo(
            id=1,
            name="owner/repo",
            url="https://gin.g-node.org/owner/repo",
            stars=7,
            status=Status.ACTIVE,
        )

    monkeypatch.setattr(GINSearcher, "get_datalad_repos", gin_ok)
    monkeypatch.setenv("GIN_TOKEN", "unused")

    result = CliRunner().invoke(main, ["--hosts", "GIN"])

    assert result.exit_code != 0
    assert isinstance(result.exception, RuntimeError)
    assert "refusing to start from an empty record" in str(result.exception)
    # Nothing was committed over the seeded record.
    assert git("rev-list", "--count", "HEAD").strip() == "1"
    assert json.loads(git("show", "HEAD:datalad-repos.json")) == SEED
