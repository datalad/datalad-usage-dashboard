import nox

nox.options.reuse_existing_virtualenvs = True


@nox.session
def run(session):
    session.install("-r", "requirements.txt")
    session.run("python", "-m", "find_datalad_repos", *session.posargs)


@nox.session
def diff(session):
    session.install("-r", "requirements.txt")
    session.run("python", "-m", "find_datalad_repos.diff", *session.posargs)


@nox.session
def addids(session):
    session.install("-r", "requirements.txt")
    session.run("python", "-m", "find_datalad_repos.addids")


@nox.session
def typing(session):
    session.install("-r", "requirements.txt")
    session.install("mypy", "types-requests")
    session.run("mypy", "find_datalad_repos")
