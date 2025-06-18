import nox

nox.options.default_venv_backend = "uv"
nox.options.sessions = ["lint", "test"]


@nox.session
def lint(session: nox.Session) -> None:
    session.install("pre-commit")
    session.install("-e", ".[dev]")

    args = *(session.posargs or ("--show-diff-on-failure",)), "--all-files"
    session.run("pre-commit", "run", *args)


@nox.session
def test(session: nox.Session) -> None:
    session.install("pytest")
    session.install("-e", ".[test]")
    session.run("pytest", *session.posargs)
