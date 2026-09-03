from devenv import constants
from devenv.lib import brew, proc

import shutil


def main(context: dict[str, str]) -> int:
    reporoot = context["reporoot"]

    brew.install()

    if constants.DARWIN:
        proc.run(
            (f"{constants.homebrew_bin}/brew", "bundle"),
            cwd=reporoot,
        )

    if not shutil.which("rustup"):
        raise SystemExit("rustup not on PATH. Did you run `direnv allow`?")

    proc.run(
        (
            "rustup",
            "toolchain",
            "install",
            "stable",
            "--profile",
            "minimal",
            "--component",
            "rustfmt",
            "--component",
            "clippy",
            "--no-self-update",
        )
    )

    print("updating submodules")
    proc.run(("git", "submodule", "update", "--init", "--recursive"))

    print("syncing .venv ...")

    if not shutil.which("uv"):
        raise SystemExit("uv not on PATH. Did you run `direnv allow`?")

    proc.run(
        (
            "uv",
            "sync",
            "--frozen",
            "--quiet",
            "--active",
            "--inexact",  # don't uninstall sentry_relay
        ),
    )

    print("installing pre-commit hooks ...")
    proc.run((f"{reporoot}/.venv/bin/pre-commit", "install", "--install-hooks"))

    print("""done!

note that you can build py/ with:

RELAY_DEBUG=1 uv pip install -v -e py
""")

    return 0
