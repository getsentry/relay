from devenv import constants
from devenv.lib import config, proc, uv


def main(context: dict[str, str]) -> int:
    reporoot = context["reporoot"]
    cfg = config.get_repo(reporoot)

    print("updating submodules")
    proc.run(("git", "submodule", "update", "--init", "--recursive"))

    uv.install(
        cfg["uv"]["version"],
        cfg["uv"][constants.SYSTEM_MACHINE],
        cfg["uv"][f"{constants.SYSTEM_MACHINE}_sha256"],
        reporoot,
    )

    print("syncing .venv ...")
    proc.run(
        (
            f"{reporoot}/.devenv/bin/uv",
            "sync",
            "--frozen",
            "--quiet",
            "--active",
            "--no-install-workspace",
        ),
    )

    print("installing pre-commit hooks ...")
    proc.run((f"{reporoot}/.venv/bin/pre-commit", "install", "--install-hooks"))

    print(
        """done!

note that you can build py/ with:

RELAY_DEBUG=1 uv pip install -v -e py
"""
    )

    return 0
