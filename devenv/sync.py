from devenv import constants
from devenv.lib import config, proc, uv


def main(context: dict[str, str]) -> int:
    reporoot = context["reporoot"]
    cfg = config.get_repo(reporoot)

    uv.install(
        cfg["uv"]["version"],
        cfg["uv"][constants.SYSTEM_MACHINE],
        cfg["uv"][f"{constants.SYSTEM_MACHINE}_sha256"],
        reporoot,
    )

    print("syncing .venv ...")
    proc.run(
        ("uv", "sync", "--frozen", "--verbose", "--active"),
        env={"RELAY_DEBUG": "1"}
    )

    # uv sync cannot editable install so long as the build-backend is setuptools
    print("editable install ...")
    proc.run(
        ("uv", "pip", "install", "-e", f"{reporoot}/py"),
    )

    print("installing pre-commit hooks ...")
    proc.run(("pre-commit", "install-hooks"))

    return 0
