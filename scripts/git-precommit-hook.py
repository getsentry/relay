#!/usr/bin/env python3
import os
import pathlib
import sys
import subprocess


def has_cargo_fmt():
    """Runs a quick check to see if cargo fmt is installed."""
    try:
        c = subprocess.run(["cargo", "fmt", "--", "--help"], capture_output=True)
    except OSError:
        return False
    else:
        return c.returncode == 0


def get_modified_files():
    """Returns a list of all modified files."""
    c = subprocess.run(
        ["git", "diff-index", "--cached", "--name-only", "HEAD"], capture_output=True
    )
    return [pathlib.Path(os.fsdecode(p)) for p in c.stdout.splitlines()]


def run_format_check(files):
    rust_files = [x for x in files if x.suffix == "rs" and x.isfile()]
    if not rust_files:
        return 0
    ret = subprocess.run(
        ["cargo", "fmt", "--", "--check", "--color=always"] + rust_files
    )
    if ret.returncode != 0:
        print("", file=sys.stderr)
        print(
            "\033[1m\033[2minfo: to fix this run `cargo fmt --all` and "
            "commit again\033[0m",
            file=sys.stderr,
        )
    return ret.returncode


def main():
    if not has_cargo_fmt():
        print("warning: cargo fmt not installed")
        return
    sys.exit(run_format_check(get_modified_files()))


if __name__ == "__main__":
    main()
