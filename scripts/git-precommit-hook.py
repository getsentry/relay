#!/usr/bin/env python
from __future__ import print_function
import os
import sys
import subprocess


def has_cargo_fmt():
    """Runs a quick check to see if cargo fmt is installed."""
    try:
        c = subprocess.Popen(
            ["cargo", "fmt", "--", "--help"],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        return c.wait() == 0
    except OSError:
        return False


def get_modified_files():
    """Returns a list of all modified files."""
    c = subprocess.Popen(
        ["git", "diff-index", "--cached", "--name-only", "HEAD"], stdout=subprocess.PIPE
    )
    return c.communicate()[0].splitlines()


def run_format_check(files):
    rust_files = [x for x in files if x.endswith(".rs") and os.path.isfile(x)]
    if not rust_files:
        return 0
    rv = subprocess.Popen(
        ["cargo", "fmt", "--", "--check", "--color=always"]
        + rust_files
    ).wait()
    if rv != 0:
        print("", file=sys.stderr)
        print(
            "\033[1m\033[2minfo: to fix this run `cargo fmt` and "
            "commit again\033[0m",
            file=sys.stderr,
        )
    return rv


def main():
    if not has_cargo_fmt:
        print("warning: cargo fmt not installed")
        return
    sys.exit(run_format_check(get_modified_files()))


if __name__ == "__main__":
    main()
