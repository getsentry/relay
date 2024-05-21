import os
import re
import sys
import atexit
import shutil
import zipfile
import tempfile
import subprocess
from setuptools import setup, find_packages
from distutils.command.sdist import sdist


_version_re = re.compile(r'(?m)^version\s*=\s*"(.*?)"\s*$')


DEBUG_BUILD = os.environ.get("RELAY_DEBUG") == "1"

with open("README", encoding="UTF-8") as f:
    readme = f.read()


if os.path.isfile("../relay-cabi/Cargo.toml"):
    with open("../relay-cabi/Cargo.toml") as f:
        match = _version_re.search(f.read())
        assert match is not None
        version = match[1]
else:
    with open("version.txt") as f:
        version = f.readline().strip()


def vendor_rust_deps():
    subprocess.Popen(["scripts/git-archive-all", "py/rustsrc.zip"], cwd="..").wait()


def write_version():
    with open("version.txt", "wb") as f:
        f.write(("%s\n" % version).encode())


class CustomSDist(sdist):
    def run(self):
        vendor_rust_deps()
        write_version()
        sdist.run(self)


def build_native(spec):
    cmd = ["cargo", "build", "-p", "relay-cabi"]
    if not DEBUG_BUILD:
        cmd.extend(("--profile", "release-cabi"))
        target = "release-cabi"
    else:
        target = "debug"

    # Step 0: find rust sources
    if not os.path.isfile("../relay-cabi/Cargo.toml"):
        scratchpad = tempfile.mkdtemp()

        @atexit.register
        def delete_scratchpad():
            try:
                shutil.rmtree(scratchpad)
            except OSError:
                pass

        zf = zipfile.ZipFile("rustsrc.zip")
        zf.extractall(scratchpad)
        rust_path = scratchpad + "/rustsrc"
    else:
        rust_path = ".."
        scratchpad = None

    # if the lib already built we replace the command
    if os.environ.get("SKIP_RELAY_LIB_BUILD") is not None:
        cmd = ["echo", "'Use pre-built library.'"]

    # Step 1: build the rust library
    build = spec.add_external_build(cmd=cmd, path=rust_path)

    def find_dylib():
        cargo_target = os.environ.get("CARGO_BUILD_TARGET")
        if cargo_target:
            in_path = f"target/{cargo_target}/{target}"
        else:
            in_path = "target/%s" % target
        return build.find_dylib("relay_cabi", in_path=in_path)

    rtld_flags = ["NOW"]
    if sys.platform == "darwin":
        rtld_flags.append("NODELETE")
    spec.add_cffi_module(
        module_path="sentry_relay._lowlevel",
        dylib=find_dylib,
        header_filename=lambda: build.find_header(
            "relay.h", in_path="relay-cabi/include"
        ),
        rtld_flags=rtld_flags,
    )


setup(
    name="sentry-relay",
    version=version,
    packages=find_packages(),
    author="Sentry",
    license="FSL-1.0-Apache-2.0",
    author_email="hello@sentry.io",
    description="A python library to access sentry relay functionality.",
    long_description=readme,
    long_description_content_type="text/markdown",
    include_package_data=True,
    package_data={"sentry_relay": ["py.typed", "_lowlevel.pyi"]},
    zip_safe=False,
    platforms="any",
    python_requires=">=3.10",
    install_requires=["milksnake>=0.1.6"],
    setup_requires=["milksnake>=0.1.6"],
    milksnake_tasks=[build_native],
    cmdclass={"sdist": CustomSDist},  # type: ignore
)
