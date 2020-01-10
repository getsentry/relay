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


_version_re = re.compile(r'^version\s*=\s*"(.*?)"\s*$(?m)')


DEBUG_BUILD = os.environ.get("RELAY_DEBUG") == "1"

with open("README", "rb") as f:
    readme = f.read()


if os.path.isfile("../relay-cabi/Cargo.toml"):
    with open("../relay-cabi/Cargo.toml") as f:
        version = _version_re.search(f.read()).group(1)
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
    cmd = ["cargo", "build"]
    if not DEBUG_BUILD:
        cmd.append("--release")
        target = "release"
    else:
        target = "debug"

    # Step 0: find rust sources
    if not os.path.isfile("../relay-cabi/Cargo.toml"):
        scratchpad = tempfile.mkdtemp()

        @atexit.register
        def delete_scratchpad():
            try:
                shutil.rmtree(scratchpad)
            except (IOError, OSError):
                pass

        zf = zipfile.ZipFile("rustsrc.zip")
        zf.extractall(scratchpad)
        rust_path = scratchpad + "/rustsrc/relay-cabi"
    else:
        rust_path = "../relay-cabi"
        scratchpad = None

    # Step 1: build the rust library
    build = spec.add_external_build(cmd=cmd, path=rust_path)

    rtld_flags = ["NOW"]
    if sys.platform == "darwin":
        rtld_flags.append("NODELETE")
    spec.add_cffi_module(
        module_path="sentry_relay._lowlevel",
        dylib=lambda: build.find_dylib("relay", in_path="target/%s" % target),
        header_filename=lambda: build.find_header("relay.h", in_path="include"),
        rtld_flags=rtld_flags,
    )


setup(
    name="sentry-relay",
    version=version,
    packages=find_packages(),
    author="Sentry",
    license="BSL-1.1",
    author_email="hello@sentry.io",
    description="A python library to access sentry relay functionality.",
    long_description=readme,
    include_package_data=True,
    zip_safe=False,
    platforms="any",
    install_requires=["milksnake>=0.1.2"],
    setup_requires=["milksnake>=0.1.2"],
    milksnake_tasks=[build_native],
    cmdclass={"sdist": CustomSDist},
)
