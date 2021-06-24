import socket
import subprocess
import itertools
import os
from os import path
from typing import Optional
import json

import pytest

# all tests fixtures must be imported so that pytest finds them
from .fixtures.gobetween import gobetween  # noqa
from .fixtures.haproxy import haproxy  # noqa
from .fixtures.mini_sentry import mini_sentry  # noqa
from .fixtures.relay import relay, get_relay_binary  # noqa
from .fixtures.processing import (
    kafka_consumer,
    get_topic_name,
    processing_config,
    relay_with_processing,
    events_consumer,
    outcomes_consumer,
    transactions_consumer,
    attachments_consumer,
    sessions_consumer,
    metrics_consumer,
)  # noqa


@pytest.fixture
def random_port():
    def inner():
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        s.bind(("127.0.0.1", 0))
        s.listen(1)
        port = s.getsockname()[1]
        s.close()
        return port

    return inner


@pytest.fixture
def background_process(request):
    def inner(*args, **kwargs):
        p = subprocess.Popen(*args, **kwargs)
        request.addfinalizer(p.kill)
        return p

    return inner


@pytest.fixture
def config_dir(tmpdir):
    counters = {}

    def inner(name):
        counters.setdefault(name, 0)
        counters[name] += 1
        return tmpdir.mkdir("{}-{}".format(name, counters[name]))

    return inner


def _parse_version(version):
    if version == "latest":
        return (float("inf"),)

    return tuple(map(int, version.split(".")))


@pytest.fixture(  # noqa
    params=[
        "relay->sentry",
        "relay->relay->sentry",
        "relay->ha->relay->gb->sentry",
        "relay->gb->relay->ha->sentry",
    ],
)
def relay_chain(request, mini_sentry, relay, gobetween, haproxy):  # noqa
    configured_versions = list(
        filter(bool, (os.environ.get("RELAY_VERSION_CHAIN") or "").split(","))
    )

    def inner(min_relay_version="0"):
        if not configured_versions:
            versions_iter = itertools.repeat("latest")
        else:
            configured_versions.reverse()
            configured_versions.append("latest")
            versions_iter = iter(configured_versions)

        def relay_with_version(upstream, **kwargs):
            version = next(versions_iter)

            if _parse_version(version) < _parse_version(min_relay_version):
                version = min_relay_version

            return relay(upstream, version=version, **kwargs)

        factories = {"gb": gobetween, "ha": haproxy, "relay": relay_with_version}

        parts = iter(reversed(request.param.split("->")))
        assert next(parts) == "sentry"

        rv = mini_sentry
        for part in parts:
            rv = factories[part](rv)

        return rv

    return inner


def _fixture_file_path_for_test_file(test_file_path, file_name):
    prefix, test_file_name = path.split(test_file_path)
    test_file_name = path.splitext(test_file_name)[0]
    # remove the 'test_' from the file name (makes the directory structure cleaner)
    if test_file_name.startswith("test_"):
        test_file_name = test_file_name[5:]

    return path.abspath(path.join(prefix, "fixtures", test_file_name, file_name))


class _JsonFixtureProvider(object):
    def __init__(self, test_file_path: str):
        """
        Initializes a JsonFixtureProvider with the current test file path (in order to create
        fixtures relative to the current directory
        :param test_file_path: should be set to __file__
        """
        if not test_file_path:
            raise ValueError(
                " JsonFixtureProvider should be initialized with the current test file path, i.e. __file__\n."
                " the code should something look like: fixture_provider = json_fixture_provider(__file__) "
            )

        self.test_file_path = test_file_path

    def save(self, obj, file_name: str, ext: Optional[str] = None):
        """
        Saves an object as a json fixture to the specified file_name.
        The directory of the file in the file system is relative to the test_file_path passed in the fixture creation
        :param obj:
        :param file_name:
        :param ext:
        :return:
        """
        if ext is not None:
            file_name = file_name + ext
        file_name = file_name + ".json"

        file_path = _fixture_file_path_for_test_file(self.test_file_path, file_name)

        if path.isfile(file_path):
            print(
                "trying to override existing fixture.\n If fixture is out of date delete manually.",
                file_path,
            )
            raise ValueError("Will not override ", file_path)

        with open(file_path, "w") as f:
            print(obj)
            return json.dump(obj, f)

    def load(self, file_name: str, ext: Optional[str] = None):
        """
        Loads a fixture with the specified file name (with the path realtive to the current test)
        :param file_name: the file name
        :param ext: an optional extension to be appended to the file name ( the ext should contain
            '.' i.e. it should be something like '.fixture'
        :return: an object deserialized from the specified file
        """
        if ext is not None:
            file_name = file_name + ext
        file_name = file_name + ".json"

        file_path = _fixture_file_path_for_test_file(self.test_file_path, file_name)

        if not path.isfile(file_path):
            return None

        with open(file_path, "rb") as f:
            return json.load(f)


@pytest.fixture
def json_fixture_provider():
    def inner(test_file_path: str):
        return _JsonFixtureProvider(test_file_path)

    return inner


@pytest.hookimpl(hookwrapper=True)
def pytest_runtest_call(item):
    """This handles the extra_failure_checks mark to perform further assertions.

    The mark can be added e.g. by a fixture that wants to run
    something that can raise pytest.fail.Exception after the test has
    completed.

    Such checks will not be run when the test already failed, just
    like multiple assertions within a test will stop the test
    execution at the first failure.
    """
    yield
    for marker in item.iter_markers("extra_failure_checks"):
        for check_func in marker.kwargs.get("checks", []):
            check_func()
