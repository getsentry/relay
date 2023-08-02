from __future__ import absolute_import

import os
import pytest

import wrapt
import sentry_sdk
from sentry_sdk.integrations import Integration

from sentry_sdk import Hub, capture_exception
from sentry_sdk.tracing import Transaction
from sentry_sdk.scope import add_global_event_processor

_ENVVARS_AS_TAGS = frozenset(
    [
        "GITHUB_WORKFLOW",  # The name of the workflow.
        "GITHUB_RUN_ID",  # A unique number for each run within a repository. This number does not change if you re-run the workflow run.
        "GITHUB_RUN_NUMBER",  # A unique number for each run of a particular workflow in a repository. This number begins at 1 for the workflow's first run, and increments with each new run. This number does not change if you re-run the workflow run.
        "GITHUB_ACTION",  # The unique identifier (id) of the action.
        "GITHUB_ACTOR",  # The name of the person or app that initiated the workflow. For example, octocat.
        "GITHUB_REPOSITORY",  # The owner and repository name. For example, octocat/Hello-World.
        "GITHUB_EVENT_NAME",  # The name of the webhook event that triggered the workflow.
        "GITHUB_EVENT_PATH",  # The path of the file with the complete webhook event payload. For example, /github/workflow/event.json.
        "GITHUB_WORKSPACE",  # The GitHub workspace directory path. The workspace directory is a copy of your repository if your workflow uses the actions/checkout action. If you don't use the actions/checkout action, the directory will be empty. For example, /home/runner/work/my-repo-name/my-repo-name.
        "GITHUB_SHA",  # The commit SHA that triggered the workflow. For example, ffac537e6cbbf934b08745a378932722df287a53.
        "GITHUB_REF",  # The branch or tag ref that triggered the workflow. For example, refs/heads/feature-branch-1. If neither a branch or tag is available for the event type, the variable will not exist.
        "GITHUB_HEAD_REF",  # Only set for pull request events. The name of the head branch.
        "GITHUB_BASE_REF",  # Only set for pull request events. The name of the base branch.
        "GITHUB_SERVER_URL",  # Returns the URL of the GitHub server. For example: https://github.com.
        "GITHUB_API_URL",  # Returns the API URL. For example: https://api.github.com.
        # Gitlab CI variables, as defined here https://docs.gitlab.com/ee/ci/variables/predefined_variables.html
        "CI_COMMIT_REF_NAME",  # Branch or tag name
        "CI_JOB_ID",  # Unique job ID
        "CI_JOB_URL",  # Job details URL
        "CI_PIPELINE_ID",  # Unique pipeline ID
        "CI_PROJECT_NAME",
        "CI_PROJECT_PATH",
        "CI_SERVER_URL",
        "GITLAB_USER_NAME",  # The name of the user who started the job.
    ]
)


class PytestIntegration(Integration):
    # Right now this integration type is only a carrier for options, and to
    # disable the pytest plugin. `setup_once` is unused.

    identifier = "pytest"

    def __init__(self, always_report=None):
        if always_report is None:
            always_report = os.environ.get(
                "PYTEST_SENTRY_ALWAYS_REPORT", ""
            ).lower() in ("1", "true", "yes")

        self.always_report = always_report

    @staticmethod
    def setup_once():
        @add_global_event_processor
        def procesor(event, hint):
            if Hub.current.get_integration(PytestIntegration) is None:
                return event

            for key in _ENVVARS_AS_TAGS:
                value = os.environ.get(key)
                if not value:
                    continue
                event.setdefault("tags", {})["pytest_environ.{}".format(key)] = value

            if "exception" in event:
                for exception in event["exception"]["values"]:
                    if "stacktrace" in exception:
                        _process_stacktrace(exception["stacktrace"])

            if "stacktrace" in event:
                _process_stacktrace(event["stacktrace"])

            return event


class Client(sentry_sdk.Client):
    def __init__(self, *args, **kwargs):
        kwargs.setdefault("dsn", os.environ.get("PYTEST_SENTRY_DSN", None))
        kwargs.setdefault("traces_sample_rate", float(os.environ.get("PYTEST_SENTRY_TRACES_SAMPLE_RATE", 1.0)))
        kwargs.setdefault("_experiments", {}).setdefault(
            "auto_enabling_integrations", True
        )
        kwargs.setdefault("environment", os.environ.get("SENTRY_ENVIRONMENT", "test"))
        kwargs.setdefault("integrations", []).append(PytestIntegration())

        sentry_sdk.Client.__init__(self, *args, **kwargs)


def hookwrapper(itemgetter, **kwargs):
    """
    A version of pytest.hookimpl that sets the current hub to the correct one
    and skips the hook if the integration is disabled.

    Assumes the function is a hookwrapper, ie yields once
    """

    @wrapt.decorator
    def _with_hub(wrapped, instance, args, kwargs):
        item = itemgetter(*args, **kwargs)
        hub = _resolve_hub_marker_value(item.get_closest_marker("sentry_client"))

        if hub.get_integration(PytestIntegration) is None:
            yield
        else:
            with hub:
                gen = wrapped(*args, **kwargs)

            while True:
                try:
                    with hub:
                        chunk = next(gen)

                    y = yield chunk

                    with hub:
                        gen.send(y)

                except StopIteration:
                    break

    def inner(f):
        return pytest.hookimpl(hookwrapper=True, **kwargs)(_with_hub(f))

    return inner


def pytest_load_initial_conftests(early_config, parser, args):
    early_config.addinivalue_line(
        "markers",
        "sentry_client(client=None): Use this client instance for reporting tests. You can also pass a DSN string directly, or a `Hub` if you need it.",
    )


def _start_transaction(**kwargs):
    transaction = Transaction.continue_from_headers(
        dict(Hub.current.iter_trace_propagation_headers()), **kwargs
    )
    transaction.same_process_as_parent = True
    return sentry_sdk.start_transaction(transaction)


@hookwrapper(itemgetter=lambda item: item)
def pytest_runtest_protocol(item):
    op = "pytest.runtest.protocol"

    name = item.nodeid

    # We use the full name including parameters because then we can identify
    # how often a single test has run as part of the same GITHUB_RUN_ID.

    with _start_transaction(op=op, name=u"{} {}".format(op, name)) as tx:
        yield

        # Purposefully drop transaction to spare quota. We only created it to
        # have a trace_id to correlate by.
        tx.sampled = False


@hookwrapper(itemgetter=lambda item: item)
def pytest_runtest_call(item):
    op = "pytest.runtest.call"

    name = item.nodeid

    # We use the full name including parameters because then we can identify
    # how often a single test has run as part of the same GITHUB_RUN_ID.

    with _start_transaction(op=op, name=u"{} {}".format(op, name)):
        yield


@hookwrapper(itemgetter=lambda fixturedef, request: request._pyfuncitem)
def pytest_fixture_setup(fixturedef, request):
    op = "pytest.fixture.setup"
    with _start_transaction(op=op, name=u"{} {}".format(op, fixturedef.argname)) as transaction:
        transaction.set_tag("pytest.fixture.scope", fixturedef.scope)
        yield


@hookwrapper(tryfirst=True, itemgetter=lambda item, call: item)
def pytest_runtest_makereport(item, call):
    sentry_sdk.set_tag("pytest.result", "pending")
    report = yield
    outcome = report.get_result().outcome
    sentry_sdk.set_tag("pytest.result", outcome)

    if call.when == "call" and outcome != "skipped":
        cur_exc_chain = getattr(item, "pytest_sentry_exc_chain", [])

        if call.excinfo is not None:
            item.pytest_sentry_exc_chain = cur_exc_chain = cur_exc_chain + [
                call.excinfo
            ]

        integration = Hub.current.get_integration(PytestIntegration)

        if (cur_exc_chain and call.excinfo is None) or integration.always_report:
            for exc_info in cur_exc_chain:
                capture_exception((exc_info.type, exc_info.value, exc_info.tb))


DEFAULT_HUB = Hub(Client())

_hub_cache = {}


def _resolve_hub_marker_value(marker_value):
    if id(marker_value) not in _hub_cache:
        _hub_cache[id(marker_value)] = rv = _resolve_hub_marker_value_uncached(
            marker_value
        )
        return rv

    return _hub_cache[id(marker_value)]


def _resolve_hub_marker_value_uncached(marker_value):
    if marker_value is None:
        marker_value = DEFAULT_HUB
    else:
        marker_value = marker_value.args[0]

    if callable(marker_value):
        marker_value = marker_value()

    if marker_value is None:
        # user explicitly disabled reporting
        return Hub()

    if isinstance(marker_value, str):
        return Hub(Client(marker_value))

    if isinstance(marker_value, dict):
        return Hub(Client(**marker_value))

    if isinstance(marker_value, Client):
        return Hub(marker_value)

    if isinstance(marker_value, Hub):
        return marker_value

    raise RuntimeError(
        "The `sentry_client` value must be a client, hub or string, not {}".format(
            repr(type(marker_value))
        )
    )


@pytest.fixture
def sentry_test_hub(request):
    """
    Gives back the current hub.
    """

    item = request.node
    return _resolve_hub_marker_value(item.get_closest_marker("sentry_client"))


def _process_stacktrace(stacktrace):
    for frame in stacktrace["frames"]:
        frame["in_app"] = not frame["module"].startswith(
            ("_pytest.", "pytest.", "pluggy.")
        )
