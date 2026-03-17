from collections import namedtuple
from unittest.mock import ANY, MagicMock, patch

import pytest
import responses
from django.urls import reverse
from requests import HTTPError
from requests.exceptions import ChunkedEncodingError, ConnectionError, Timeout

from sentry.api.serializers import serialize
from sentry.api.serializers.rest_framework import convert_dict_key_case, snake_to_camel_case
from sentry.constants import SentryAppStatus
from sentry.eventstream.types import EventStreamEventType
from sentry.exceptions import RestrictedIPAddress
from sentry.integrations.types import EventLifecycleOutcome
from sentry.issues.ingest import save_issue_occurrence
from sentry.models.activity import Activity
from sentry.sentry_apps.metrics import SentryAppWebhookFailureReason, SentryAppWebhookHaltReason
from sentry.sentry_apps.models.sentry_app import SentryApp
from sentry.sentry_apps.models.sentry_app_installation import SentryAppInstallation
from sentry.sentry_apps.models.servicehook import ServiceHook, ServiceHookProject
from sentry.sentry_apps.tasks.sentry_apps import (
    build_comment_webhook,
    installation_webhook,
    notify_sentry_app,
    process_resource_change_bound,
    regenerate_service_hooks_for_installation,
    send_alert_webhook_v2,
    send_webhooks,
    workflow_notification,
)
from sentry.sentry_apps.utils.errors import SentryAppSentryError
from sentry.services.eventstore.models import GroupEvent
from sentry.shared_integrations.exceptions import ClientError
from sentry.silo.base import SiloMode
from sentry.tasks.post_process import post_process_group
from sentry.testutils.asserts import (
    assert_count_of_metric,
    assert_failure_metric,
    assert_halt_metric,
    assert_many_halt_metrics,
    assert_success_metric,
)
from sentry.testutils.cases import TestCase
from sentry.testutils.helpers import with_feature
from sentry.testutils.helpers.datetime import before_now
from sentry.testutils.helpers.eventprocessing import write_event_to_cache
from sentry.testutils.silo import assume_test_silo_mode, assume_test_silo_mode_of, control_silo_test
from sentry.testutils.skips import requires_snuba
from sentry.types.activity import ActivityType
from sentry.types.rules import RuleFuture
from sentry.users.services.user.service import user_service
from sentry.utils import json
from sentry.utils.http import absolute_uri
from sentry.utils.sentry_apps import SentryAppWebhookRequestsBuffer
from sentry.utils.sentry_apps.service_hook_manager import (
    create_or_update_service_hooks_for_installation,
)
from tests.sentry.issues.test_utils import OccurrenceTestMixin

pytestmark = [requires_snuba]


def raiseStatusFalse() -> bool:
    return False


def raiseStatusTrue() -> bool:
    return True


def raiseException():
    raise Exception


def raiseHTTPError():
    raise HTTPError()


class RequestMock:
    def __init__(self):
        self.body = "blah blah"


headers = {"Sentry-Hook-Error": "d5111da2c28645c5889d072017e3445d", "Sentry-Hook-Project": "1"}
html_content = "a bunch of garbage HTML"
json_content = '{"error": "bad request"}'

MockResponse = namedtuple(
    "MockResponse",
    ["headers", "content", "text", "ok", "status_code", "raise_for_status", "request"],
)

MockFailureHTMLContentResponseInstance = MockResponse(
    headers, html_content, "", True, 400, raiseStatusFalse, RequestMock()
)
MockFailureJSONContentResponseInstance = MockResponse(
    headers, json_content, "", True, 400, raiseStatusFalse, RequestMock()
)
MockFailureResponseInstance = MockResponse(
    headers, html_content, "", True, 400, raiseStatusFalse, RequestMock()
)
MockResponseWithHeadersInstance = MockResponse(
    headers, html_content, "", True, 400, raiseStatusFalse, RequestMock()
)
MockResponseInstance = MockResponse({}, b"{}", "", True, 200, raiseStatusFalse, None)
MockResponse404 = MockResponse({}, b'{"bruh": "bruhhhhhhh"}', "", False, 404, raiseException, None)
MockResponse504 = MockResponse(headers, json_content, "", False, 504, raiseStatusFalse, None)
MockResponse503 = MockResponse(headers, json_content, "", False, 503, raiseStatusFalse, None)
MockResponse502 = MockResponse(headers, json_content, "", False, 502, raiseHTTPError, None)


@patch("sentry.utils.sentry_apps.webhooks.safe_urlopen", return_value=MockResponseInstance)
class TestWorkflowNotification(TestCase):
    def setUp(self) -> None:
        self.project = self.create_project()
        self.user = self.create_user()

        self.sentry_app = self.create_sentry_app(
            organization=self.project.organization,
            events=["issue.resolved", "issue.ignored", "issue.assigned"],
        )

        self.install = self.create_sentry_app_installation(
            organization=self.project.organization, slug=self.sentry_app.slug
        )

        self.issue = self.create_group(project=self.project)

    @patch("sentry.integrations.utils.metrics.EventLifecycle.record_event")
    def test_sends_resolved_webhook(self, mock_record: MagicMock, safe_urlopen: MagicMock) -> None:
        workflow_notification(self.install.id, self.issue.id, "resolved", self.user.id)

        ((_, kwargs),) = safe_urlopen.call_args_list
        assert kwargs["url"] == self.sentry_app.webhook_url
        assert kwargs["headers"]["Sentry-Hook-Resource"] == "issue"
        data = json.loads(kwargs["data"])
        assert data["action"] == "resolved"
        assert data["data"]["issue"]["id"] == str(self.issue.id)

        # SLO assertions
        assert_success_metric(mock_record)
        # PREPARE_WEBHOOK (success) -> SEND_WEBHOOK (success) -> SEND_WEBHOOK (success)
        assert_count_of_metric(
            mock_record=mock_record, outcome=EventLifecycleOutcome.STARTED, outcome_count=3
        )
        assert_count_of_metric(
            mock_record=mock_record, outcome=EventLifecycleOutcome.SUCCESS, outcome_count=3
        )

    @patch("sentry.integrations.utils.metrics.EventLifecycle.record_event")
    def test_sends_resolved_webhook_as_Sentry_without_user(
        self, mock_record: MagicMock, safe_urlopen: MagicMock
    ) -> None:
        workflow_notification(self.install.id, self.issue.id, "resolved", None)

        ((_, kwargs),) = safe_urlopen.call_args_list
        data = json.loads(kwargs["data"])
        assert data["actor"]["type"] == "application"
        assert data["actor"]["id"] == "sentry"
        assert data["actor"]["name"] == "Sentry"

        # SLO assertions
        assert_success_metric(mock_record)
        # PREPARE_WEBHOOK (success) -> SEND_WEBHOOK (success) -> SEND_WEBHOOK (success)
        assert_count_of_metric(
            mock_record=mock_record, outcome=EventLifecycleOutcome.STARTED, outcome_count=3
        )
        assert_count_of_metric(
            mock_record=mock_record, outcome=EventLifecycleOutcome.SUCCESS, outcome_count=3
        )

    @patch("sentry.integrations.utils.metrics.EventLifecycle.record_event")
    def test_does_not_send_if_no_service_hook_exists(
        self, mock_record: MagicMock, safe_urlopen: MagicMock
    ) -> None:
        sentry_app = self.create_sentry_app(
            name="Another App", organization=self.project.organization, events=[]
        )
        install = self.create_sentry_app_installation(
            organization=self.project.organization, slug=sentry_app.slug
        )
        workflow_notification(install.id, self.issue.id, "assigned", self.user.id)
        assert not safe_urlopen.called

        # SLO assertions
        assert_halt_metric(
            mock_record=mock_record,
            error_msg=SentryAppWebhookHaltReason.MISSING_SERVICEHOOK,
        )
        # APP_CREATE (success) -> UPDATE_WEBHOOK (success) -> GRANT_EXCHANGER (success) -> PREPARE_WEBHOOK (success) -> send_webhook (halted)
        assert_count_of_metric(
            mock_record=mock_record, outcome=EventLifecycleOutcome.STARTED, outcome_count=5
        )
        assert_count_of_metric(
            mock_record=mock_record, outcome=EventLifecycleOutcome.SUCCESS, outcome_count=4
        )
        assert_count_of_metric(
            mock_record=mock_record, outcome=EventLifecycleOutcome.HALTED, outcome_count=1
        )

    @patch("sentry.integrations.utils.metrics.EventLifecycle.record_event")
    def test_does_not_send_if_event_not_in_app_events(
        self, mock_record: MagicMock, safe_urlopen: MagicMock
    ) -> None:
        sentry_app = self.create_sentry_app(
            name="Another App",
            organization=self.project.organization,
            events=["issue.resolved", "issue.ignored"],
        )
        install = self.create_sentry_app_installation(
            organization=self.project.organization, slug=sentry_app.slug
        )
        workflow_notification(install.id, self.issue.id, "assigned", self.user.id)
        assert not safe_urlopen.called

        # SLO assertions
        assert_failure_metric(
            mock_record, SentryAppSentryError(SentryAppWebhookFailureReason.EVENT_NOT_IN_SERVCEHOOK)
        )
        # APP_CREATE (success) -> UPDATE_WEBHOOK (success) -> GRANT_EXCHANGER (success) -> PREPARE_WEBHOOK (success) -> SEND_WEBHOOK (failure)
        assert_count_of_metric(
            mock_record=mock_record, outcome=EventLifecycleOutcome.STARTED, outcome_count=5
        )
        assert_count_of_metric(
            mock_record=mock_record, outcome=EventLifecycleOutcome.SUCCESS, outcome_count=4
        )
        assert_count_of_metric(
            mock_record=mock_record, outcome=EventLifecycleOutcome.FAILURE, outcome_count=1
        )
