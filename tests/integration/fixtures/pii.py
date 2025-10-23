from dataclasses import dataclass
from typing import Any
import pytest


@pytest.fixture(
    params=[
        ("@ip", "127.0.0.1", "[ip]"),
        ("@email", "test@example.com", "[email]"),
        ("@creditcard", "4242424242424242", "[creditcard]"),
        ("@iban", "DE89370400440532013000", "[iban]"),
        ("@mac", "4a:00:04:10:9b:50", "*****************"),
        (
            "@uuid",
            "ceee0822-ed8f-4622-b2a3-789e73e75cd1",
            "************************************",
        ),
        ("@imei", "356938035643809", "[imei]"),
        (
            "@pemkey",
            "-----BEGIN EC PRIVATE KEY-----\nMIHbAgEBBEFbLvIaAaez3q0u6BQYMHZ28B7iSdMPPaODUMGkdorl3ShgTbYmzqGL\n-----END EC PRIVATE KEY-----",
            "-----BEGIN EC PRIVATE KEY-----\n[pemkey]\n-----END EC PRIVATE KEY-----",
        ),
        (
            "@urlauth",
            "https://username:password@example.com/",
            "https://[auth]@example.com/",
        ),
        ("@usssn", "078-05-1120", "***********"),
        ("@userpath", "/Users/john/Documents", "/Users/[user]/Documents"),
        ("@password", "my_password_123", ""),
        ("@bearer", "Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9", "Bearer [token]"),
    ],
    ids=[
        "ip",
        "email",
        "creditard",
        "iban",
        "mac",
        "uuid",
        "imei",
        "pemkey",
        "urlauth",
        "usssn",
        "userpath",
        "password",
        "bearer",
    ],
)
def scrubbing_rule(request):
    return request.param


@pytest.fixture(
    params=[
        ("password", "my_password_123", "[Filtered]", "@password:filter"),
        ("secret_key", "my_secret_key_123", "[Filtered]", "@password:filter"),
        ("api_key", "my_api_key_123", "[Filtered]", "@password:filter"),
    ],
    ids=["password", "secret_key", "api_key"],
)
def secret_attribute(request):
    return request.param


@dataclass
class NonDestructive:
    pii_config: Any
    scrub_data: bool
    scrub_defaults: bool
    input_message: str
    expected_output: str
    additional_checks: Any

    def install(self, project_config):
        project_config["config"]["piiConfig"] = self.pii_config
        project_config["config"]["datascrubbingSettings"] = {
            "scrubData": self.scrub_data,
            "scrubDefaults": self.scrub_defaults,
        }

    def scrubs(self):
        return self.input_message != self.expected_output


@pytest.fixture(
    params=[
        NonDestructive(
            pii_config=None,
            scrub_data=True,
            scrub_defaults=True,
            input_message="API request failed with Bearer ABC123XYZ789TOKEN and returned 401",
            expected_output="API request failed with Bearer [token] and returned 401",
            additional_checks=lambda formatted_value: (
                "Bearer [token]" in formatted_value
                and "ABC123XYZ789TOKEN" not in formatted_value
                and "API request failed" in formatted_value
                and "returned 401" in formatted_value
            ),
        ),
        NonDestructive(
            pii_config={},
            scrub_data=False,
            scrub_defaults=True,
            input_message="API failed with Bearer ABC123TOKEN for user@example.com using card 4111-1111-1111-1111",
            expected_output="API failed with Bearer ABC123TOKEN for user@example.com using card 4111-1111-1111-1111",
            additional_checks=None,
        ),
        NonDestructive(
            pii_config={},
            scrub_data=True,
            scrub_defaults=True,
            input_message="API failed with Bearer ABC123TOKEN for user@example.com using card 4111-1111-1111-1111",
            expected_output="API failed with Bearer [token] for [email] using card [creditcard]",
            additional_checks=None,
        ),
        NonDestructive(
            pii_config=None,
            scrub_data=True,
            scrub_defaults=True,
            input_message="User's password is 12345",
            expected_output="User's password is 12345",
            additional_checks=None,
        ),
    ],
    ids=[
        "bearer_token_scrubbing",
        "no_scrubbing_when_disabled",
        "all_scrubbing_when_enabled",
        "password_not_scrubbed",
    ],
)
def non_destructive(request):
    """
    A collection of non destructive PII rules.

    The non-destructive rules are a custom set of rules applied to specific keys in payloads,
    like the error message.
    """
    return request.param
