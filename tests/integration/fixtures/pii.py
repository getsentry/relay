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
