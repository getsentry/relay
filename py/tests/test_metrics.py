import sentry_relay


def test_to_metrics_symbol():
    assert 0x90285684421F9857 == sentry_relay.metrics.to_metrics_symbol("asdf")
