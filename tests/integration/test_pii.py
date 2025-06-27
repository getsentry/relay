def test_snapshot_scrub_span_sentry_tags_advanced_rules(
    mini_sentry, relay, relay_snapshot
):
    project_id = 42
    relay = relay(
        mini_sentry,
        options={"geoip": {"path": "tests/fixtures/GeoIP2-Enterprise-Test.mmdb"}},
    )
    config = mini_sentry.add_basic_project_config(project_id)
    config["config"]["piiConfig"]["applications"][
        "$span.sentry_tags.'user.geo.country_code'"
    ] = ["@anything:mask"]
    config["config"]["piiConfig"]["applications"][
        "$span.sentry_tags.'user.geo.subregion'"
    ] = ["@anything:mask"]

    relay.send_event(
        project_id,
        {
            "user": {"ip_address": "2.125.160.216"},
            "spans": [
                {
                    "timestamp": 1746007551,
                    "start_timestamp": 1746007545,
                    "span_id": "aaaaaaaa00000000",
                    "trace_id": "aaaaaaaaaaaaaaaaaaaa000000000000",
                    "sentry_tags": {
                        "user.geo.country_code": "ATdasdsa",
                        "user.geo.subregion": "12",
                    },
                }
            ],
        },
    )

    envelope = mini_sentry.captured_events.get(timeout=1)
    event = envelope.get_event()
    assert relay_snapshot("json") == event
