# Relay load test tools

## Load test contains tools for load testing relay.

The project contains two tools: a load tester based on Locust (see https://locust.io/) and a
fake Sentry server that contains just enough functionality to get relay working with an upstream.

## Fake Sentry Server

The FakeSentryServer runs a Flask server that responds to the security challenge messages from Relay and
is able to provide project configurations for any project (it responds with a canned project configuration)

The FakeSentryServer can be configured via the fake.sentry.config.yml file (situated in the top level directory).

To start the Fake Sentry Server run:
    .venv/bin/python -m relay_load_tests.fake_sentry
or
    make fake-sentry

## Load tester

The load tester is a locust script that can be used to send messages to Relay.
The load tester is configured via the locust_config.yml file (situated in the top level directory).

To start the load tester just run:
    .venv/bin/locust -f locustfile.py
or
    make load-test
