# Relay load test tools

## Load test contains tools for load testing relay.

The project contains two tools: a load tester based on Locust (see https://locust.io/) and a
fake Sentry server that contains just enough functionality to get relay working with an upstream.

## Fake Sentry Server

The FakeSentryServer runs a Flask server that responds to the security challenge messages from Relay and
is able to provide project configurations for any project (it responds with a canned project configuration)

The FakeSentryServer can be configured via the config/fake.sentry.config.yml file (situated in the top level directory).

To start the Fake Sentry Server run:
    .venv/bin/python -m fake_sentry.fake_sentry
or
    make fake-sentry

## Load tester

In order to create a virtual env and install all the necessary dependencies just call once

    make config

In order to load test you need to invoke locust and pass it the locust file that needs to be executed.
Presuming that you are in the load-tests directory you can run:

    make TEST=simple load-test
    make TEST=kafka_consumers load-test

Which will ensure that the virtual environment is installed and set up and will call:
`.venv/bin/locust -f simple_locustfile.py`

You can run other locust files directly just by starting the virtual env and calling:

    .venv/bin/locust -f <MY-LOCUST-FILE> 
    
After starting a load test as described above locust will start a control web server from which
you can start various load test sessions. Just go with a browser to http://localhost.8089

You can also start a session without a web server by passing the `--no-web` flag, like in the
example below (that starts 4 users, with a spawn rate of 2 per second and runs for 20 seconds).

    locust -f kafka_consumers_locustfile.py --no-web -u 4 -r 2 --run-time 20s --stop-timeout 10
    
Please consult the locust documentation for details: https://docs.locust.io/en/0.14.6/running-locust-without-web-ui.html

**Note:** At the moment (18.05.2020) we are using locust 0.14.6, which is not the latest version and has slightly different Python API and CLI.
