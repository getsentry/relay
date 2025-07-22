#!/bin/bash

checks-githubactions-checkruns \
    getsentry/relay \
    "${GO_REVISION_RELAY_REPO}" \
    "Integration Tests" \
    "Test All Features (ubuntu-latest)" \
    "Publish Relay to Internal AR (relay)" \
    "Publish Relay to Internal AR (relay-pop)" \
    "Upload build artifacts to gocd (relay)" \
    "Upload build artifacts to gocd (relay-pop)" \
