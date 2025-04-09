#!/bin/bash

/devinfra/scripts/checks/githubactions/checkruns.py \
    getsentry/relay \
    "${GO_REVISION_RELAY_REPO}" \
    "Integration Tests" \
    "Test All Features (ubuntu-latest)" \
    "Publish Relay to Internal AR (relay)" \
    "Publish Relay to Internal AR (relay-pop)"
