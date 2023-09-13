#!/bin/bash

/devinfra/scripts/checks/githubactions/checkruns.py \
    getsentry/relay \
    "${GO_REVISION_RELAY_REPO}" \
    "Integration Tests" \
    "Test (macos-latest)" \
    "Test (windows-latest)" \
    "Test All Features (ubuntu-latest)" \
    "Push GCR Docker Image"
