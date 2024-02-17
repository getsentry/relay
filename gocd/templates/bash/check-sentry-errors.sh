#!/bin/bash

# TODO: Make this work with s4s
/devinfra/scripts/checks/sentry/release_error_events.py \
  --project-id="${SENTRY_PROJECT_ID}" \
  --project-slug="${SENTRY_PROJECT}" \
  --release="relay@${GO_REVISION_GETSENTRY_REPO}" \
  --duration=5 \
  --error-events-limit="${ERROR_LIMIT}" \
  --dry-run="${DRY_RUN}" \
  --single-tenant="${SENTRY_SINGLE_TENANT}" \
  --skip-check="${SKIP_CANARY_CHECKS}"
