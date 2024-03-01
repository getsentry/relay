#!/bin/bash

# shellcheck disable=SC2206
project_ids=(${SENTRY_PROJECT_IDS})
# shellcheck disable=SC2206
project_slugs=(${SENTRY_PROJECTS})


if [ ${##project_ids[@]} -ne ${##project_slugs[@]} ]; then
  echo "Error: SENTRY_PROJECT_IDS and SENTRY_PROJECTS must have the same number of elements"
  exit 1
fi

for i in "${!project_ids[@]}"; do
  /devinfra/scripts/checks/sentry/release_error_events.py \
    --project-id="${project_ids[i]}" \
    --project-slug="${project_slugs[i]}" \
    --release="relay@${GO_REVISION_RELAY_REPO}" \
    --duration=5 \
    --error-events-limit="${ERROR_LIMIT}" \
    --dry-run="${DRY_RUN}" \
    --single-tenant="${SENTRY_SINGLE_TENANT}" \
    --skip-check="${SKIP_CANARY_CHECKS}" \
    --sentry-base="${SENTRY_BASE}"
done
