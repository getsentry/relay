#!/bin/bash

# Check Sentry for errors in the last 5 minutes for a given release (from GO_REVISION_RELAY_REPO)
#
# Required environment variables:
#   DRY_RUN: When dry-run is 'true' this script will not fail if the checks indicate an issue
#   ERROR_LIMIT: The number of error events to permit before failing
#   GO_REVISION_RELAY_REPO: Git commit hash (provided by GoCD)
#   SENTRY_AUTH_TOKEN: Sentry auth token (https://sentry.io/settings/account/api/auth-tokens/) (required by devinfra/scripts/checks/sentry/release_error_events.py)
#   SENTRY_BASE: Sentry base API URL (e.g. https://sentry.io/api/0)
#   SENTRY_PROJECTS: A space-separated list of <project_id>:<project_slug>:<service> tuples
#                    The reason for this is because project_slug and service do not always
#                    match, and we need the service to get the release name
#   SENTRY_SINGLE_TENANT: When single-tenant is 'true' this script will use the sentry-st organization instead of sentry
#   SKIP_CANARY_CHECKS: Whether to skip checks entirely (true/false)
#
# Since Processing and PoPs can be deployed independently, we don't fail if
# we can't find a release as it may not exist yet

# shellcheck disable=SC2206
projects=(${SENTRY_PROJECTS})

for project in "${projects[@]}"; do
  IFS=':' read -r -a project_info <<<"$project"
  project_id="${project_info[0]}"
  project_slug="${project_info[1]}"
  service="${project_info[2]}"

  release_name=$(./relay/scripts/get-sentry-release-name "${GO_REVISION_RELAY_REPO}" "${service}")
  if [ -z "${release_name}" ]; then
    echo "Failed to get the release name for ${service} at ${GO_REVISION_RELAY_REPO}"
    continue
  fi

  checks-sentry-release-error-events \
    --project-id="${project_id}" \
    --project-slug="${project_slug}" \
    --release="${release_name}" \
    --duration=5 \
    --error-events-limit="${ERROR_LIMIT}" \
    --dry-run="${DRY_RUN}" \
    --single-tenant="${SENTRY_SINGLE_TENANT}" \
    --skip-check="${SKIP_CANARY_CHECKS}" \
    --sentry-base="${SENTRY_BASE}"
done
