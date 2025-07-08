#!/bin/bash

eval $(project_env_vars --region="${SENTRY_REGION}")

/devinfra/scripts/k8s/k8stunnel

k8s-deploy \
    --label-selector="service=relay-pop" \
    --image="us-central1-docker.pkg.dev/internal-sentry/relay/relay-pop:${GO_REVISION_RELAY_REPO}" \
    --container-name="relay"
