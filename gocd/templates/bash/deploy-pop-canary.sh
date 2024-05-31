#!/bin/bash

eval $(/devinfra/scripts/regions/project_env_vars.py --region="${SENTRY_REGION}")

/devinfra/scripts/k8s/k8stunnel

/devinfra/scripts/k8s/k8s-deploy.py \
    --label-selector="service=relay-pop,env=canary" \
    --image="us-central1-docker.pkg.dev/sentryio/relay/relay-pop:${GO_REVISION_RELAY_REPO}" \
    --container-name="relay"
