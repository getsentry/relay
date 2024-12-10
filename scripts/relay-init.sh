#!/usr/bin/env bash

set -euo pipefail

CONFIG_DIR="config"

# Ensure the config directory is cleaned up on exit
trap "rm -rf ${CONFIG_DIR}" EXIT

# Prompt the user for the Sentry upstream URL
read -p "Enter your Sentry upstream URL (e.g. https://oXXXX.ingest.sentry.io): " SENTRY_UPSTREAM

# Create a configuration directory for relay if it doesn't exist
mkdir -p "${CONFIG_DIR}"

echo "Initializing Relay configuration..."
# Run relay config init to create config files (.relay/config.yml and credentials.json)
# We assume you'll choose the default configuration by pressing enter.
docker run --rm -it \
  -v "$(pwd)/${CONFIG_DIR}":/work/.relay \
  getsentry/relay \
  config init

# At this point, config.yml and credentials.json should be in ${CONFIG_DIR}
CREDENTIALS_FILE="${CONFIG_DIR}/credentials.json"

if [ ! -f "${CREDENTIALS_FILE}" ]; then
  echo "Error: ${CREDENTIALS_FILE} not found. Make sure relay config init completed successfully."
  exit 1
fi

echo "Reading credentials from ${CREDENTIALS_FILE}..."
# Parse the credentials file directly with jq
SECRET_KEY=$(jq -r '.secret_key' "${CREDENTIALS_FILE}")
PUBLIC_KEY=$(jq -r '.public_key' "${CREDENTIALS_FILE}")
RELAY_ID=$(jq -r '.id' "${CREDENTIALS_FILE}")

echo "Credentials and upstream URL obtained successfully."

cat <<EOF

Setup complete!

Your relay chart can now be installed with the following command:
    helm install sentry-relay ../sentry-relay \\
      --set relay.upstream="${SENTRY_UPSTREAM}" \\
      --set credentials.secretKey="${SECRET_KEY}" \\
      --set credentials.publicKey="${PUBLIC_KEY}" \\
      --set credentials.relayId="${RELAY_ID}"

Once deployed, you can port-forward and test sending events as noted in the chart's NOTES.txt.
EOF