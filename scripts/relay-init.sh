#!/usr/bin/env bash

set -euo pipefail

# Usage: ./setup_relay.sh <DSN> <ORG_SLUG> [--open]
if [[ $# -lt 2 ]]; then
  echo "Usage: $0 <DSN> <ORG_SLUG> [--open]"
  exit 1
fi

DSN="$1"
ORG_SLUG="$2"
OPEN_PAGE="false"

if [[ "${3:-}" == "--open" ]]; then
  OPEN_PAGE="true"
fi

# Extract the host part from the DSN
# DSN format could be: https://o[org_id].ingest.us.sentry.io/[project_id]
# or even just https://o[org_id].ingest.us.sentry.io
# We'll strip the protocol and any trailing path:
UPSTREAM_HOST=$(echo "$DSN" | sed -E 's@^https://([^/]+).*@\1@')
UPSTREAM="https://$UPSTREAM_HOST"

if [[ -z "$UPSTREAM_HOST" ]]; then
  echo "Failed to parse upstream from DSN. Check the DSN format."
  exit 1
fi

# Create the config directory
mkdir -p config

echo "Initializing Relay configuration..."
docker run --rm -it \
  -v "$(pwd)/config/:/work/.relay/:z" \
  getsentry/relay \
  config init <<EOF
1
EOF

# The default config is now in ./config/.relay/config.yml
# We'll edit it to set mode=managed, update the upstream, and ensure host/port
echo "Configuring Relay..."
sed -i 's/mode: .*/mode: managed/' config/.relay/config.yml
sed -i "s|upstream:.*|upstream: $UPSTREAM|" config/.relay/config.yml
sed -i 's|host:.*|host: 0.0.0.0|' config/.relay/config.yml
sed -i 's|port:.*|port: 3000|' config/.relay/config.yml

echo "Retrieving Relay credentials (public key)..."
PUBLIC_KEY=$(docker run --rm -it \
  -v "$(pwd)/config/:/work/.relay/" \
  getsentry/relay \
  credentials show | grep 'public key' | awk '{print $3}')

if [[ -z "$PUBLIC_KEY" ]]; then
  echo "Failed to retrieve Relay public key."
  exit 1
fi

echo "Relay public key: $PUBLIC_KEY"

# Optionally open the Relay registration page in the browser
RELAY_URL="https://${ORG_SLUG}.sentry.io/settings/relay"
if [[ "$OPEN_PAGE" == "true" ]]; then
  echo "Opening Relay registration page: $RELAY_URL"
  if command -v xdg-open &>/dev/null; then
    xdg-open "$RELAY_URL"
  elif command -v open &>/dev/null; then
    open "$RELAY_URL"
  else
    echo "Please open the following URL in your browser to register the Relay:"
    echo "$RELAY_URL"
  fi
else
  echo "To register this Relay, visit:"
  echo "$RELAY_URL"
  echo "and add the public key: $PUBLIC_KEY"
fi

echo "Starting Relay on port 3000..."
docker run -d \
  -v "$(pwd)/config/:/work/.relay/" \
  -p 3000:3000 \
  getsentry/relay \
  run

echo "Relay is now running on http://localhost:3000"
echo "Using DSN: $DSN"
echo "Upstream is set to: $UPSTREAM"
echo "Relay public key: $PUBLIC_KEY"
echo
echo "To send events through Relay, modify your DSN to point to http://localhost:3000."
echo "For example, if your original DSN was: $DSN"
echo "Replace the host part with localhost:3000 (use http, not https)."