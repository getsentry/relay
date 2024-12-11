#!/usr/bin/env bash

set -euo pipefail

# Usage: ./setup_relay.sh <DSN> <ORG_SLUG> [--open] [--config-path <path>] [--port <port>] [--host <host>]
if [[ $# -lt 2 ]]; then
  echo "Usage: $0 <DSN> <ORG_SLUG> [--open] [--config-path <path>] [--port <port>] [--host <host>]"
  exit 1
fi

DSN="$1"
ORG_SLUG="$2"
OPEN_PAGE="false"
CONFIG_PATH="$(pwd)/config"
RELAY_PORT="3000"
RELAY_HOST="0.0.0.0"

# Parse optional arguments
shift 2
while [[ $# -gt 0 ]]; do
  case $1 in
    --open)
      OPEN_PAGE="true"
      shift
      ;;
    --config-path)
      CONFIG_PATH="$2"
      shift 2
      ;;
    --port)
      RELAY_PORT="$2"
      shift 2
      ;;
    --host)
      RELAY_HOST="$2"
      shift 2
      ;;
    *)
      echo "Unknown option: $1"
      exit 1
      ;;
  esac
done

# Extract the host part from the DSN
UPSTREAM_HOST=$(echo "$DSN" | sed -E 's@^https://([^/]+).*@\1@')
UPSTREAM="https://$UPSTREAM_HOST"

if [[ -z "$UPSTREAM_HOST" ]]; then
  echo "Failed to parse upstream from DSN. Check the DSN format."
  exit 1
fi

# Create the config directory
mkdir -p "$CONFIG_PATH"

echo -e "\n=== Initializing Relay ===\n"
echo "Initializing Relay configuration in $CONFIG_PATH..."
docker run --rm -it \
  -v "$CONFIG_PATH/:/work/.relay/:z" \
  getsentry/relay \
  config init

# The default config is now in $CONFIG_PATH/config.yml
echo -e "\n=== Configuring Relay ===\n"
echo "Updating configuration settings..."
sed -i '' "s|mode:.*|mode: managed|" "$CONFIG_PATH/config.yml"
sed -i '' "s|upstream:.*|upstream: $UPSTREAM|" "$CONFIG_PATH/config.yml"
sed -i '' "s|host:.*|host: $RELAY_HOST|" "$CONFIG_PATH/config.yml"
sed -i '' "s|port:.*|port: $RELAY_PORT|" "$CONFIG_PATH/config.yml"

echo -e "\n=== Retrieving Credentials ===\n"
echo "Fetching Relay public key..."
PUBLIC_KEY=$(docker run --rm -it \
  -v "$CONFIG_PATH/:/work/.relay/" \
  getsentry/relay \
  credentials show | grep 'public key' | awk '{print $3}')

if [[ -z "$PUBLIC_KEY" ]]; then
  echo "❌ Failed to retrieve Relay public key."
  exit 1
fi

echo "✅ Public key retrieved successfully: $PUBLIC_KEY"

# Optionally open the Relay registration page in the browser
RELAY_URL="https://${ORG_SLUG}.sentry.io/settings/relay"
echo -e "\n=== Registration Information ===\n"
if [[ "$OPEN_PAGE" == "true" ]]; then
  echo "Opening Relay registration page: $RELAY_URL"
  if command -v xdg-open &>/dev/null; then
    xdg-open "$RELAY_URL"
  elif command -v open &>/dev/null; then
    open "$RELAY_URL"
  else
    echo "📝 Please open the following URL in your browser to register the Relay:"
    echo "$RELAY_URL"
  fi
else
  echo "📝 To register this Relay, visit:"
  echo "$RELAY_URL"
  echo "and add the public key: $PUBLIC_KEY"
fi

echo -e "\n=== Starting Relay ===\n"
echo "Launching Relay container on port $RELAY_PORT..."
docker run -d \
  -v "$CONFIG_PATH/:/work/.relay/" \
  -p "$RELAY_PORT:$RELAY_PORT" \
  getsentry/relay \
  run

echo -e "\n=== Setup Complete ===\n"
echo "✅ Relay is now running on http://$RELAY_HOST:$RELAY_PORT"
echo "📋 Configuration Summary:"
echo "------------------------"
echo "DSN: $DSN"
echo "Upstream: $UPSTREAM"
echo "Public Key: $PUBLIC_KEY"
echo "Local Host: $RELAY_HOST"
echo "Local Port: $RELAY_PORT"
echo -e "\n📌 Next Steps:"
echo "------------------------"
echo "To send events through Relay, modify your DSN to point to http://$RELAY_HOST:$RELAY_PORT"
echo "Example: If your original DSN was: $DSN"
echo "Replace the host part with $RELAY_HOST:$RELAY_PORT (use http, not https)"
