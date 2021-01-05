#!/usr/bin/env bash
set -e

# Enable core dumps. Requires privileged mode.
if [[ "${RELAY_ENABLE_COREDUMPS:-}" == "1" ]]; then
  mkdir -p /var/dumps
  chmod a+rwx /var/dumps
  echo '/var/dumps/core.%h.%e.%t' > /proc/sys/kernel/core_pattern
  ulimit -c unlimited
fi

# Sleep for the specified number of seconds before starting.
# For example, can be helpful to synchronize container startup in Kubernetes environment.
if [[ -n "${RELAY_DELAY_STARTUP_SECONDS:-}" ]]; then
  echo "Sleeping for ${RELAY_DELAY_STARTUP_SECONDS}s..."
  sleep "${RELAY_DELAY_STARTUP_SECONDS}"
fi

# Make sure that a specified URL (e.g. the upstream or a proxy sidecar) is reachable before starting.
# Only 200 response is accepted as success.
if [[ -n "${RELAY_PRESTART_ENDPOINT:-}" ]]; then
  max_retry="${RELAY_PRESTART_MAX_RETRIES:-120}"
  for attempt in $(seq 1 "$max_retry"); do
    status=$(curl --max-time 1 --silent --output /dev/null \
                  --write-out "%{http_code}" \
                  -H 'Connection: close' \
                  "${RELAY_PRESTART_ENDPOINT}" \
              || true)
    if [[ "$status" == "200" ]]; then
      break
    fi
    echo "Waiting for a 200 response from ${RELAY_PRESTART_ENDPOINT}"
    sleep 1
  done
  if [[ "$attempt" == "$max_retry" ]]; then
    echo "The prestart endpoint has not returned 200 for $max_retry seconds, exiting!"
    exit 1
  fi
fi

# For compatibility with older images
if [ "$1" == "bash" ]; then
  set -- bash "${@:2}"
elif [ "$(id -u)" == "0" ]; then
  set -- gosu relay /bin/relay "$@"
else
  set -- /bin/relay "$@"
fi

exec "$@"
