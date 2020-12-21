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

# For compatibility with older images
if [ "$1" == "bash" ]; then
  set -- bash "${@:2}"
elif [ "$(id -u)" == "0" ]; then
  set -- gosu relay /bin/relay "$@"
else
  set -- /bin/relay "$@"
fi

exec "$@"
