#!/usr/bin/env bash
set -e

# Enable core dumps. Requires privileged mode.
if [[ "${RELAY_ENABLE_COREDUMPS:-}" == "1" ]]; then
  mkdir -p /var/dumps
  chmod a+rwx /var/dumps
  echo '/var/dumps/core.%h.%e.%t' > /proc/sys/kernel/core_pattern
  ulimit -c unlimited
fi

# For compatibility with older images
if [ "$1" == "bash" ]; then
  set -- bash
elif [ "$(id -u)" == "0" ]; then
  set -- gosu semaphore /bin/semaphore "$@"
else
  set -- /bin/semaphore "$@"
fi

exec "$@"
