#!/usr/bin/env bash
set -e

# For compatibility with older images
if [ "$1" == "bash" ]; then
  set -- bash
elif [ "$(id -u)" == "0" ]; then
  set -- gosu semaphore /bin/semaphore "$@"
else
  set -- /bin/semaphore "$@"
fi

exec "$@"
