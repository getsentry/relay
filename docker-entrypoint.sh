#!/usr/bin/env bash
set -e

if [ "$(id -u)" == "0" ]; then
  exec gosu semaphore /bin/semaphore "$@"
else
  exec /bin/semaphore "$@"
fi
