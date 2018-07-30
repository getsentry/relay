#!/usr/bin/env bash
set -e

# Fix permissions
chmod 755 /bin/semaphore
chown -R semaphore:semaphore /etc/semaphore /work

exec gosu semaphore /bin/semaphore "$@"
