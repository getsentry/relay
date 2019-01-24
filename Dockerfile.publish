### Copy the semaphore binary and debug symbols to a clean image.
# Requrements:
# * The following files should exist in the build directory:
#   - semaphore             # semaphore binary
#   - semaphore-debug.zip   # zipped debug symbols

FROM debian:stretch-slim

RUN apt-get update \
  && apt-get install -y ca-certificates gosu --no-install-recommends \
  && apt-get clean \
  && rm -rf /var/lib/apt/lists/*

ENV \
  SEMAPHORE_UID=10001 \
  SEMAPHORE_GID=10001

# Create a new user and group with fixed uid/gid
RUN groupadd --system semaphore --gid $SEMAPHORE_GID \
  && useradd --system --gid semaphore --uid $SEMAPHORE_UID semaphore

RUN mkdir /work /etc/semaphore \
  && chown semaphore:semaphore /work /etc/semaphore
VOLUME ["/work", "/etc/semaphore"]
WORKDIR /work

EXPOSE 3000

COPY ./docker-entrypoint.sh /
ENTRYPOINT ["/bin/bash", "/docker-entrypoint.sh"]

# Do this as the last steps to use cache as much as possible
COPY semaphore /bin/semaphore
COPY semaphore-debug.zip /opt/semaphore-debug.zip
