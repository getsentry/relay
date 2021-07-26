# syntax=docker/dockerfile:1
FROM relay-builder AS relay-builder
FROM debian:buster-20210721-slim

RUN apt-get update \
    && apt-get install -y ca-certificates gosu curl --no-install-recommends \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

ENV \
    RELAY_UID=10001 \
    RELAY_GID=10001

# Create a new user and group with fixed uid/gid
RUN groupadd --system relay --gid $RELAY_GID \
    && useradd --system --gid relay --uid $RELAY_UID relay

RUN mkdir /work /etc/relay \
    && chown relay:relay /work /etc/relay
VOLUME ["/work", "/etc/relay"]
WORKDIR /work

EXPOSE 3000

COPY --from=relay-builder /bin/relay /bin/relay
COPY --from=relay-builder /opt/relay-debug.zip /opt/relay.src.zip /opt/

COPY ./docker-entrypoint.sh /
ENTRYPOINT ["/bin/bash", "/docker-entrypoint.sh"]
CMD ["run"]
