FROM ubuntu:24.04

SHELL ["/bin/bash", "-o", "pipefail", "-c"]

RUN apt-get update \
    && DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
      bash \
      ca-certificates \
      curl \
      docker.io \
      iproute2 \
      tini \
    && (DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends docker-compose-v2 \
        || DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends docker-compose-plugin) \
    && rm -rf /var/lib/apt/lists/*

COPY build/linux/x64/naim-hostd /runtime/bin/naim-hostd
COPY build/linux/x64/naim-node /runtime/bin/naim-node

RUN chmod +x /runtime/bin/naim-hostd /runtime/bin/naim-node \
    && mkdir -p /naim/state /naim/runtime /naim/storage

WORKDIR /naim

ENTRYPOINT ["/usr/bin/tini", "--", "/runtime/bin/naim-node"]
CMD ["run", "hostd", "--foreground", "--skip-systemctl"]
