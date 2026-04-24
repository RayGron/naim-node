FROM ubuntu:24.04

SHELL ["/bin/bash", "-o", "pipefail", "-c"]

RUN apt-get update \
    && DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
      bash \
      ca-certificates \
      libssl3t64 \
      tini \
    && rm -rf /var/lib/apt/lists/*

COPY build/linux/x64/naim-controller /runtime/bin/naim-controller

RUN chmod +x /runtime/bin/naim-controller \
    && mkdir -p /naim/state /naim/artifacts

WORKDIR /naim

EXPOSE 18080 18082

ENTRYPOINT ["/usr/bin/tini", "--", "/runtime/bin/naim-controller"]
CMD ["serve", "--db", "/naim/state/controller.sqlite", "--artifacts-root", "/naim/artifacts", "--listen-host", "0.0.0.0", "--listen-port", "18080"]
