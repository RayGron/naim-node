FROM python:3.12-slim

SHELL ["/bin/bash", "-o", "pipefail", "-c"]

RUN apt-get update \
    && DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
      bash \
      ca-certificates \
      curl \
      tini \
    && rm -rf /var/lib/apt/lists/*

COPY runtime/skills /runtime/skills

RUN chmod +x /runtime/skills/entrypoint.sh

WORKDIR /runtime
ENTRYPOINT ["/usr/bin/tini", "--"]

