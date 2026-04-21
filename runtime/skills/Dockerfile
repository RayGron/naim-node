ARG BASE_IMAGE=naim/base-runtime:dev
FROM ${BASE_IMAGE}

SHELL ["/bin/bash", "-o", "pipefail", "-c"]

RUN apt-get update \
    && DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
      bash \
    && rm -rf /var/lib/apt/lists/*

COPY runtime/skills /runtime/skills
COPY build/linux/x64/naim-skillsd /runtime/bin/naim-skillsd

RUN chmod +x /runtime/skills/entrypoint.sh /runtime/bin/naim-skillsd

WORKDIR /runtime
