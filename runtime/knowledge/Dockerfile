ARG BASE_IMAGE=naim/base-runtime:dev
FROM ${BASE_IMAGE}

SHELL ["/bin/bash", "-o", "pipefail", "-c"]

RUN apt-get update \
    && DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
      bash \
    && rm -rf /var/lib/apt/lists/*

COPY build/linux/x64/naim-knowledged /runtime/bin/naim-knowledged

RUN chmod +x /runtime/bin/naim-knowledged \
    && mkdir -p /naim/knowledge

WORKDIR /runtime

EXPOSE 18200

ENTRYPOINT ["/runtime/bin/naim-knowledged"]
