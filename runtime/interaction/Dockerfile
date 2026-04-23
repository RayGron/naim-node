ARG BASE_IMAGE=naim/base-runtime:dev
FROM ${BASE_IMAGE}

SHELL ["/bin/bash", "-o", "pipefail", "-c"]

RUN apt-get update \
    && DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
      bash \
      curl \
    && rm -rf /var/lib/apt/lists/*

COPY build/linux/x64/naim-interactiond /runtime/bin/naim-interactiond

RUN chmod +x /runtime/bin/naim-interactiond

WORKDIR /runtime

EXPOSE 18110

ENTRYPOINT ["/runtime/bin/naim-interactiond"]
