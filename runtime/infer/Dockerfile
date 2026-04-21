ARG BASE_IMAGE=naim/base-runtime:dev
FROM ${BASE_IMAGE}

SHELL ["/bin/bash", "-o", "pipefail", "-c"]

RUN apt-get update \
    && DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
      libgomp1 \
    && rm -rf /var/lib/apt/lists/*

COPY runtime/infer /runtime/infer
COPY build/linux/x64/naim-inferctl /runtime/bin/naim-inferctl
COPY build/linux/x64/bin/llama-server /runtime/bin/llama-server

RUN chmod +x /runtime/infer/entrypoint.sh /runtime/infer/inferctl.sh /runtime/bin/naim-inferctl /runtime/bin/llama-server \
    && ldconfig

WORKDIR /runtime
