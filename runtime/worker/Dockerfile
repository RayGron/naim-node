ARG BASE_IMAGE=naim/base-runtime:dev
FROM ${BASE_IMAGE}

SHELL ["/bin/bash", "-o", "pipefail", "-c"]

RUN apt-get update \
    && DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
      libgomp1 \
    && rm -rf /var/lib/apt/lists/* \
    && mkdir -p /runtime/bin/turboquant

COPY runtime/worker /runtime/worker
COPY build/linux/x64/naim-workerd /runtime/bin/naim-workerd
COPY build/linux/x64/bin/rpc-server /runtime/bin/rpc-server
COPY build-turboquant/linux/x64/bin/rpc-server /runtime/bin/turboquant/rpc-server

RUN chmod +x /runtime/worker/entrypoint.sh /runtime/bin/naim-workerd /runtime/bin/rpc-server /runtime/bin/turboquant/rpc-server \
    && ldconfig

WORKDIR /runtime
