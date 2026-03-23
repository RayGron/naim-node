FROM comet/base-runtime:dev

SHELL ["/bin/bash", "-o", "pipefail", "-c"]

RUN apt-get update \
    && DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
      libgomp1 \
    && rm -rf /var/lib/apt/lists/*

COPY runtime/worker /runtime/worker
COPY build/linux/x64/comet-workerd /runtime/bin/comet-workerd

RUN chmod +x /runtime/worker/entrypoint.sh /runtime/bin/comet-workerd \
    && ldconfig

WORKDIR /runtime
