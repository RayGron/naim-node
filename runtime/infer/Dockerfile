FROM comet/base-runtime:dev

SHELL ["/bin/bash", "-o", "pipefail", "-c"]

RUN apt-get update \
    && DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
      libgomp1 \
    && rm -rf /var/lib/apt/lists/*

COPY runtime/infer /runtime/infer
COPY build/linux/x64/comet-inferctl /runtime/bin/comet-inferctl

RUN chmod +x /runtime/infer/entrypoint.sh /runtime/infer/inferctl.sh /runtime/bin/comet-inferctl \
    && ldconfig

WORKDIR /runtime
