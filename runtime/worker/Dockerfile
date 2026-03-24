ARG VLLM_BASE_IMAGE=vllm/vllm-openai:latest
FROM ${VLLM_BASE_IMAGE}

SHELL ["/bin/bash", "-o", "pipefail", "-c"]

COPY runtime/worker /runtime/worker
COPY build/linux/x64/comet-workerd /runtime/bin/comet-workerd

RUN chmod +x /runtime/worker/entrypoint.sh /runtime/bin/comet-workerd \
    && ldconfig

ENTRYPOINT []

WORKDIR /runtime
