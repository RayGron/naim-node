FROM nvidia/cuda:13.0.1-devel-ubuntu24.04 AS workerd-builder

SHELL ["/bin/bash", "-o", "pipefail", "-c"]

RUN apt-get update \
    && DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
      build-essential \
      ca-certificates \
      cmake \
      curl \
      git \
      ninja-build \
      pkg-config \
      unzip \
      zip \
    && rm -rf /var/lib/apt/lists/*

RUN git clone https://github.com/microsoft/vcpkg.git /opt/vcpkg \
    && /opt/vcpkg/bootstrap-vcpkg.sh -disableMetrics

WORKDIR /src

ENV VCPKG_MAX_CONCURRENCY=6
ENV CMAKE_BUILD_PARALLEL_LEVEL=6

COPY CMakeLists.txt /src/CMakeLists.txt
COPY vcpkg.json /src/vcpkg.json
COPY common /src/common
COPY runtime /src/runtime
COPY controller /src/controller
COPY hostd /src/hostd

RUN /opt/vcpkg/vcpkg install \
      --x-manifest-root=/src \
      --x-install-root=/src/vcpkg_installed \
      --triplet=x64-linux \
      --x-wait-for-lock

RUN cmake -S /src -B /tmp/comet-build -G Ninja \
      -DVCPKG_TARGET_TRIPLET=x64-linux \
      -DVCPKG_INSTALLED_DIR=/src/vcpkg_installed \
      -DCMAKE_PREFIX_PATH=/src/vcpkg_installed/x64-linux \
      -DCOMET_SKIP_VCPKG_TOOLCHAIN=ON \
    && cmake --build /tmp/comet-build --target comet-workerd

FROM comet/base-runtime:dev

SHELL ["/bin/bash", "-o", "pipefail", "-c"]

RUN apt-get update \
    && DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
      libgomp1 \
    && rm -rf /var/lib/apt/lists/*

COPY runtime/worker /runtime/worker
COPY --from=workerd-builder /tmp/comet-build/comet-workerd /runtime/bin/comet-workerd
COPY --from=workerd-builder /src/vcpkg_installed/x64-linux/lib/ /usr/local/lib/

RUN chmod +x /runtime/worker/entrypoint.sh /runtime/bin/comet-workerd \
    && ldconfig

WORKDIR /runtime
