FROM comet/base-runtime:dev

SHELL ["/bin/bash", "-o", "pipefail", "-c"]

RUN apt-get update \
    && DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
      bash \
      curl \
      libasound2t64 \
      libatk-bridge2.0-0 \
      libatk1.0-0 \
      libatspi2.0-0 \
      libcairo2 \
      libcups2t64 \
      libdbus-1-3 \
      libdrm2 \
      libgbm1 \
      libglib2.0-0 \
      libgtk-3-0 \
      libnspr4 \
      libnss3 \
      libpango-1.0-0 \
      libx11-6 \
      libx11-xcb1 \
      libxcb-dri3-0 \
      libxcb1 \
      libxcomposite1 \
      libxdamage1 \
      libxext6 \
      libxfixes3 \
      libxkbcommon0 \
      libxrandr2 \
      libxshmfence1 \
    && rm -rf /var/lib/apt/lists/*

COPY runtime/browsing /runtime/browsing
COPY build/linux/x64/comet-webgatewayd /runtime/bin/comet-webgatewayd
COPY build/linux/x64/libcef.so /runtime/bin/libcef.so
COPY build/linux/x64/chrome-sandbox /runtime/bin/chrome-sandbox
COPY build/linux/x64/chrome_100_percent.pak /runtime/bin/chrome_100_percent.pak
COPY build/linux/x64/chrome_200_percent.pak /runtime/bin/chrome_200_percent.pak
COPY build/linux/x64/icudtl.dat /runtime/bin/icudtl.dat
COPY build/linux/x64/libEGL.so /runtime/bin/libEGL.so
COPY build/linux/x64/libGLESv2.so /runtime/bin/libGLESv2.so
COPY build/linux/x64/resources.pak /runtime/bin/resources.pak
COPY build/linux/x64/v8_context_snapshot.bin /runtime/bin/v8_context_snapshot.bin
COPY build/linux/x64/vk_swiftshader_icd.json /runtime/bin/vk_swiftshader_icd.json
COPY build/linux/x64/locales /runtime/bin/locales

RUN chmod +x \
      /runtime/browsing/entrypoint.sh \
      /runtime/bin/comet-webgatewayd \
      /runtime/bin/chrome-sandbox

WORKDIR /runtime
