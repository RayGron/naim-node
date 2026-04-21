FROM node:20-bookworm-slim AS node-runtime

FROM ubuntu:24.04

SHELL ["/bin/bash", "-o", "pipefail", "-c"]

RUN apt-get update \
    && DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
      bash \
      ca-certificates \
      tini \
    && rm -rf /var/lib/apt/lists/*

COPY --from=node-runtime /usr/local/bin/node /usr/local/bin/node
COPY --from=node-runtime /usr/local/lib/node_modules /usr/local/lib/node_modules
COPY build/linux/x64/naim-controller /runtime/bin/naim-controller
COPY ui/operator-react/package.json ui/operator-react/package-lock.json /runtime/ui/operator-react/
COPY ui/operator-react/scripts/webauthn-helper.mjs /runtime/ui/operator-react/scripts/webauthn-helper.mjs

RUN chmod +x /runtime/bin/naim-controller \
    && ln -sf ../lib/node_modules/npm/bin/npm-cli.js /usr/local/bin/npm \
    && ln -sf ../lib/node_modules/npm/bin/npx-cli.js /usr/local/bin/npx \
    && mkdir -p /naim/state /naim/artifacts \
    && cd /runtime/ui/operator-react \
    && npm ci --omit=dev \
    && npm cache clean --force

WORKDIR /naim

EXPOSE 18080 18082

ENTRYPOINT ["/usr/bin/tini", "--", "/runtime/bin/naim-controller"]
CMD ["serve", "--db", "/naim/state/controller.sqlite", "--artifacts-root", "/naim/artifacts", "--listen-host", "0.0.0.0", "--listen-port", "18080"]
