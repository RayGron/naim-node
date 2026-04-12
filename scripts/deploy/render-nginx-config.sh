#!/usr/bin/env bash
set -euo pipefail

script_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
source "${script_dir}/naim-production-env.sh"

inventory_path=""
domain=""
ssl_certificate=""
ssl_certificate_key=""
declare -a worker_allow_ips=()

usage() {
  cat <<'EOF'
Usage:
  render-nginx-config.sh [--inventory <path>] --domain <name>
                         [--ssl-certificate <path>]
                         [--ssl-certificate-key <path>]
                         [--worker-allow-ip <ip>] ...

Prints an nginx config for the NAIM Web UI and restricted hostd-facing
controller endpoint. It does not install or reload nginx.
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --inventory)
      inventory_path="${2:-}"
      if [[ -z "${inventory_path}" ]]; then
        echo "error: --inventory requires a path" >&2
        exit 1
      fi
      shift 2
      ;;
    --domain)
      domain="${2:-}"
      if [[ -z "${domain}" ]]; then
        echo "error: --domain requires a name" >&2
        exit 1
      fi
      shift 2
      ;;
    --ssl-certificate)
      ssl_certificate="${2:-}"
      shift 2
      ;;
    --ssl-certificate-key)
      ssl_certificate_key="${2:-}"
      shift 2
      ;;
    --worker-allow-ip)
      next_value="${2:-}"
      if [[ -z "${next_value}" ]]; then
        echo "error: --worker-allow-ip requires an IP address" >&2
        exit 1
      fi
      worker_allow_ips+=("${next_value}")
      shift 2
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "error: unknown argument '$1'" >&2
      usage >&2
      exit 1
      ;;
  esac
done

if [[ -n "${inventory_path}" ]]; then
  eval "$(naim_load_inventory_env "${inventory_path}")"
fi

if [[ -z "${domain}" ]]; then
  domain="${NAIM_CONTROL_DOMAIN:-}"
fi
if [[ -z "${domain}" ]]; then
  echo "error: --domain is required when inventory control.domain is empty" >&2
  exit 1
fi

if [[ -z "${ssl_certificate}" ]]; then
  ssl_certificate="/etc/letsencrypt/live/${domain}/fullchain.pem"
fi
if [[ -z "${ssl_certificate_key}" ]]; then
  ssl_certificate_key="/etc/letsencrypt/live/${domain}/privkey.pem"
fi

cat <<NGINX
server {
    listen 443 ssl http2;
    server_name ${domain};

    ssl_certificate ${ssl_certificate};
    ssl_certificate_key ${ssl_certificate_key};

    location / {
        proxy_pass http://127.0.0.1:${NAIM_MAIN_WEB_UI_LOCAL_PORT};
        proxy_http_version 1.1;
        proxy_set_header Host \$host;
        proxy_set_header X-Forwarded-For \$proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto \$scheme;
    }
}

server {
    listen ${NAIM_MAIN_HOSTD_PUBLIC_PORT};
    server_name _;

NGINX

if [[ ${#worker_allow_ips[@]} -eq 0 ]]; then
  cat <<'NGINX'
    # Add one allow line per worker host IP before installing this config.
    # allow 203.0.113.10;
NGINX
else
  for ip in "${worker_allow_ips[@]}"; do
    printf '    allow %s;\n' "${ip}"
  done
fi

cat <<NGINX
    deny all;

    location / {
        proxy_pass http://127.0.0.1:${NAIM_MAIN_CONTROLLER_LOCAL_PORT};
        proxy_http_version 1.1;
        proxy_set_header Host \$host;
        proxy_set_header X-Forwarded-For \$proxy_add_x_forwarded_for;
    }
}
NGINX
