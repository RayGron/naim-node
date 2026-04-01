FROM caddy:2.10.2-alpine

COPY runtime/web-ui/Caddyfile /etc/caddy/Caddyfile
COPY ui/operator-react/dist/ /srv/

EXPOSE 8080
