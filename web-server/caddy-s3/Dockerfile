FROM caddy:2.4.3-builder AS builder

RUN xcaddy build \
    --with github.com/lindenlab/caddy-s3-proxy

FROM caddy:2.4.3

COPY Caddyfile /etc/caddy/Caddyfile

COPY --from=builder /usr/bin/caddy /usr/bin/caddy