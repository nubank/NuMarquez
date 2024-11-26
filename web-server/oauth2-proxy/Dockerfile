# Dockerfile for OAuth2 Proxy
FROM quay.io/oauth2-proxy/oauth2-proxy:v7.2.1

# Expose the port
EXPOSE 4180

# Start OAuth2 Proxy with arguments
CMD ["oauth2-proxy", "--redirect-url=https://staging-marquez-web.nubank.world/oauth2/callback", "--upstream=http://marquez-web-ui:80", "--ping-path=/ping", "--skip-auth-regex=^/ping$", "--path-prefix=/oauth2", "--reverse-proxy"]