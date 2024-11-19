# Dockerfile for OAuth2 Proxy
FROM quay.io/oauth2-proxy/oauth2-proxy:v7.2.0

# Optionally, set environment variables directly
ENV OAUTH2_PROXY_PROVIDER=oidc
ENV OAUTH2_PROXY_SCOPE="openid profile email groups"
ENV OAUTH2_PROXY_PROVIDER_DISPLAY_NAME=Okta
ENV OAUTH2_PROXY_SKIP_PROVIDER_BUTTON=true
ENV OAUTH2_PROXY_EMAIL_DOMAINS=*
ENV OAUTH2_PROXY_COOKIE_SECURE=true
ENV OAUTH2_PROXY_COOKIE_HTTPONLY=true
ENV OAUTH2_PROXY_COOKIE_SAMESITE=lax
ENV OAUTH2_PROXY_PASS_USER_HEADERS=true
ENV OAUTH2_PROXY_REVERSE_PROXY=true
ENV OAUTH2_PROXY_COOKIE_NAME=SESSION
ENV OAUTH2_PROXY_SKIP_AUTH_REGEX=^/favicon\.ico$

# Expose the port
EXPOSE 4180

# Start OAuth2 Proxy with arguments
CMD ["oauth2-proxy", "--redirect-url=https://staging-marquez-web.nubank.world/oauth2/callback", "--upstream=http://marquez-web-ui:3000", "--ping-path=/ping", "--skip-auth-regex=^/ping$", "--path-prefix=/oauth2"]