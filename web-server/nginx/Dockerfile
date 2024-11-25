# Use the official NGINX base image
FROM nginx:1.27.2-alpine

# Remove the default NGINX configuration and update and install required packages, including aws-cli and PyYAML
RUN rm -f /etc/nginx/conf.d/default.conf && \
    apk update && \
    apk add --no-cache bash openssl py3-pip py3-yaml python3

# Copy your custom NGINX configuration
COPY nginx.conf /etc/nginx/nginx.conf

# Expose ports (optional)
EXPOSE 80

# Start NGINX
CMD ["nginx", "-g", "daemon off;"]