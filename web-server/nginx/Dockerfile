# Use the official NGINX base image
FROM nginx:1.27.2-alpine

# Remove the default NGINX configuration
RUN rm -f /etc/nginx/conf.d/default.conf

# Update and install required packages, including aws-cli and PyYAML
RUN apk update && \
    apk add --no-cache python3 py3-pip py3-yaml aws-cli openssl bash

# Copy your custom NGINX configuration
COPY nginx.conf /etc/nginx/nginx.conf

# Copy and set permissions for init.sh
COPY init.sh /init.sh
RUN chmod +x /init.sh

# Expose ports (optional)
EXPOSE 80 443

# Set the entrypoint
ENTRYPOINT ["/init.sh"]

# Start NGINX
CMD ["nginx", "-g", "daemon off;"]