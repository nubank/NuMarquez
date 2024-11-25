# Use the official NGINX base image
FROM nginx:1.27.2-alpine

# Set the working directory (optional)
WORKDIR /usr/src/app

# Remove the default NGINX configuration
RUN rm -f /etc/nginx/conf.d/default.conf

# Update and install required packages
RUN apk update && \
    apk add --no-cache python3 py3-pip

# Install Python packages
RUN pip3 install --no-cache-dir PyYAML awscli

# Copy your custom NGINX configuration
COPY nginx.conf /etc/nginx/nginx.conf

# Make sure init.sh is executable
COPY init.sh /init.sh
RUN chmod +x /init.sh

# Expose ports (optional)
EXPOSE 80 443

# Set the entrypoint
ENTRYPOINT ["/init.sh"]

# Start NGINX
CMD ["nginx", "-g", "daemon off;"]