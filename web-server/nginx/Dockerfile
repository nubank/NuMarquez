# Use the official NGINX base image
FROM 193814090748.dkr.ecr.us-east-1.amazonaws.com/mirror/nginx

# Remove the default NGINX configuration and update and install required packages, including aws-cli and PyYAML
RUN apt-get update && apt-get install -y python3-pip && \
        pip3 install --no-cache-dir awscli==1.29.3 && \
        apt-get clean && rm -rf /var/lib/apt/lists/*

# Copy your custom NGINX configuration
COPY nginx.conf /etc/nginx/nginx.conf

# Expose ports (optional)
EXPOSE 80 443

# Start NGINX
CMD ["nginx", "-g", "daemon off;"]