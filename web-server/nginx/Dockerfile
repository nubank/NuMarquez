FROM 193814090748.dkr.ecr.us-east-1.amazonaws.com/mirror/nginx

RUN apt-get update && \
    apt-get install -y python3-pip && \
    apt-get install -y python3-yaml && \
    apt-get install -y curl && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Copy your custom NGINX configuration
COPY nginx.conf /etc/nginx/nginx.conf

# Expose ports (optional)
EXPOSE 80 443

# Start NGINX
CMD ["nginx", "-g", "daemon off;"]