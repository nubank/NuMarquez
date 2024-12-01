# Use the official NGINX base image
FROM nginx:1.27.3

# Install OpenSSL and required packages, including aws-cli and PyYAML
RUN apk add --no-cache openssl py3-pip python3

# Copy your custom NGINX configuration
COPY nginx.conf /etc/nginx/nginx.conf

# Expose ports (optional)
EXPOSE 80 443

# Start NGINX
CMD ["nginx", "-g", "daemon off;"]