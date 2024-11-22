# Use the official NGINX base image
FROM nginx:1.27.2-alpine

# Set the working directory (optional)
WORKDIR /usr/src/app

# Remove the default NGINX configuration
RUN rm /etc/nginx/conf.d/default.conf

# Copy your custom NGINX configuration
COPY nginx.conf /etc/nginx/nginx.conf

# Expose ports (optional)
EXPOSE 80 443

# Start NGINX
CMD ["nginx", "-g", "daemon off;"]