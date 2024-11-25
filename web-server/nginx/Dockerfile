# Use the official NGINX base image
FROM nginx:1.27.2-alpine

# Set the working directory (optional)
WORKDIR /usr/src/app

# Remove the default NGINX configuration
RUN rm /etc/nginx/conf.d/default.conf

RUN apt-get update && \
    apt-get install -y python3-pip && \
    apt-get install -y python3-yaml

RUN pip3 install --no-cache-dir PyYAML
RUN pip3 install --no-cache-dir awscli

# Copy your custom NGINX configuration
COPY nginx.conf /etc/nginx/nginx.conf

# Expose ports (optional)
EXPOSE 80 443

COPY init.sh /init.sh

ENTRYPOINT ["/init.sh"]

CMD ["nginx", "-g", "daemon off;"]