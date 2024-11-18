# Use the official NGINX base image
FROM nginx:1.27.2-alpine

# Install OpenSSL
RUN apk add --no-cache openssl

# Set the working directory
WORKDIR /usr/src/app

# Copy the localhost.ext file into the container
COPY localhost.ext /etc/nginx/certs/localhost.ext

# Generate a private key
RUN openssl genrsa -out localhost.key 2048

RUN openssl req -x509 -new -nodes -key domain.key -sha256 -days 3650 -out domain.crt \
    -subj "/C=US/ST=State/L=City/O=Organization/OU=OrgUnit/CN=staging-marquez-web.nubank.world"

# Generate a CSR with the correct domain
RUN openssl req -new -key domain.key -out domain.csr \
    -subj "/C=US/ST=State/L=City/O=Organization/OU=OrgUnit/CN=staging-marquez-web.nubank.world"

# Generate the .pem certificate using the CSR and the updated extension file
RUN openssl x509 -req -in domain.csr -CA domain.crt -CAkey domain.key -CAcreateserial \
    -out domain.pem -days 365 -sha256 -extfile /etc/nginx/certs/domain.ext

# Copy the generated certificates to /etc/nginx/certs/
RUN cp domain.key /etc/nginx/certs/domain.key && \
    cp domain.crt /etc/nginx/certs/domain.crt && \
    cp domain.pem /etc/nginx/certs/domain.pem

# Remove the default NGINX configuration
RUN rm /etc/nginx/conf.d/default.conf

# Copy your custom NGINX configuration
COPY nginx.conf /etc/nginx/nginx.conf

# Expose ports (optional, adjust as needed)
EXPOSE 80 443

# Start NGINX
CMD ["nginx", "-g", "daemon off;"]