#!/bin/sh

set -eu

mkdir -p /etc/nginx/certs
aws s3 cp --region us-west-2 s3://nu-keysets-data-staging/certificates/pri/global/keystore.p12-2022-2023 /etc/nginx/certs
cd /etc/nginx/certs
openssl pkcs12 -in keystore.p12 -passin pass:nubankp12 -nocerts -nodes | sed -n '/BEGIN/,$p' > nubank_key.pem
openssl pkcs12 -in keystore.p12 -passin pass:nubankp12 -nokeys > nubank_cert.pem

rm /etc/nginx/certs/keystore.p12
chmod 400 /etc/nginx/certs/nubank_key.pem

envsubst '${DOMAIN} ${ES_INDEX} ${REDIRECTS}' < /templates/nginx.conf.template > /etc/nginx/nginx.conf

echo "Starting nginx"

exec "$@"