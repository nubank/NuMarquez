#!/bin/sh

set -e

export OAUTH_CLIENT_ID=$(aws s3 cp ${OAUTH_CLIENT_ID_S3_LOCATION} -)
export OAUTH_CLIENT_SECRET=$(aws s3 cp ${OAUTH_CLIENT_SECRET_S3_LOCATION} -)

/vouch-proxy