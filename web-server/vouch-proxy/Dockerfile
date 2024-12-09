FROM quay.io/vouch/vouch-proxy:alpine-latest

USER root

RUN apk update && apk upgrade && \
    apk --no-cache add python3 py3-pip

# Create a virtual environment and install awscli within it
RUN python3 -m venv /opt/venv && \
    /opt/venv/bin/pip install --no-cache-dir awscli

COPY --chown=1000 init.sh /init.sh

USER 1000

RUN chmod +x /init.sh

ENTRYPOINT ["/init.sh"]