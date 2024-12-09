FROM quay.io/vouch/vouch-proxy:alpine-latest

USER root

RUN apk update && apk upgrade && \
    apk --no-cache add python3 py3-pip awscli==1.29.3

COPY --chown=1000 init.sh /init.sh

USER 1000

RUN chmod +x /init.sh

ENTRYPOINT ["/init.sh"]