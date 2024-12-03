FROM 193814090748.dkr.ecr.us-east-1.amazonaws.com/mirror/nginx

RUN apt-get update && apt-get install -y python3-pip

# awscli latest version has a bug that prevents it from working with IAM roles
# until fixed, use a specific version
RUN pip3 install --no-cache-dir awscli==1.29.3

COPY nginx.conf.template /templates/nginx.conf.template

COPY init.sh /init.sh

RUN chmod +x /init.sh

ENTRYPOINT ["/init.sh"]

CMD ["nginx", "-g", "daemon off;"]