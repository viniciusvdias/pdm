FROM alpine:latest

WORKDIR /app

CMD ["./vpn-and-ssh-to-host-docker.sh"]

RUN apk add --no-cache \
    openvpn \
    openssh \
    bash

# Setup vpnc service
COPY bin/vpn-and-ssh-to-host-docker.sh .
RUN chmod 777 vpn-and-ssh-to-host-docker.sh
COPY ovpns/pdm-vpn.ovpn .

# Clean up to reduce image size
RUN rm -rf /tmp/* /var/tmp/* /var/cache/apk/*
