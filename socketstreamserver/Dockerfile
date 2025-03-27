FROM alpine:latest
WORKDIR /app
RUN apk add --update --no-cache bash ucspi-tcp6 words-en
COPY cmd.sh /bin/cmd.sh
COPY tcpserver.sh /bin/tcpserver.sh
RUN chmod +x /bin/cmd.sh
RUN chmod +x /bin/tcpserver.sh
CMD ["/bin/cmd.sh"]
