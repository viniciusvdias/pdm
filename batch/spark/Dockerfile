FROM alpine:latest
WORKDIR /app
ENV PYTHONUNBUFFERED=1
RUN apk add --update --no-cache bash build-base python3 python3-dev musl-dev linux-headers py3-pip && ln -sf python3 /usr/bin/python
RUN python3 -m venv .venv
RUN source .venv/bin/activate && python3 -m pip install jupyterlab
COPY entrypoint.sh /bin/entrypoint.sh
RUN chmod +x /bin/entrypoint.sh
EXPOSE 8888
ENTRYPOINT ["/bin/entrypoint.sh"]
