#!/usr/bin/env bash

DIR=$(dirname "$(realpath $0)")
(docker start "almondcli-$USER" && sleep 5 && docker logs "almondcli-$USER") || (docker run \
  -d \
  -p 4040:4040 \
  -p 8888:8888 \
  -v $DIR/../../hostdir:/app/hostdir \
  --name "almondcli-$USER" \
  --network pdmnet \
  almondcli \
  jupyter lab --no-browser --ip=0.0.0.0 --allow-root && sleep 5 && docker logs "almondcli-$USER")
