#!/usr/bin/env bash

DIR=$(dirname "$(realpath $0)")
(docker start "jupytercli-$USER" && sleep 5 && docker logs "jupytercli-$USER") || (docker run \
  -d \
  -p 4040:4040 \
  -p 8888:8888 \
  -v $DIR/../src/:/app/src \
  -v $DIR/../data/:/app/data \
  --name "jupytercli-$USER" \
  --network pdmnet \
  jupytercli \
  jupyter lab --no-browser --ip=0.0.0.0 --allow-root && sleep 5 && docker logs "jupytercli-$USER")
