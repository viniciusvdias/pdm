#!/usr/bin/env bash

DIR=$(dirname "$(realpath $0)")
docker stack deploy --compose-file $DIR/../spark/docker-compose.yml sparkstack
