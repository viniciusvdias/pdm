#!/usr/bin/env bash

DIR=$(dirname "$(realpath $0)")
docker stack deploy --compose-file $DIR/../docker-compose.yml sparkstack
