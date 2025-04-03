#!/usr/bin/env bash

DIR=$(dirname "$(realpath $0)")
docker stack deploy --detach=false --compose-file $DIR/../docker-compose.yml kafkastack
