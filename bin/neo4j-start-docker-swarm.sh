#!/usr/bin/env bash

DIR=$(dirname "$(realpath $0)")
docker stack deploy --compose-file $DIR/../neo4j/docker-compose.yml neo4jstack
