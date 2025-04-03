#!/usr/bin/env bash

DIR=$(dirname "$(realpath $0)")
docker stack rm kafkastack
