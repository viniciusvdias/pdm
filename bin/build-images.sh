#!/usr/bin/env bash

DIR=$(dirname "$(realpath $0)")
echo $DIR
docker buildx build --progress=plain --output type=docker --tag jupytercli $DIR/../jupytercli
