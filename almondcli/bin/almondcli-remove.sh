#!/usr/bin/env bash

DIR=$(dirname "$(realpath $0)")
docker rm "almondcli-$USER"
