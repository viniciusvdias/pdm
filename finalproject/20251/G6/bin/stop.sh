#!/usr/bin/env bash

DIR=$(dirname "$(realpath $0)")
docker stop "jupytercli-$USER"
