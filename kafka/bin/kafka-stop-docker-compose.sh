#!/usr/bin/env bash

DIR=$(dirname "$(realpath $0)")
cd "$DIR/.." && docker compose down
