#!/usr/bin/env bash

required="topic bootstrapserver"
for argname in $required; do
  if [ -z ${!argname+x} ]; then
    >&2 printf "error: $argname is unset\n"
    >&2 printf "$wholeusage\n"
    exit 1
  else
    >&2 echo "info: $argname is set to '${!argname}'"
  fi
done

docker run -it --rm \
  --network pdmnet \
  bitnami/kafka:latest kafka-console-producer.sh --bootstrap-server $bootstrapserver --topic $topic
