#!/usr/bin/env bash

NETWORK=${network:-pdmnet-local}
required="bootstrapserver topic"
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
  --network "$NETWORK" \
  bitnami/kafka:latest kafka-console-consumer.sh --bootstrap-server $bootstrapserver --topic $topic --from-beginning
