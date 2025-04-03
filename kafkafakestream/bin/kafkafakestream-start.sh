#!/usr/bin/env bash

required="host port topic nmessages maxwaitingtime subject"
for argname in $required; do
  if [ -z ${!argname+x} ]; then
    >&2 printf "error: $argname is unset. required=$required\n"
    exit 1
  else
    >&2 echo "info: $argname is set to '${!argname}'"
  fi
done

DIR=$(dirname "$(realpath $0)")

docker run \
  --rm \
  --network pdmnet \
  kafkafakestream \
  python main.py --security-protocol plaintext \
  --host $host \
  --port $port \
  --topic-name $topic \
  --nr-messages $nmessages \
  --max-waiting-time $maxwaitingtime \
  --subject $subject
