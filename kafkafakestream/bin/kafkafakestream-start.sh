#!/usr/bin/env bash

NETWORK=${network:-pdmnet-local}
required="host port topic nmessages maxwaitingtime subject"
for argname in $required; do
  if [ -z ${!argname+x} ]; then
    >&2 printf "error: $argname is unset. required=$required\n"
    exit 1
  else
    >&2 echo "info: $argname is set to '${!argname}'"
  fi
done

case "$subject" in
stock | userbehaviour | realstock | metric | bet | rolling | advancedmetric | pizza) ;;
*)
  >&2 printf "error: valid 'subject' value: stock|userbehaviour|realstock|metric|bet|rolling|advancedmetric|pizza\n"
  exit 1
  ;;
esac

DIR=$(dirname "$(realpath $0)")

docker run \
  --rm \
  --network "$NETWORK" \
  kafkafakestream \
  python main.py --security-protocol plaintext \
  --host $host \
  --port $port \
  --topic-name $topic \
  --nr-messages $nmessages \
  --max-waiting-time $maxwaitingtime \
  --subject $subject
