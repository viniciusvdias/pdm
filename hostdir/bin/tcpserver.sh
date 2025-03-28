#!/bin/sh

while true; do
  id="$((RANDOM % 21))"
  sentencesize="$((RANDOM % 10 + 1))"
  sentence="$(zcat /app/hostdir/data/100words.txt.gz | shuf -n$sentencesize | tr '\n' ' ')"
  echo "$id $sentence"
  randominterval="$((RANDOM % 3))"
  sleep $randominterval
done
