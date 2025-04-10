#!/usr/bin/env bash

set -x

required="filename"
for argname in $required; do
  if [ -z ${!argname+x} ]; then
    >&2 printf "error: $argname is unset\n"
    >&2 printf "$wholeusage\n"
    exit 1
  else
    >&2 echo "info: $argname is set to '${!argname}'"
  fi
done

DIR=$(dirname "$(realpath $0)")

docker run -it --rm --name minio-client \
  --env MINIO_SERVER_HOST="minio" \
  --env MINIO_SERVER_ACCESS_KEY="pdm_minio" \
  --env MINIO_SERVER_SECRET_KEY="pdm_minio" \
  --network pdmnet \
  -v $(realpath $filename):/data/$(basename $filename) \
  bitnami/minio-client \
  sh -c "mc alias set minio http://minio:9000 pdm_minio pdm_minio && mc mb -p minio/public && mc cp /data/$(basename $filename) minio/public/ && mc ls minio/public"
