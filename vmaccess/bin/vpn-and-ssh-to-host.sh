#!/bin/bash

set -x

required="sshhost sshusername"
for argname in $required; do
  if [ -z ${!argname+x} ]; then
    >&2 printf "error: $argname is unset\n"
    >&2 printf "$wholeusage\n"
    exit 1
  else
    >&2 echo "info: $argname is set to '${!argname}'"
  fi
done

sshportconfig=""
dockerportconfig=""
for p in $bindports; do
  sshportconfig="$sshportconfig -L 0.0.0.0:$p:127.0.0.1:$p"
  dockerportconfig="$dockerportconfig -p $p:$p"
done

docker info &>/dev/null

if [ "$?" == 1 ]; then
  sudo systemctl start docker.service
fi

docker run -it --privileged=true --name pdmvmaccess --rm \
  -e "SSH_HOST=$sshhost" \
  -e "SSH_USERNAME=$sshusername" \
  -e "SSH_EXTRA_OPTS=$sshextraopts $sshportconfig" \
  -e "TERM=xterm-256color" \
  $dockerportconfig \
  pdmvmaccess
