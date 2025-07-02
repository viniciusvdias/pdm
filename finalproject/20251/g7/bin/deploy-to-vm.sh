#!/bin/bash

set -e

# Nome da imagem/container da VM
VM_IMAGE="g7-vm"
VM_CONTAINER="g7-vm-container"

# Diretórios
DIR=$(dirname "$(realpath $0)")
REAL_DIR="$(realpath "$DIR/../../../../")"

# Solicita dados do usuário
read -p "Digite o nome de usuário SSH (ex: userK, onde k é o seu id do grupo): " SSH_USERNAME
read -p "Digite o host do grupo (informado via email): " SSH_HOST

echo ">> Construindo imagem da máquina virtual com projeto..."
docker build -t $VM_IMAGE -f "$(pwd)/misc/Dockerfile" $REAL_DIR

SSH_EXTRA_OPTS="-L 8888:localhost:8888 -L 8080:localhost:8080 -L 8501:localhost:8501 -L 4040:localhost:4040"

# Inicia container da VM
echo ">> Iniciando container da VM..."
docker run --rm -it \
  --name $VM_CONTAINER \
  --privileged \
  --cap-add=NET_ADMIN \
  --device /dev/net/tun \
  --network host \
  -e "SSH_USERNAME=$SSH_USERNAME" \
  -e "SSH_HOST=$SSH_HOST" \
  -e "SSH_EXTRA_OPTS=$SSH_EXTRA_OPTS" \
  -e "TERM=xterm-256color" \
  -v /var/run/docker.sock:/var/run/docker.sock \
  $VM_IMAGE
