#!/bin/bash

set -e

openvpn --config ../pdm-vpn.ovpn --daemon
sleep 2

ip a &>/tmp/ipa.out
while ! grep "192.168" /tmp/ipa.out >/dev/null; do
  echo "Waiting for VPN connection ..."
  sleep 1
  ip a &>/tmp/ipa.out
done

echo ">> Conectando via SSH..."

# Pasta temporária para socket de controle
mkdir -p /tmp/ssh_control
SOCKET="/tmp/ssh_control/%r@%h:%p"

# Estabelece conexão SSH com controle
ssh -o ControlMaster=yes -o ControlPersist=5m -o ControlPath="$SOCKET" \
    -o StrictHostKeyChecking=no ${SSH_USERNAME}@${SSH_HOST} true

echo ">> Apagando a pasta g7 da VM (se existir)"
ssh -o ControlPath="$SOCKET" ${SSH_USERNAME}@${SSH_HOST} "rm -rf ~/g7"

echo ">> Copiando pasta g7 para a VM via SCP..."

# Cria tar local com exclusões
tar -czf /tmp/g7.tar.gz --exclude-from="../exclude.txt" -C . .

# Envia tar para a VM
scp -o ControlPath="$SOCKET" /tmp/g7.tar.gz ${SSH_USERNAME}@${SSH_HOST}:~/

# Extrai tar na VM
ssh -o ControlPath="$SOCKET" ${SSH_USERNAME}@${SSH_HOST} <<EOF
  mkdir ~/g7
  tar -xzf ~/g7.tar.gz -C ~/g7
  rm ~/g7.tar.gz
EOF

# Limpa o tar temporário
rm /tmp/g7.tar.gz

echo ">> Entrando na VM..."
exec ssh -o ControlPath="$SOCKET" ${SSH_EXTRA_OPTS} ${SSH_USERNAME}@${SSH_HOST}
