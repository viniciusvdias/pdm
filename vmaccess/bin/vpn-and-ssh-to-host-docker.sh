#!/bin/bash

openvpn --config pdm-vpn.ovpn --daemon
ip a &>/tmp/ipa.out
while ! grep "192.168" /tmp/ipa.out >/dev/null; do
  echo "Waiting for VPN connection ..."
  sleep 1
  ip a &>/tmp/ipa.out
done

ssh ${SSH_EXTRA_OPTS} -o StrictHostKeyChecking=no ${SSH_USERNAME}@${SSH_HOST}

killall openvpn
