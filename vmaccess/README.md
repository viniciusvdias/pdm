# VM access via VPN and SSH

**IMPORTANT: there will be ABSOLUTELY NO BACKUP in those VMs. You are
responsible for making backups of your work (code and/or data)**

1. Download and save the VPN file as `pdm/vmaccess/ovpns/pdm-vpn.ovpn` --
   **check course material on how to get this security-sensitive file**.

2. This command will open an SSH session with the VM via VPN. You will be prompted
two times: first will must enter your UFLA credentials, second you must enter
your VM credentials (check course material for this information).

```bash
sshusername=<USERNAME> sshhost=192.168.20.1 ./vmaccess/bin/vpn-and-ssh-to-host.sh
```
