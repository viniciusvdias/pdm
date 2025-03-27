# VM access via VPN and SSH

**IMPORTANT: there will be ABSOLUTELY NO BACKUP in those VMs. You are
responsible for making backups of your work (code and/or data)**

1. Make sure repository images are properly build with:

```bash
make
```

2. Download and save the VPN file as `pdm/vmaccess/ovpns/pdm-vpn.ovpn` --
   **check course material on how to get this security-sensitive file**.

3. This command will open an SSH session with the VM via VPN. You will be prompted
two times: first will must enter your UFLA credentials, second you must enter
your VM credentials (check course material for this information).

```bash
sshusername=<USERNAME> sshhost=<GROUP_HOST> ./vmaccess/bin/vpn-and-ssh-to-host.sh
```

`USERNAME`: `userK`, where `k` is your group ID.
`GROUP_HOST`: informed to you by e-mail, **each group logs into a specific VM --
please respect that**.

This command will prompt you:

1. First, enter **UFLA institutional username and password**. This is for VPN
   access.
2. Second, enter the password of your group username (this will be informed by
   e-mail).
