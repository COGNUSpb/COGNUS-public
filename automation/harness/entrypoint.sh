#!/bin/bash
set -e

# create ssh host keys if missing
if [ ! -f /etc/ssh/ssh_host_rsa_key ]; then
  ssh-keygen -A
fi

# create default srv dir
mkdir -p /srv

# generate certs if not present
if [ ! -f /srv/ca.pem ]; then
  /usr/local/bin/prepare_certs.sh
fi

# start a simple HTTP server to serve certs/artifacts
cd /srv
nohup python3 -m http.server 8080 >/var/log/http-server.log 2>&1 &

# start sshd in foreground
/usr/sbin/sshd -D
