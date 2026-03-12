#!/bin/bash
set -e

# Simple CA + server cert generator for harness
WORKDIR=/srv
mkdir -p "$WORKDIR"
cd "$WORKDIR"

# CA
openssl genrsa -out ca.key 2048
openssl req -x509 -new -nodes -key ca.key -sha256 -days 3650 -subj "/CN=Test CA" -out ca.pem

# server key + csr
openssl genrsa -out server.key 2048
openssl req -new -key server.key -subj "/CN=harness.local" -out server.csr

# sign server cert
openssl x509 -req -in server.csr -CA ca.pem -CAkey ca.key -CAcreateserial -out server.crt -days 365 -sha256

# combine
cat server.key server.crt > server.pem

# ensure perms
chmod 644 ca.pem server.crt server.pem

echo "Certificates generated in /srv: ca.pem, server.crt, server.key, server.pem"
