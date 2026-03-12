#!/bin/bash
# Corrige o ca.crt do orderer para garantir que sempre seja igual ao tlsca da organização
# Uso: ./fix_orderer_ca.sh <org_name> <orderer_name>

set -e

if [ $# -ne 2 ]; then
  echo "Uso: $0 <org_name> <orderer_name>"
  exit 1
fi

ORG_NAME="$1"
ORDERER_NAME="$2"

# Caminho base dos artefatos
BASE="/opt/cello/${ORG_NAME}/crypto-config/ordererOrganizations/cello.local"
TLSCA="$BASE/tlsca/tlsca.cello.local-cert.pem"
ORDERER_TLS="$BASE/orderers/${ORDERER_NAME}/tls/ca.crt"

if [ ! -f "$TLSCA" ]; then
  echo "Arquivo CA não encontrado: $TLSCA"
  exit 2
fi

cp "$TLSCA" "$ORDERER_TLS"
echo "CA do orderer corrigido: $ORDERER_TLS <- $TLSCA"

# Reinicia o container do orderer
if docker ps -a --format '{{.Names}}' | grep -q "^${ORDERER_NAME}$"; then
  docker restart "$ORDERER_NAME"
  echo "Container $ORDERER_NAME reiniciado."
else
  echo "Container $ORDERER_NAME não encontrado."
fi
