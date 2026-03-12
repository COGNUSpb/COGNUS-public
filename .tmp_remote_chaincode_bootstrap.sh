#!/usr/bin/env bash
set -euo pipefail
RUN_ID="run-2fac528174f6"
CHANNEL="library-channel"
CC_NAME="reserve-book-cc"
CC_VERSION="1.0"

ORDERER=$(docker ps --filter "label=cognus.run_id=${RUN_ID}" --filter "label=cognus.node_type=orderer" --format "{{.Names}}" | head -n1)
PEER=$(docker ps --filter "label=cognus.run_id=${RUN_ID}" --filter "label=cognus.node_type=peer" --format "{{.Names}}" | head -n1)
APIGW=$(docker ps --filter "label=cognus.run_id=${RUN_ID}" --filter "label=cognus.node_type=apigateway" --format "{{.Names}}" | head -n1)

[ -n "$ORDERER" ] && [ -n "$PEER" ] && [ -n "$APIGW" ]
echo "ORDERER=$ORDERER"
echo "PEER=$PEER"
echo "APIGW=$APIGW"

mkdir -p /tmp/cognus

docker image inspect hyperledger/fabric-tools:2.5 >/dev/null 2>&1 || docker pull hyperledger/fabric-tools:2.5 >/dev/null 2>&1

if ! docker run --rm -v /tmp/cognus:/tmp/cognus hyperledger/fabric-tools:2.5 sh -lc "FABRIC_CFG_PATH=/etc/hyperledger/fabric configtxgen -profile SampleSingleMSPChannel -channelID ${CHANNEL} -outputBlock /tmp/cognus/${CHANNEL}.block" >/tmp/cognus/configtxgen.log 2>&1; then
  docker run --rm -v /tmp/cognus:/tmp/cognus hyperledger/fabric-tools:2.5 sh -lc "FABRIC_CFG_PATH=/etc/hyperledger/fabric configtxgen -profile SampleDevModeSolo -channelID ${CHANNEL} -outputBlock /tmp/cognus/${CHANNEL}.block" >/tmp/cognus/configtxgen.log 2>&1 || true
fi

if [ ! -s "/tmp/cognus/${CHANNEL}.block" ]; then
  echo "[bootstrap] failed to generate channel block" >&2
  tail -n 80 /tmp/cognus/configtxgen.log >&2 || true
  exit 21
fi

docker cp "$ORDERER:/etc/hyperledger/fabric/tls/ca.crt" /tmp/cognus/osn-ca.crt >/dev/null 2>&1 || true
docker cp "$ORDERER:/etc/hyperledger/fabric/tls/server.crt" /tmp/cognus/osn-cert.crt >/dev/null 2>&1 || true
docker cp "$ORDERER:/etc/hyperledger/fabric/tls/server.key" /tmp/cognus/osn-key.key >/dev/null 2>&1 || true

docker run --rm --network container:$ORDERER -v /tmp/cognus:/tmp/cognus hyperledger/fabric-tools:2.5 sh -lc "for ep in 127.0.0.1:9443 127.0.0.1:7053; do osnadmin channel join --channelID ${CHANNEL} --config-block /tmp/cognus/${CHANNEL}.block -o \"\$ep\" --ca-file /tmp/cognus/osn-ca.crt --client-cert /tmp/cognus/osn-cert.crt --client-key /tmp/cognus/osn-key.key && exit 0; done; exit 0" >/dev/null 2>&1 || true

docker cp "/tmp/cognus/${CHANNEL}.block" "$PEER:/tmp/${CHANNEL}.block" >/dev/null 2>&1 || true
docker exec "$PEER" sh -lc "peer channel join -b /tmp/${CHANNEL}.block >/dev/null 2>&1 || true"
docker exec "$PEER" sh -lc "peer channel list" | grep -F "$CHANNEL" >/dev/null

echo "[bootstrap] channel ready: $CHANNEL"

docker cp /tmp/reserve-book-cc_1.0.tar.gz "$PEER:/tmp/reserve-book-cc_1.0.tar.gz" >/dev/null 2>&1
docker exec "$PEER" sh -lc "peer lifecycle chaincode install /tmp/reserve-book-cc_1.0.tar.gz >/tmp/cc_install.log 2>&1 || true; tail -n 80 /tmp/cc_install.log"

PKG_ID=$(docker exec "$PEER" sh -lc "peer lifecycle chaincode queryinstalled 2>/dev/null | awk -F, '/reserve-book-cc_1.0/{gsub(/Package ID: /,\"\",\$1); print \$1; exit}'")
if [ -z "$PKG_ID" ]; then
  echo "[bootstrap] package id missing" >&2
  docker exec "$PEER" sh -lc "peer lifecycle chaincode queryinstalled 2>/dev/null || true"
  exit 31
fi

echo "PKG_ID=$PKG_ID"

docker exec "$PEER" sh -lc "peer lifecycle chaincode approveformyorg -o ${ORDERER}:7050 --ordererTLSHostnameOverride SampleOrg-orderer --channelID ${CHANNEL} --name ${CC_NAME} --version ${CC_VERSION} --package-id ${PKG_ID} --sequence 1 --tls --cafile /etc/hyperledger/fabric/tls/ca.crt >/tmp/cc_approve.log 2>&1 || true; tail -n 100 /tmp/cc_approve.log"
docker exec "$PEER" sh -lc "peer lifecycle chaincode checkcommitreadiness --channelID ${CHANNEL} --name ${CC_NAME} --version ${CC_VERSION} --sequence 1 --output json >/tmp/cc_ready.json 2>&1 || true; tail -n 100 /tmp/cc_ready.json"
docker exec "$PEER" sh -lc "peer lifecycle chaincode commit -o ${ORDERER}:7050 --ordererTLSHostnameOverride SampleOrg-orderer --channelID ${CHANNEL} --name ${CC_NAME} --version ${CC_VERSION} --sequence 1 --tls --cafile /etc/hyperledger/fabric/tls/ca.crt >/tmp/cc_commit.log 2>&1 || true; tail -n 120 /tmp/cc_commit.log"
docker exec "$PEER" sh -lc "peer lifecycle chaincode querycommitted -C ${CHANNEL} || true"

docker restart "$APIGW" >/dev/null 2>&1 || true
sleep 3
