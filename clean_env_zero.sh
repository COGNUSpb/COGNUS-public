#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

FULL_PRUNE=0
KEEP_IMAGES=0

for arg in "$@"; do
  case "$arg" in
    --full-prune)
      FULL_PRUNE=1
      ;;
    --keep-images)
      KEEP_IMAGES=1
      ;;
  esac
done

log() {
  local level="$1"
  shift
  printf '[%s] %s\n' "$level" "$*"
}
log_info() { log INFO "$@"; }
log_warn() { log AVISO "$@"; }

select_docker_compose() {
  if docker compose version >/dev/null 2>&1; then
    DOCKER_COMPOSE_CMD=(docker compose)
    return
  fi
  if command -v docker-compose >/dev/null 2>&1; then
    DOCKER_COMPOSE_CMD=(docker-compose)
    return
  fi
  DOCKER_COMPOSE_CMD=()
}

docker_safe_rm_container() {
  local name="$1"
  docker rm -f "$name" >/dev/null 2>&1 || true
}

docker_safe_rm_network() {
  local name="$1"
  docker network rm "$name" >/dev/null 2>&1 || true
}

safe_rm_tree() {
  local rel_path="$1"
  [ -n "$rel_path" ] || return 0

  if [ ! -e "$SCRIPT_DIR/$rel_path" ]; then
    return 0
  fi

  if docker info >/dev/null 2>&1; then
    docker run --rm -v "$SCRIPT_DIR":/workspace busybox sh -lc "rm -rf '/workspace/$rel_path'" >/dev/null 2>&1 && return 0
  fi

  rm -rf "$SCRIPT_DIR/$rel_path" >/dev/null 2>&1 && return 0
  sudo rm -rf "$SCRIPT_DIR/$rel_path" >/dev/null 2>&1 || true
}

log_info "Iniciando limpeza zero do ambiente COGNUS/Cello"

if docker info >/dev/null 2>&1; then
  select_docker_compose

  if [ ${#DOCKER_COMPOSE_CMD[@]} -gt 0 ]; then
    if [ -f "cello-v2/bootup/docker-compose-files/docker-compose.dev.yml" ]; then
      log_info "Derrubando stack principal"
      "${DOCKER_COMPOSE_CMD[@]}" -f cello-v2/bootup/docker-compose-files/docker-compose.dev.yml down --remove-orphans -v || true
    fi

    if [ -f "chaincode-gateway/docker-compose.yaml" ]; then
      log_info "Derrubando stack chaincode-gateway"
      (cd chaincode-gateway && "${DOCKER_COMPOSE_CMD[@]}" down --remove-orphans) || true
    fi
  fi

  log_info "Removendo contêineres runbook/cello/chaincode"
  while IFS= read -r cname; do
    [ -n "$cname" ] || continue
    case "$cname" in
      cognusrb-*|cello-*|chaincode-gateway|fabric-auto-operator|peer*|orderer*|ca_*|couchdb*|dev-peer*|*-cdb)
        docker_safe_rm_container "$cname"
        ;;
    esac
  done < <(docker ps -a --format '{{.Names}}' || true)

  log_info "Removendo redes do ambiente"
  docker_safe_rm_network "cello-net"
  docker_safe_rm_network "cognus-runbook-net"
  docker_safe_rm_network "chaincode-blockchain-stcs-net"

  log_info "Removendo volumes do ambiente"
  docker volume ls --format '{{.Name}}' | grep -E '^(cello-|cognusrb-|runbook-|chaincode-)' | xargs -r docker volume rm >/dev/null 2>&1 || true
  docker volume rm -f cello-postgres >/dev/null 2>&1 || true

  if [ "$FULL_PRUNE" = "1" ]; then
    log_warn "Executando docker system prune completo (--full-prune)"
    if [ "$KEEP_IMAGES" = "1" ]; then
      docker system prune -f --volumes || true
    else
      docker system prune -a -f --volumes || true
    fi
  elif [ "$KEEP_IMAGES" = "0" ]; then
    log_info "Removendo imagens locais de runtime do projeto"
    docker images --format '{{.Repository}}:{{.Tag}}' \
      | grep -E '^(hyperledger/cello-api-engine|cognus/chaincode-gateway|hyperledger/fabric-(peer|orderer|ca|ccenv)):' \
      | xargs -r docker rmi -f >/dev/null 2>&1 || true
  fi
else
  log_warn "Docker indisponível; limpando apenas artefatos locais."
fi

log_info "Removendo artefatos, caches e resíduos de teste"
safe_rm_tree "automation/resources/crypto-config"
safe_rm_tree "automation/resources/configtx"
safe_rm_tree "automation/resources/channel-artifacts"
safe_rm_tree "chaincode-gateway/data"
safe_rm_tree "cello-v2/bootup/chaincode-gateway/data"
safe_rm_tree "cello-v2/cello-storage/pgdata"
safe_rm_tree "cello-storage/pgdata"
safe_rm_tree "cello-v2/src/api-engine/cello-storage/pgdata"
safe_rm_tree ".pytest_cache"
safe_rm_tree "htmlcov"

find . -type d -name '__pycache__' -prune -exec rm -rf {} + 2>/dev/null || true
find . -type f -name '*.pyc' -delete 2>/dev/null || true
find . -type f -name '*.log' -delete 2>/dev/null || true
find . -type f -name '.coverage*' -delete 2>/dev/null || true
find . -type f -name 'runbook_store*.json' -delete 2>/dev/null || true

mkdir -p chaincode-gateway/data cello-v2/bootup/chaincode-gateway/data
if [ -f chaincode-gateway/templates/fabric-tlscas.pem ]; then
  cp chaincode-gateway/templates/fabric-tlscas.pem chaincode-gateway/data/fabric-tlscas.pem || true
  cp chaincode-gateway/templates/fabric-tlscas.pem cello-v2/bootup/chaincode-gateway/data/fabric-tlscas.pem || true
fi

mkdir -p cello-v2/src/dashboard/public
cleaned_at_utc="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
cat > cello-v2/src/dashboard/public/cognus-clean-marker.json <<EOF
{
  "cleaned_at_utc": "$cleaned_at_utc",
  "source": "clean_env_zero.sh"
}
EOF

log_info "Ambiente zerado concluído."
