#!/usr/bin/env bash
# Script para inicializar o ambiente COGNUSpb/Cello em modo offline-first.
# Prioriza artefatos locais, garantindo builds mesmo sem acesso ao Docker Hub.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

DEFAULT_STORAGE_PATH="$SCRIPT_DIR/cello-v2/cello-storage"
mkdir -p "$DEFAULT_STORAGE_PATH"
export CELLO_STORAGE_PATH="${CELLO_STORAGE_PATH:-$DEFAULT_STORAGE_PATH}"
SECONDARY_STORAGE_PATH="$SCRIPT_DIR/cello-v2/src/api-engine/cello-storage"

DEFAULT_ORDERER_HOST="orderer1.cello.local"
DEFAULT_ORDERER_TLS_CA=""
orderer_search_roots=(
  "$SECONDARY_STORAGE_PATH/crypto-config/ordererOrganizations"
  "$CELLO_STORAGE_PATH/crypto-config/ordererOrganizations"
)
for orderer_root in "${orderer_search_roots[@]}"; do
  [ -d "$orderer_root" ] || continue
  first_orderer_org="$(find "$orderer_root" -mindepth 1 -maxdepth 1 -type d | head -n1 2>/dev/null)"
  if [ -n "$first_orderer_org" ]; then
    first_orderer_dir="$(find "$first_orderer_org/orderers" -mindepth 1 -maxdepth 1 -type d | head -n1 2>/dev/null)"
    if [ -n "$first_orderer_dir" ]; then
      DEFAULT_ORDERER_HOST="$(basename "$first_orderer_dir")"
      orderer_domain="$(basename "$first_orderer_org")"
      potential_tls="$first_orderer_org/tlsca/tlsca.${orderer_domain}-cert.pem"
      if [ -f "$potential_tls" ]; then
        DEFAULT_ORDERER_TLS_CA="$potential_tls"
      fi
      break
    fi
  fi
done

declare -a DISCOVERED_CHANNELS=()

BASE_IMAGES=(
  python:3.8
  postgres:12.0
  node:20.15
  nginx:1.15.12
  node:22-alpine
  hyperledger/cello-api-engine:latest
)

OPTIONAL_GATEWAY_IMAGES=(
  python:3.11-slim
)

OPTIONAL_PEER_RUNTIME_IMAGES=(
  golang:1.24.2
  couchdb:3.3.3
)

IMAGE_TAR_DIRS=(
  "$SCRIPT_DIR"
  "$SCRIPT_DIR/docker-images"
  "$SCRIPT_DIR/images"
  "$SCRIPT_DIR/artifacts"
)
START_ENV_FORCE_OFFLINE="${START_ENV_FORCE_OFFLINE:-0}"
START_ENV_FORCE_CLASSIC_BUILDER="${START_ENV_FORCE_CLASSIC_BUILDER:-1}"
CHAINCODE_EXTERNAL_NETWORK="${CHAINCODE_EXTERNAL_NETWORK:-chaincode-blockchain-stcs-net}"
COGNUS_ENABLE_LOCAL_GATEWAY="${COGNUS_ENABLE_LOCAL_GATEWAY:-0}"
COGNUS_ENABLE_LOCAL_PEER_RUNTIME="${COGNUS_ENABLE_LOCAL_PEER_RUNTIME:-0}"
COGNUS_ENABLE_DOCKER_AGENT="${COGNUS_ENABLE_DOCKER_AGENT:-0}"
COGNUS_APIGATEWAY_IMAGE_PRIMARY="${COGNUS_APIGATEWAY_IMAGE_PRIMARY:-cognus/chaincode-gateway:latest}"
COGNUS_APIGATEWAY_IMAGE_LEGACY_ALIAS="${COGNUS_APIGATEWAY_IMAGE_LEGACY_ALIAS:-chaincode-gateway:latest}"

declare -a MISSING_IMAGES=()
REGISTRY_AVAILABLE=0
DOCKER_COMPOSE_CMD=()
MAKE_AVAILABLE=0

log() {
  local level="$1"
  shift
  printf '[%s] %s\n' "$level" "$*"
}
log_info() { log INFO "$@"; }
log_warn() { log AVISO "$@"; }
log_error() { log ERRO "$@"; }

flag_enabled() {
  case "$(printf '%s' "${1:-0}" | tr '[:upper:]' '[:lower:]')" in
    1|true|yes|on) return 0 ;;
    *) return 1 ;;
  esac
}

ensure_command() {
  local cmd="$1"
  local pkg="${2:-$1}"
  if ! command -v "$cmd" >/dev/null 2>&1; then
    log_error "Dependência '$cmd' não encontrada. Instale $pkg e execute novamente."
    exit 1
  fi
}

restart_docker() {
  if command -v systemctl >/dev/null 2>&1; then
    sudo systemctl restart docker >/dev/null 2>&1 && return 0
  fi
  sudo service docker restart >/dev/null 2>&1
}

ensure_docker_daemon_available() {
  if docker info >/dev/null 2>&1; then
    log_info "Docker daemon disponível."
    return
  fi

  log_warn "Docker daemon indisponível. Tentando iniciar automaticamente..."
  if command -v systemctl >/dev/null 2>&1; then
    sudo systemctl start docker >/dev/null 2>&1 || true
  elif command -v service >/dev/null 2>&1; then
    sudo service docker start >/dev/null 2>&1 || true
  fi

  sleep 2
  if ! docker info >/dev/null 2>&1; then
    log_error "Docker daemon indisponível. Inicie o serviço Docker e execute novamente."
    exit 1
  fi

  log_info "Docker daemon iniciado com sucesso."
}

configure_docker_ipv4() {
  local daemon_file="/etc/docker/daemon.json"
  if [ ! -f "$daemon_file" ]; then
    log_info "Aplicando configuração DNS/IPv4 padrão em $daemon_file."
    echo '{"dns":["8.8.8.8","1.1.1.1"],"ipv6":false}' | sudo tee "$daemon_file" >/dev/null
    if ! restart_docker; then
      log_warn "Não foi possível reiniciar o daemon Docker automaticamente. Reinicie manualmente antes de continuar."
    fi
  fi
  log_info "Forçando pilha IPv4 para pulls do Docker."
  sudo sysctl -w net.ipv6.conf.all.disable_ipv6=1 >/dev/null
  sudo sysctl -w net.ipv6.conf.default.disable_ipv6=1 >/dev/null
  prefer_ipv4_resolution
  enforce_dockerd_ipv4_runtime
}

check_registry_connectivity() {
  if ! command -v curl >/dev/null 2>&1; then
    return 1
  fi
  local status
  status="$(curl -4 -sS -o /dev/null -w '%{http_code}' --max-time 5 https://registry-1.docker.io/v2/ || true)"
  case "$status" in
    200|301|302|401) return 0 ;;
    *) return 1 ;;
  esac
}

prefer_ipv4_resolution() {
  local gai_conf="/etc/gai.conf"
  local precedence_rule="precedence ::ffff:0:0/96  100"
  local hosts=(registry-1.docker.io auth.docker.io production.cloudflare.docker.com docker.io hub.docker.com)

  if ! grep -qF "$precedence_rule" "$gai_conf" 2>/dev/null; then
    log_info "Aumentando precedência IPv4 no $gai_conf."
    echo "$precedence_rule" | sudo tee -a "$gai_conf" >/dev/null
  else
    log_info "Preferência IPv4 já configurada em $gai_conf."
  fi

  for host in "${hosts[@]}"; do
    ensure_hosts_ipv4_override "$host"
  done
}

ensure_hosts_ipv4_override() {
  local host="$1"
  local ip
  ip="$(getent ahostsv4 "$host" | awk 'NR==1 {print $1}')"
  if [ -z "$ip" ]; then
    log_warn "Não foi possível obter IPv4 para $host. Verifique DNS."
    return
  fi

  if grep -q "[[:space:]]$host" /etc/hosts; then
    if grep -q "^$ip[[:space:]]\+$host" /etc/hosts; then
      log_info "Entrada IPv4 fixa para $host já configurada."
    else
      log_info "Atualizando IPv4 fixo para $host ($ip)."
      sudo sed -i.bak "/[[:space:]]$host/d" /etc/hosts
      echo "$ip $host" | sudo tee -a /etc/hosts >/dev/null
    fi
  else
    log_info "Fixando resolução IPv4 para $host ($ip) em /etc/hosts."
    echo "$ip $host" | sudo tee -a /etc/hosts >/dev/null
  fi
}

enforce_dockerd_ipv4_runtime() {
  if ! command -v systemctl >/dev/null 2>&1; then
    log_warn "systemd não disponível; defina GODEBUG=ipv6=0 manualmente para o dockerd."
    return
  fi

  local dropin="/etc/systemd/system/docker.service.d/10-force-ipv4.conf"
  local dropin_dir
  dropin_dir="$(dirname "$dropin")"
  local desired_env='Environment="GODEBUG=netdns=go+1,ipv6=0"'

  sudo mkdir -p "$dropin_dir"

  if [ -f "$dropin" ] && grep -q "$desired_env" "$dropin"; then
    log_info "dockerd já executa com GODEBUG=ipv6=0."
    return
  fi

  log_info "Configurando dockerd para desabilitar IPv6 no runtime (GODEBUG=ipv6=0)."
  {
    echo "[Service]"
    echo "$desired_env"
  } | sudo tee "$dropin" >/dev/null

  sudo systemctl daemon-reload || true
  if ! restart_docker; then
    log_warn "Reinicie manualmente o serviço Docker para aplicar GODEBUG=ipv6=0."
  fi
}

find_tar_for_image() {
  local img="$1"
  local tar_name="${img//:/_}.tar"
  for dir in "${IMAGE_TAR_DIRS[@]}"; do
    local candidate="$dir/$tar_name"
    if [ -f "$candidate" ]; then
      printf '%s\n' "$candidate"
      return 0
    fi
  done
  return 1
}

load_image_from_tar() {
  local img="$1"
  local tar_path
  if ! tar_path=$(find_tar_for_image "$img"); then
    return 1
  fi

  if [ ! -s "$tar_path" ]; then
    log_warn "Arquivo $tar_path está vazio. Ignorando."
    return 1
  fi

  log_info "Importando $img do arquivo $tar_path"
  if docker load -i "$tar_path"; then
    if docker image inspect "$img" >/dev/null 2>&1; then
      log_info "Imagem $img importada localmente."
      return 0
    fi
    log_warn "Importação concluída, mas a tag $img não foi encontrada. Verifique o conteúdo de $tar_path."
    return 1
  fi

  log_warn "Falha ao importar $img do arquivo $tar_path."
  return 1
}

pull_image_online() {
  local img="$1"
  local repo tag alt_img
  if [ "$REGISTRY_AVAILABLE" -ne 1 ]; then
    return 1
  fi
  log_info "Tentando baixar $img do Docker Hub..."
  if docker_pull_with_ipv4_enforcement "$img"; then
    return 0
  fi
  log_warn "Pull padrão falhou para $img. Tentando forçar arquitetura linux/amd64."
  if docker_pull_with_ipv4_enforcement "$img" --platform linux/amd64; then
    return 0
  fi
  repo="${img%%:*}"
  tag="${img##*:}"
  alt_img="registry.docker-cn.com/library/${repo}:${tag}"
  log_warn "Tentando mirror alternativo: $alt_img"
  if docker_pull_with_ipv4_enforcement "$alt_img"; then
    docker tag "$alt_img" "$img"
    docker rmi "$alt_img" >/dev/null 2>&1 || true
    return 0
  fi
  return 1
}

fetch_image_via_skopeo() {
  local img="$1"
  if [ "$START_ENV_FORCE_OFFLINE" = "1" ]; then
    return 1
  fi
  if ! command -v skopeo >/dev/null 2>&1; then
    log_warn "Skopeo não encontrado no PATH; instale (ex.: sudo apt install -y skopeo) para habilitar o fallback."
    return 1
  fi
  log_info "Tentando baixar $img via skopeo (forçando IPv4)."
  if GODEBUG=netdns=go+1,ipv6=0 skopeo --override-os linux --override-arch amd64 copy "docker://$img" "docker-daemon:$img"; then
    log_info "Imagem $img importada via skopeo."
    return 0
  fi
  log_warn "Falha ao importar $img via skopeo."
  return 1
}

docker_pull_with_ipv4_enforcement() {
  local img="$1"
  shift
  local extra_args=("$@")
  local attempt output status matched_host=0

  for attempt in 1 2 3; do
    if output=$(GODEBUG=netdns=go+1,ipv6=0 docker pull "${extra_args[@]}" "$img" 2>&1); then
      printf '%s\n' "$output"
      return 0
    fi
    status=$?
    printf '%s\n' "$output" >&2
    add_ipv4_overrides_from_output "$output" && matched_host=1
    if [ $attempt -lt 3 ]; then
      if [ $matched_host -eq 1 ]; then
        log_warn "Pull falhou, mas novos hosts foram fixados para IPv4. Tentando novamente..."
      else
        log_warn "Pull falhou. Tentando novamente..."
      fi
      matched_host=0
      sleep 1
    fi
  done
  return $status
}

add_ipv4_overrides_from_output() {
  local log="$1"
  local url host added=0
  while read -r url; do
    host="${url#https://}"
    host="${host%%/*}"
    host="${host%%\"}"
    host="${host%%\'}"
    if [ -n "$host" ]; then
      ensure_hosts_ipv4_override "$host"
      added=1
    fi
  done < <(printf '%s' "$log" | grep -oE 'https://[A-Za-z0-9._:-]+' | sort -u || true)
  return $added
}

ensure_image() {
  local img="$1"
  if docker image inspect "$img" >/dev/null 2>&1; then
    log_info "Imagem $img já presente."
    return 0
  fi
  if load_image_from_tar "$img"; then
    return 0
  fi
  if pull_image_online "$img"; then
    log_info "Imagem $img baixada com sucesso."
    return 0
  fi
  if fetch_image_via_skopeo "$img"; then
    return 0
  fi
  log_error "Não foi possível preparar a imagem $img."
  MISSING_IMAGES+=("$img")
  return 1
}

select_docker_compose() {
  if docker compose version >/dev/null 2>&1; then
    DOCKER_COMPOSE_CMD=(docker compose)
    return 0
  elif command -v docker-compose >/dev/null 2>&1; then
    DOCKER_COMPOSE_CMD=(docker-compose)
    return 0
  fi
  log_error "Docker Compose não encontrado. Instale o plugin oficial ou o binário docker-compose."
  exit 1
}

using_legacy_docker_compose() {
  [ "${DOCKER_COMPOSE_CMD[0]:-}" = "docker-compose" ]
}

compose_up_detached() {
  local compose_file="$1"
  shift
  local extra_args=("$@")

  if using_legacy_docker_compose; then
    log_warn "docker-compose legado detectado; executando 'up -d' sem --force-recreate para evitar falha conhecida de ContainerConfig."
    "${DOCKER_COMPOSE_CMD[@]}" -f "$compose_file" up -d --remove-orphans "${extra_args[@]}"
    return
  fi

  "${DOCKER_COMPOSE_CMD[@]}" -f "$compose_file" up -d --force-recreate --remove-orphans "${extra_args[@]}"
}

run_make() {
  local dir="$1"
  local target="$2"
  if [ "$MAKE_AVAILABLE" -eq 1 ]; then
    log_info "Executando 'make $target' em $dir"
    (cd "$dir" && make "$target")
    return
  fi

  run_make_fallback "$dir" "$target"
}

resolve_docker_platform_arch() {
  case "$(uname -m)" in
    x86_64) printf '%s\n' 'amd64' ;;
    aarch64|arm64) printf '%s\n' 'arm64' ;;
    *) uname -m ;;
  esac
}

run_make_fallback() {
  local dir="$1"
  local target="$2"
  local platform_arch

  platform_arch="$(resolve_docker_platform_arch)"
  log_warn "'make' não encontrado; executando fallback nativo para o alvo '$target'."

  case "$target" in
    dashboard)
      (cd "$dir" && docker build -t hyperledger/cello-dashboard:latest -f build_image/docker/common/dashboard/Dockerfile.in ./)
      ;;
    api-engine)
      (cd "$dir" && docker build -t hyperledger/cello-api-engine:latest -f build_image/docker/common/api-engine/Dockerfile.in ./ --platform "linux/$platform_arch")
      ;;
    docker-rest-agent)
      (cd "$dir" && docker build -t hyperledger/cello-agent-docker:latest -f build_image/docker/agent/docker-rest-agent/Dockerfile.in ./ --build-arg pip=pip.conf.bak --platform "linux/$platform_arch")
      ;;
    fabric)
      (cd "$dir" && docker build -t hyperledger/fabric:2.5.13 -f build_image/docker/cello-hlf/Dockerfile build_image/docker/cello-hlf/)
      ;;
    *)
      log_error "Fallback para target '$target' não implementado. Instale Make e execute novamente."
      exit 1
      ;;
  esac
}

resolve_peer_runtime_compose_file() {
  if [ -n "${COGNUS_PEER_RUNTIME_COMPOSE_FILE:-}" ]; then
    if [ -f "$COGNUS_PEER_RUNTIME_COMPOSE_FILE" ]; then
      printf '%s\n' "$COGNUS_PEER_RUNTIME_COMPOSE_FILE"
      return 0
    fi
    log_warn "COGNUS_PEER_RUNTIME_COMPOSE_FILE definido, mas arquivo não encontrado: $COGNUS_PEER_RUNTIME_COMPOSE_FILE"
  fi

  local discovered_file
  discovered_file="$(find "$SCRIPT_DIR" -maxdepth 8 -type f -path '*/fabric/docker/docker-compose-couch.yaml' 2>/dev/null | sort | head -n 1 || true)"
  if [ -n "$discovered_file" ] && [ -f "$discovered_file" ]; then
    printf '%s\n' "$discovered_file"
    return 0
  fi

  return 1
}

run_osnadmin_channel_join() {
  local channel_id="$1"
  local config_block="$2"
  local orderer_host="$3"
  local ca_file="$4"
  local client_cert="$5"
  local client_key="$6"

  if command -v osnadmin >/dev/null 2>&1; then
    osnadmin channel join --channelID "$channel_id" --config-block "$config_block" -o "$orderer_host" --ca-file "$ca_file" --client-cert "$client_cert" --client-key "$client_key"
    return $?
  fi

  if ! docker image inspect hyperledger/fabric-tools:2.5 >/dev/null 2>&1; then
    docker pull hyperledger/fabric-tools:2.5 >/dev/null 2>&1 || true
  fi

  if docker image inspect hyperledger/fabric-tools:2.5 >/dev/null 2>&1; then
    docker run --rm --network cello-net -v /:/host:ro hyperledger/fabric-tools:2.5 \
      osnadmin channel join --channelID "$channel_id" --config-block "/host$config_block" -o "$orderer_host" --ca-file "/host$ca_file" --client-cert "/host$client_cert" --client-key "/host$client_key"
    return $?
  fi

  return 127
}

prepare_permissions() {
  local paths=("$@")
  for path in "${paths[@]}"; do
    mkdir -p "$path"
    log_info "Ajustando permissões em $path"
    sudo chown -R "$USER":"$USER" "$path"
    sudo chmod -R a+rX "$path"
  done
}

derive_org_alias() {
  local fqdn="$1"
  if [[ "$fqdn" == *.* ]]; then
    printf '%s\n' "${fqdn%%.*}"
  else
    printf '%s\n' "$fqdn"
  fi
}

to_msp_id() {
  local domain="$1"
  local base="${domain%%.*}"
  if [ -z "$base" ]; then
    base="$domain"
  fi
  local first_char="${base:0:1}"
  local rest="${base:1}"
  first_char="$(printf '%s' "$first_char" | tr '[:lower:]' '[:upper:]')"
  rest="$(printf '%s' "$rest" | tr '[:upper:]' '[:lower:]')"
  printf '%s%sMSP\n' "$first_char" "$rest"
}

discover_channels() {
  local -A seen=()
  local -a channels=()
  local search_paths=(
    "$CELLO_STORAGE_PATH"
    "$SECONDARY_STORAGE_PATH"
  )
  for base in "${search_paths[@]}"; do
    [ -d "$base" ] || continue
    while IFS= read -r block; do
      local filename channel
      filename="$(basename "$block")"
      channel="${filename%.block}"
      if [ -n "$channel" ] && [ -z "${seen[$channel]:-}" ]; then
        channels+=("$channel")
        seen[$channel]=1
      fi
    done < <(find "$base" -maxdepth 2 -type f -name '*.block' 2>/dev/null)
  done
  if [ ${#channels[@]} -eq 0 ]; then
    channels=("stcs-channel")
  fi
  printf '%s\n' "${channels[@]}"
}

discover_peer_hosts() {
  local -A seen=()
  local -a hosts=()
  local search_roots=(
    "$SECONDARY_STORAGE_PATH/crypto-config/peerOrganizations"
    "$CELLO_STORAGE_PATH/crypto-config/peerOrganizations"
  )
  for root in "${search_roots[@]}"; do
    [ -d "$root" ] || continue
    while IFS= read -r peer_dir; do
      [ -d "$peer_dir" ] || continue
      local fqdn
      fqdn="$(basename "$peer_dir")"
      if [ -n "$fqdn" ] && [ -z "${seen[$fqdn]:-}" ]; then
        hosts+=("$fqdn")
        seen[$fqdn]=1
      fi
    done < <(find "$root" -mindepth 3 -maxdepth 3 -type d -path '*/peers/*' 2>/dev/null)
  done
  printf '%s\n' "${hosts[@]}"
}

discover_orderer_hosts() {
  local -A seen=()
  local -a hosts=()
  local search_roots=(
    "$SECONDARY_STORAGE_PATH/crypto-config/ordererOrganizations"
    "$CELLO_STORAGE_PATH/crypto-config/ordererOrganizations"
  )
  for root in "${search_roots[@]}"; do
    [ -d "$root" ] || continue
    while IFS= read -r orderer_dir; do
      [ -d "$orderer_dir" ] || continue
      local fqdn
      fqdn="$(basename "$orderer_dir")"
      if [ -n "$fqdn" ] && [ -z "${seen[$fqdn]:-}" ]; then
        hosts+=("$fqdn")
        seen[$fqdn]=1
      fi
    done < <(find "$root" -mindepth 3 -maxdepth 3 -type d -path '*/orderers/*' 2>/dev/null)
  done
  printf '%s\n' "${hosts[@]}"
}

resolve_orderer_for_org() {
  local org_domain="$1"
  local orderer_domain="${org_domain#*.}"
  local fallback_host="$DEFAULT_ORDERER_HOST"
  local fallback_tls="$DEFAULT_ORDERER_TLS_CA"
  if [ -z "$orderer_domain" ] || [ "$orderer_domain" = "$org_domain" ]; then
    printf '%s;%s\n' "$fallback_host" "${fallback_tls:-}"
    return 0
  fi
  local search_roots=(
    "$SECONDARY_STORAGE_PATH/crypto-config/ordererOrganizations"
    "$CELLO_STORAGE_PATH/crypto-config/ordererOrganizations"
  )
  for root in "${search_roots[@]}"; do
    local domain_dir="$root/$orderer_domain"
    [ -d "$domain_dir" ] || continue
    local orderer_dir
    orderer_dir="$(find "$domain_dir/orderers" -mindepth 1 -maxdepth 1 -type d | head -n1 2>/dev/null)"
    if [ -n "$orderer_dir" ]; then
      local host
      host="$(basename "$orderer_dir")"
      local tls_path="$domain_dir/tlsca/tlsca.${orderer_domain}-cert.pem"
      if [ ! -f "$tls_path" ]; then
        tls_path=""
      fi
      printf '%s;%s\n' "$host" "$tls_path"
      return 0
    fi
  done
  printf '%s;%s\n' "$fallback_host" "${fallback_tls:-}"
}

sync_chaincode_gateway_artifacts() {
  local data_dir="chaincode-gateway/data"
  local storage_candidates=(
    "$CELLO_STORAGE_PATH"
    "$SECONDARY_STORAGE_PATH"
  )
  local copied_any=0
  for storage_root in "${storage_candidates[@]}"; do
    [ -d "$storage_root" ] || continue
    find "$storage_root" -type f -name 'tlsca.*.cello.local-cert.pem' | while read -r source_bundle; do
      copied_any=1
      bundle_name="$(basename "$source_bundle")"
      target_bundle="$data_dir/$bundle_name"
      if [ -f "$target_bundle" ]; then
        log_info "Bundle TLS já presente em $target_bundle."
        continue
      fi
      log_info "Copiando bundle TLS do caminho $source_bundle para $target_bundle"
      cp "$source_bundle" "$target_bundle"
      sudo chown "$USER":"$USER" "$target_bundle" || true
      sudo chmod 644 "$target_bundle" || true
    done
  done
  if [ "$copied_any" -eq 0 ]; then
    log_warn "Nenhum bundle TLS encontrado em ${storage_candidates[*]}."
  fi
}

sync_gateway_alias_artifacts() {
  local data_dir="chaincode-gateway/data"
  local storage_candidates=(
    "$CELLO_STORAGE_PATH"
    "$SECONDARY_STORAGE_PATH"
  )
  local -A processed_orgs=()

  for crypto_root in "${storage_candidates[@]}"; do
    local peer_orgs_dir="$crypto_root/crypto-config/peerOrganizations"
    [ -d "$peer_orgs_dir" ] || continue
    for org_dir in "$peer_orgs_dir"/*; do
      [ -d "$org_dir" ] || continue
      local org_domain
      org_domain="$(basename "$org_dir")"
      if [ -n "${processed_orgs[$org_domain]:-}" ]; then
        continue
      fi
      processed_orgs[$org_domain]=1

      local alias
      alias="$(derive_org_alias "$org_domain")"
      [ -n "$alias" ] || alias="$org_domain"
      local alias_dir="$data_dir/${alias}"
      rm -rf "$alias_dir"
      mkdir -p "$alias_dir/msp/signcerts" "$alias_dir/msp/keystore" "$alias_dir/tls"
      sudo chown -R "$USER":"$USER" "$alias_dir" || true
      sudo chmod -R a+rX "$alias_dir" || true

      local admin_cert="$org_dir/users/Admin@${org_domain}/msp/signcerts/Admin@${org_domain}-cert.pem"
      if [ -f "$admin_cert" ]; then
        cp "$admin_cert" "$alias_dir/msp/signcerts/"
        local cert_target="$alias_dir/msp/signcerts/$(basename "$admin_cert")"
        sudo chown "$USER":"$USER" "$cert_target" || true
        sudo chmod 644 "$cert_target" || true
        log_info "Certificado Admin sincronizado para alias ${alias}."
      else
        log_warn "Certificado Admin não encontrado para ${org_domain}."
      fi

      local key_dir="$org_dir/users/Admin@${org_domain}/msp/keystore"
      local key_source
      key_source="$(find "$key_dir" -type f -print -quit 2>/dev/null || true)"
      if [ -n "$key_source" ] && [ -f "$key_source" ]; then
        local key_target="$alias_dir/msp/keystore/priv_sk"
        cp "$key_source" "$key_target"
        sudo chown "$USER":"$USER" "$key_target" || true
        sudo chmod 600 "$key_target" || true
        log_info "Chave privada sincronizada para alias ${alias}."
      else
        log_warn "Chave Admin não encontrada para ${org_domain} (dir: ${key_dir})."
      fi

      local tls_source="$org_dir/tlsca/tlsca.${org_domain}-cert.pem"
      if [ -f "$tls_source" ]; then
        cp "$tls_source" "$alias_dir/tls/${alias}-tlsca.pem"
        sudo chown "$USER":"$USER" "$alias_dir/tls/${alias}-tlsca.pem" || true
        sudo chmod 644 "$alias_dir/tls/${alias}-tlsca.pem" || true
      fi
      local orderer_info
      orderer_info="$(resolve_orderer_for_org "$org_domain")"
      local orderer_host="${orderer_info%%;*}"
      local orderer_tls_path="${orderer_info#*;}"
      if [ -z "$orderer_host" ]; then
        orderer_host="$DEFAULT_ORDERER_HOST"
      fi
      if [ -n "$orderer_tls_path" ] && [ -f "$orderer_tls_path" ]; then
        local orderer_bundle
        orderer_bundle="$(basename "$orderer_tls_path")"
        cp "$orderer_tls_path" "$data_dir/${orderer_bundle}"
        sudo chown "$USER":"$USER" "$data_dir/${orderer_bundle}" || true
        sudo chmod 644 "$data_dir/${orderer_bundle}" || true
        cp "$orderer_tls_path" "$alias_dir/tls/${orderer_host}-tlsca.pem"
        sudo chown "$USER":"$USER" "$alias_dir/tls/${orderer_host}-tlsca.pem" || true
        sudo chmod 644 "$alias_dir/tls/${orderer_host}-tlsca.pem" || true
      else
        log_warn "Bundle TLS do orderer não encontrado para domínio ${org_domain}; usando host ${orderer_host} sem atualizar TLS específico."
      fi

      local peer_dir
      peer_dir="$(find "$org_dir/peers" -mindepth 1 -maxdepth 1 -type d | head -n1 2>/dev/null)"
      if [ -z "$peer_dir" ]; then
        log_warn "Nenhum peer encontrado para ${org_domain}; conexão não será gerada."
        continue
      fi
      local peer_host
      peer_host="$(basename "$peer_dir")"

      local peer_tls_container_path="/workspace/data/${alias}/tls/${alias}-tlsca.pem"
      local orderer_tls_container_path="/workspace/data/${alias}/tls/${orderer_host}-tlsca.pem"

      local channels_entries=""
      for channel in "${DISCOVERED_CHANNELS[@]}"; do
        channels_entries+="      \"${channel}\": {\n        \"orderers\": [\"${orderer_host}\"],\n        \"peers\": {\n          \"${peer_host}\": {\n            \"endorsingPeer\": true,\n            \"chaincodeQuery\": true,\n            \"ledgerQuery\": true,\n            \"eventSource\": true\n          }\n        }\n      },\n"
      done
      channels_entries="${channels_entries%,\n}"

      cat > "chaincode-gateway/data/connection-${alias}.json" <<EOF
{
  "name": "${alias}-network",
  "version": "1.0.0",
  "client": {
    "organization": "$(to_msp_id "$org_domain")"
  },
  "organizations": {
    "$(to_msp_id "$org_domain")": {
      "mspid": "$(to_msp_id "$org_domain")",
      "peers": ["${peer_host}"],
      "certificateAuthorities": []
    }
  },
  "peers": {
    "${peer_host}": {
      "url": "grpcs://${peer_host}:7051",
      "tlsCACerts": {
        "path": "${peer_tls_container_path}"
      },
      "grpcOptions": {
        "ssl-target-name-override": "${peer_host}",
        "hostnameOverride": "${peer_host}"
      }
    }
  },
  "orderers": {
    "${orderer_host}": {
      "url": "grpcs://${orderer_host}:7050",
      "tlsCACerts": {
        "path": "${orderer_tls_container_path}"
      },
      "grpcOptions": {
        "ssl-target-name-override": "${orderer_host}",
        "hostnameOverride": "${orderer_host}"
      }
    }
  },
  "channels": {
${channels_entries}
  }
}
EOF
    done
  done
}

fix_couchdb_permissions() {
  local candidates=(
    "$CELLO_STORAGE_PATH/hyperledger/couchdb"
    "$SECONDARY_STORAGE_PATH/hyperledger/couchdb"
  )
  for dir in "${candidates[@]}"; do
    [ -d "$dir" ] || continue
    log_info "Garantindo permissões de escrita para CouchDB em $dir"
    sudo chmod -R 777 "$dir" || log_warn "Não foi possível ajustar permissões em $dir"
  done
}

prepare_build_environment() {
  if [ "$START_ENV_FORCE_CLASSIC_BUILDER" = "1" ]; then
    export DOCKER_BUILDKIT=0
    export BUILDKIT_PROGRESS=plain
    log_info "BuildKit desativado (DOCKER_BUILDKIT=0) para permitir builds offline."
  else
    log_warn "START_ENV_FORCE_CLASSIC_BUILDER=0 detectado: mantendo BuildKit ativo."
  fi
}

build_api_engine_image() {
  local build_ctx="$SCRIPT_DIR/cello-v2/src/api-engine"
  local dockerfile="$build_ctx/Dockerfile"
  local base_image="hyperledger/cello-api-engine"
  local base_image_latest="${base_image}:latest"
  local base_image_source=""
  if [ ! -f "$dockerfile" ]; then
    log_warn "Dockerfile customizado para hyperledger/cello-api-engine ausente; pulando build local."
    return
  fi

  if docker image inspect "$base_image_latest" >/dev/null 2>&1; then
    base_image_source="$base_image_latest"
  elif docker image inspect "$base_image" >/dev/null 2>&1; then
    base_image_source="$base_image"
  fi

  if [ -z "$base_image_source" ] && docker container inspect cello-api-engine >/dev/null 2>&1; then
    local backend_container_image_id=""
    backend_container_image_id="$(docker inspect -f '{{.Image}}' cello-api-engine 2>/dev/null || true)"
    if [ -n "$backend_container_image_id" ]; then
      log_info "Criando base estável local do api-engine a partir do container existente."
      docker tag "$backend_container_image_id" "$base_image" >/dev/null 2>&1 || true
      docker tag "$backend_container_image_id" "$base_image_latest" >/dev/null 2>&1 || true
      base_image_source="$base_image_latest"
    fi
  fi

  if [ -z "$base_image_source" ]; then
    log_warn "Imagem base local do api-engine não encontrada; o rebuild corretivo dependerá de registry externo."
  fi

  log_info "Construindo imagem hyperledger/cello-api-engine com CLI Docker embutido."
  local build_args=()
  if [ "$REGISTRY_AVAILABLE" -eq 1 ] && [ -z "$base_image_source" ]; then
    build_args+=(--pull)
  fi

  if [ -n "$base_image_source" ]; then
    build_args+=(--build-arg "BASE_IMAGE=$base_image_source")
  fi

  if ! docker build "${build_args[@]}" -t hyperledger/cello-api-engine "$build_ctx"; then
    log_warn "Falha ao construir hyperledger/cello-api-engine; mantendo imagem previamente disponível."
    return 1
  else
    log_info "Imagem hyperledger/cello-api-engine atualizada localmente."
    return 0
  fi
}

build_gateway_runtime_image() {
  local gateway_dir="$SCRIPT_DIR/chaincode-gateway"
  local gateway_dockerfile="$gateway_dir/Dockerfile"

  if [ ! -d "$gateway_dir" ]; then
    log_warn "Diretório do runtime do gateway não encontrado: $gateway_dir"
    return
  fi

  if [ ! -f "$gateway_dockerfile" ]; then
    log_warn "Dockerfile do runtime do gateway não encontrado: $gateway_dockerfile"
    return
  fi

  log_info "Construindo imagem runtime do gateway ($COGNUS_APIGATEWAY_IMAGE_PRIMARY) para uso do runbook SSH."
  if docker build -t "$COGNUS_APIGATEWAY_IMAGE_PRIMARY" "$gateway_dir"; then
    docker tag "$COGNUS_APIGATEWAY_IMAGE_PRIMARY" "$COGNUS_APIGATEWAY_IMAGE_LEGACY_ALIAS" >/dev/null 2>&1 || true
    log_info "Imagem runtime do gateway pronta localmente."
    return 0
  else
    log_warn "Falha ao construir $COGNUS_APIGATEWAY_IMAGE_PRIMARY; o provisionamento SSH pode falhar ao seedar o runtime remoto."
    return 1
  fi
}

gateway_runtime_image_ready() {
  docker image inspect "$COGNUS_APIGATEWAY_IMAGE_PRIMARY" >/dev/null 2>&1 || \
    docker image inspect "$COGNUS_APIGATEWAY_IMAGE_LEGACY_ALIAS" >/dev/null 2>&1
}

backend_runtime_tooling_ready() {
  if ! docker container inspect cello-api-engine >/dev/null 2>&1; then
    return 1
  fi

  docker exec cello-api-engine sh -lc 'command -v docker >/dev/null 2>&1 || python -c "import docker,sys; c=docker.from_env(); c.ping(); sys.exit(0)" >/dev/null 2>&1' >/dev/null 2>&1
}

recreate_api_engine_service() {
  local compose_file="$SCRIPT_DIR/cello-v2/bootup/docker-compose-files/docker-compose.dev.yml"

  if [ ! -f "$compose_file" ]; then
    log_error "Arquivo docker-compose.dev.yml não encontrado. Não é possível recriar o cello-api-engine."
    exit 1
  fi

  log_info "Recriando serviço cello-api-engine para aplicar imagem local atualizada."
  if using_legacy_docker_compose; then
    docker rm -f cello-api-engine >/dev/null 2>&1 || true
    "${DOCKER_COMPOSE_CMD[@]}" -f "$compose_file" up -d --no-deps cello-api-engine
    return
  fi

  "${DOCKER_COMPOSE_CMD[@]}" -f "$compose_file" up -d --force-recreate --no-deps cello-api-engine
}

ensure_gateway_runtime_image_ready() {
  if gateway_runtime_image_ready; then
    docker tag "$COGNUS_APIGATEWAY_IMAGE_PRIMARY" "$COGNUS_APIGATEWAY_IMAGE_LEGACY_ALIAS" >/dev/null 2>&1 || true
    return 0
  fi

  log_warn "Imagem runtime do gateway ausente após bootstrap inicial. Tentando rebuild corretivo."
  build_gateway_runtime_image || true

  if gateway_runtime_image_ready; then
    docker tag "$COGNUS_APIGATEWAY_IMAGE_PRIMARY" "$COGNUS_APIGATEWAY_IMAGE_LEGACY_ALIAS" >/dev/null 2>&1 || true
    return 0
  fi

  log_error "Imagem obrigatória do runbook ainda ausente: $COGNUS_APIGATEWAY_IMAGE_PRIMARY"
  log_error "Execute novamente quando o host puder construir/puxar a imagem do chaincode-gateway."
  exit 1
}

ensure_backend_runtime_tooling_ready() {
  if backend_runtime_tooling_ready; then
    log_info "cello-api-engine com tooling Docker operacional para runbooks SSH."
    return 0
  fi

  log_warn "cello-api-engine iniciou sem tooling Docker operacional. Tentando rebuild corretivo do backend."
  build_api_engine_image || true
  recreate_api_engine_service

  if backend_runtime_tooling_ready; then
    log_info "cello-api-engine recuperado com tooling Docker operacional."
    return 0
  fi

  log_error "cello-api-engine segue sem tooling Docker operacional após rebuild corretivo."
  log_error "O bootstrap não vai continuar com runbook SSH quebrado. Verifique o build do backend local."
  exit 1
}

validate_backend_runtime_tooling() {
  if ! docker container inspect cello-api-engine >/dev/null 2>&1; then
    log_warn "Container cello-api-engine não encontrado para validação de tooling Docker."
    return
  fi

  if backend_runtime_tooling_ready; then
    log_info "cello-api-engine com tooling Docker operacional para runbooks SSH."
  else
    log_warn "cello-api-engine sem tooling Docker operacional. Se o runbook SSH falhar por docker_cli_missing_and_sdk_unavailable, execute ./update_env_fast.sh --force-backend-build."
  fi
}

prepare_chaincode_gateway() {
  if ! flag_enabled "$COGNUS_ENABLE_LOCAL_GATEWAY"; then
    return
  fi

  log_info "Preparando chaincode-gateway"
  pushd chaincode-gateway >/dev/null
  if [ -d node_modules ]; then
    log_info "node_modules já presente; pulando npm install."
  else
    npm install --registry=https://registry.npmjs.org/
  fi
  # Garante que contêineres antigos não reutilizem redes removidas.
  "${DOCKER_COMPOSE_CMD[@]}" down --remove-orphans >/dev/null 2>&1 || true
  "${DOCKER_COMPOSE_CMD[@]}" up -d --build --force-recreate
  popd >/dev/null
}

main() {
  local force_zero_clean=0
  local -a orchestrator_services=(
    cello-postgres
    cello-api-engine
    cello-dashboard
  )
  local -a required_images=("${BASE_IMAGES[@]}")
  for arg in "$@"; do
    case "$arg" in
      --clean|--zero|--zero-clean)
        force_zero_clean=1
        ;;
    esac
  done

  if flag_enabled "$COGNUS_ENABLE_DOCKER_AGENT"; then
    orchestrator_services+=(cello-docker-agent)
  fi

  if flag_enabled "$COGNUS_ENABLE_LOCAL_GATEWAY"; then
    required_images+=("${OPTIONAL_GATEWAY_IMAGES[@]}")
  fi

  if flag_enabled "$COGNUS_ENABLE_LOCAL_PEER_RUNTIME"; then
    required_images+=("${OPTIONAL_PEER_RUNTIME_IMAGES[@]}")
  fi

  log_info "Iniciando start_env em $SCRIPT_DIR"
  ensure_command docker Docker
  ensure_command npm Node.js

  if command -v make >/dev/null 2>&1; then
    MAKE_AVAILABLE=1
  else
    log_warn "Dependência opcional 'make' não encontrada; usando fallback direto com Docker para os builds suportados."
  fi

  select_docker_compose
  ensure_docker_daemon_available

  if [ "$force_zero_clean" = "1" ]; then
    log_info "Flag de limpeza detectada (--clean/--zero). Executando limpeza completa antes do start."
    if [ -f "$SCRIPT_DIR/clean_env_zero.sh" ]; then
      bash "$SCRIPT_DIR/clean_env_zero.sh" --keep-images
    else
      bash "$SCRIPT_DIR/clean_env.sh"
    fi
  fi

  prepare_build_environment

  configure_docker_ipv4

  if [ "$START_ENV_FORCE_OFFLINE" = "1" ]; then
    log_warn "Modo offline forçado via START_ENV_FORCE_OFFLINE=1."
  elif check_registry_connectivity; then
    REGISTRY_AVAILABLE=1
    log_info "Conectividade com Docker Hub validada (IPv4)."
  else
    log_warn "Sem conectividade com Docker Hub; usarei apenas artefatos locais."
  fi
  for img in "${required_images[@]}"; do
    ensure_image "$img"
  done

  if [ "${#MISSING_IMAGES[@]}" -gt 0 ]; then
    log_error "As seguintes imagens ainda não foram importadas: ${MISSING_IMAGES[*]}"
    log_error "Disponibilize os arquivos .tar correspondentes em qualquer diretório listado em IMAGE_TAR_DIRS e execute novamente."
    exit 1
  fi

  ensure_cello_network
  if flag_enabled "$COGNUS_ENABLE_LOCAL_PEER_RUNTIME"; then
    ensure_chaincode_external_network
  fi

  prepare_permissions \
    "${CELLO_STORAGE_PATH}" \
    "$SECONDARY_STORAGE_PATH"

  if flag_enabled "$COGNUS_ENABLE_LOCAL_GATEWAY" || flag_enabled "$COGNUS_ENABLE_LOCAL_PEER_RUNTIME"; then
    prepare_permissions chaincode-gateway/data

    # Automatiza configuração de DNS/hosts para todos os nomes relevantes
    log_info "Configurando /etc/hosts para nomes de containers Hyperledger Fabric."
    host_ip="127.0.0.1"
    mapfile -t ORDERER_HOSTS < <(discover_orderer_hosts)
    if [ ${#ORDERER_HOSTS[@]} -eq 0 ]; then
      log_warn "Nenhum orderer encontrado em crypto-config; pulando atualização de hosts para orderers."
    else
      for orderer_name in "${ORDERER_HOSTS[@]}"; do
        if ! grep -q "[[:space:]]$orderer_name" /etc/hosts; then
          log_info "Adicionando $orderer_name em /etc/hosts."
          echo "$host_ip $orderer_name" | sudo tee -a /etc/hosts >/dev/null
        fi
      done
    fi
    mapfile -t PEER_HOSTS < <(discover_peer_hosts)
    if [ ${#PEER_HOSTS[@]} -eq 0 ]; then
      log_warn "Nenhum peer encontrado em crypto-config; pulando atualização de hosts para peers."
    else
      for peer_name in "${PEER_HOSTS[@]}"; do
        if ! grep -q "[[:space:]]$peer_name" /etc/hosts; then
          log_info "Adicionando $peer_name em /etc/hosts."
          echo "$host_ip $peer_name" | sudo tee -a /etc/hosts >/dev/null
        fi
      done
    fi
    # Corrige permissões de arquivos problemáticos criados por container para todas orgs detectadas
    for org_dir in chaincode-gateway/data/*; do
      [ -d "$org_dir" ] || continue
      if [ -d "$org_dir/msp/signcerts" ]; then
        for f in "$org_dir/msp/signcerts"/*; do
          [ -e "$f" ] || continue
          sudo chown "$USER":"$USER" "$f" || true
          sudo chmod 644 "$f" || true
        done
      fi
      if [ -d "$org_dir/msp/keystore" ]; then
        for f in "$org_dir/msp/keystore"/*; do
          [ -e "$f" ] || continue
          sudo chown "$USER":"$USER" "$f" || true
          sudo chmod 600 "$f" || true
        done
      fi
      if [ -d "$org_dir/tls" ]; then
        for f in "$org_dir/tls"/*; do
          [ -e "$f" ] || continue
          sudo chown "$USER":"$USER" "$f" || true
          sudo chmod 644 "$f" || true
        done
      fi
    done

    mapfile -t DISCOVERED_CHANNELS < <(discover_channels)
    sync_chaincode_gateway_artifacts
    sync_gateway_alias_artifacts
    if [ ! -f "chaincode-gateway/data/fabric-tlscas.pem" ] && [ -f "chaincode-gateway/templates/fabric-tlscas.pem" ]; then
      cp "chaincode-gateway/templates/fabric-tlscas.pem" "chaincode-gateway/data/fabric-tlscas.pem"
      sudo chown "$USER":"$USER" "chaincode-gateway/data/fabric-tlscas.pem" || true
      sudo chmod 644 "chaincode-gateway/data/fabric-tlscas.pem" || true
    fi
    fix_couchdb_permissions
  fi

  if flag_enabled "$COGNUS_ENABLE_LOCAL_GATEWAY"; then
    log_info "Gerando identities.<org>.json para cada organização detectada."
    mkdir -p automation/resources
    rm -f automation/resources/identities.*.json
    for gw_org_dir in chaincode-gateway/data/*; do
      [ -d "$gw_org_dir" ] || continue
      org_name="$(basename "$gw_org_dir")"
      cert_file="$(find "$gw_org_dir/msp/signcerts" -maxdepth 1 -type f -name 'Admin@*.pem' -print -quit 2>/dev/null)"
      key_file="$(find "$gw_org_dir/msp/keystore" -maxdepth 1 -type f -print -quit 2>/dev/null)"
      ccp_path="/workspace/data/connection-${org_name}.json"
      if [ -z "$cert_file" ] || [ -z "$key_file" ]; then
        log_warn "Cert ou key não encontrados para org ${org_name}, identities.${org_name}.json não será gerado."
        continue
      fi
      if [ ! -f "chaincode-gateway/data/connection-${org_name}.json" ]; then
        log_warn "Arquivo connection-${org_name}.json ausente; pulei geração de identidade."
        continue
      fi
      cert_filename="$(basename "$cert_file")"
      key_filename="$(basename "$key_file")"
      org_domain="${cert_filename#Admin@}"
      org_domain="${org_domain%-cert.pem}"
      msp_id="$(to_msp_id "$org_domain")"
      cert_path="/workspace/data/${org_name}/msp/signcerts/${cert_filename}"
      key_path="/workspace/data/${org_name}/msp/keystore/${key_filename}"

      local channels_json="["
      for channel in "${DISCOVERED_CHANNELS[@]}"; do
        channels_json+="\"${channel}\",";
      done
      channels_json="${channels_json%,}"
      channels_json="${channels_json}]"
      if [ "$channels_json" = "[]" ]; then
        channels_json="[\"stcs-channel\"]"
      fi

      cat > "automation/resources/identities.${org_name}.json" <<EOF
{
  "${org_name}": {
    "ccpPath": "${ccp_path}",
    "certPath": "${cert_path}",
    "channels": ${channels_json},
    "discoveryAsLocalhost": false,
    "keyPath": "${key_path}",
    "mspId": "${msp_id}"
  }
}
EOF
      log_info "Arquivo identities.${org_name}.json gerado."
    done

    log_info "Gerando identities.json multi-org para o chaincode-gateway."
    shopt -s nullglob
    identity_files=(automation/resources/identities.*.json)
    shopt -u nullglob
    if [ ${#identity_files[@]} -eq 0 ]; then
      log_warn "Nenhum identities.*.json encontrado em automation/resources/. identities.json multi-org não será gerado."
      echo '{}' > chaincode-gateway/data/identities.json
    else
      jq -s 'reduce .[] as $item ({}; . * $item)' "${identity_files[@]}" > chaincode-gateway/data/identities.json
      log_info "Arquivo identities.json multi-org gerado para o gateway."
      first_identity_key="$(jq -r 'keys[0] // empty' chaincode-gateway/data/identities.json)"
      if [ -n "$first_identity_key" ]; then
        jq --arg fk "$first_identity_key" '.default = .[$fk]' chaincode-gateway/data/identities.json > chaincode-gateway/data/identities.tmp && mv chaincode-gateway/data/identities.tmp chaincode-gateway/data/identities.json
        log_info "Entrada 'default' configurada com base em $first_identity_key."
      fi
      touch chaincode-gateway/data/identities.json
    fi
  fi

  run_make cello-v2 dashboard
  run_make cello-v2 api-engine
  if flag_enabled "$COGNUS_ENABLE_DOCKER_AGENT"; then
    run_make cello-v2 docker-rest-agent
  fi
  if flag_enabled "$COGNUS_ENABLE_LOCAL_PEER_RUNTIME"; then
    run_make cello-v2 fabric
  fi

  if flag_enabled "$COGNUS_ENABLE_LOCAL_PEER_RUNTIME"; then
    log_info "Executando join do canal para todas orgs/orderers detectados."
    for channel_id in "${DISCOVERED_CHANNELS[@]}"; do
      block_dir="${CELLO_STORAGE_PATH}/hyperledger/fabric"
      if [ ! -d "$block_dir" ]; then
        alt_block_dir="$SECONDARY_STORAGE_PATH/hyperledger/fabric"
        if [ -d "$alt_block_dir" ]; then
          block_dir="$alt_block_dir"
        else
          log_warn "Nenhum diretório hyperledger/fabric encontrado para executar join automático."
          continue
        fi
      fi
      config_block="$(find "$CELLO_STORAGE_PATH" "$SECONDARY_STORAGE_PATH" -maxdepth 2 -type f -name "${channel_id}.block" -print -quit 2>/dev/null)"
      if [ -z "$config_block" ]; then
        log_warn "Block ${channel_id}.block não encontrado; pulando join automático para este canal."
        continue
      fi
      for orderer_dir in "$block_dir"/*; do
        [ -d "$orderer_dir" ] || continue
        orderer_name="$(basename "$orderer_dir")"
        ca_file="$orderer_dir/tls/ca.crt"
        for org_dir in chaincode-gateway/data/*; do
          [ -d "$org_dir" ] || continue
          org_name="$(basename "$org_dir")"
          client_cert="$(find "$org_dir/msp/signcerts" -maxdepth 1 -type f -name 'Admin@*.pem' -print -quit 2>/dev/null)"
          client_key="$(find "$org_dir/msp/keystore" -maxdepth 1 -type f -print -quit 2>/dev/null)"
          orderer_host="${orderer_name}:7053"
          if [ -f "$ca_file" ] && [ -f "$client_cert" ] && [ -f "$client_key" ] && [ -f "$config_block" ]; then
            log_info "Join do canal ${channel_id} para org ${org_name} e orderer ${orderer_name}."
            if run_osnadmin_channel_join "$channel_id" "$config_block" "$orderer_host" "$ca_file" "$client_cert" "$client_key"; then
              log_info "Canal $channel_id joined com sucesso para org $org_name e orderer $orderer_name."
            else
              log_error "Falha ao join do canal para org $org_name e orderer $orderer_name. Verifique conectividade TLS/osnadmin."
            fi
          else
            log_warn "Artefatos ausentes para org $org_name ou orderer $orderer_name no canal $channel_id. Pulando join."
          fi
        done
      done
    done
  fi

  build_api_engine_image || true
  build_gateway_runtime_image || true
  prepare_chaincode_gateway

  if flag_enabled "$COGNUS_ENABLE_LOCAL_PEER_RUNTIME"; then
    log_info "Subindo peers e chaincode com CouchDB (compose resolvido dinamicamente)"
    peer_runtime_compose_file="$(resolve_peer_runtime_compose_file || true)"
    if [ -n "$peer_runtime_compose_file" ] && [ -f "$peer_runtime_compose_file" ]; then
      log_info "Compose peers/chaincode selecionado: $peer_runtime_compose_file"
      peer_runtime_compose_dir="$(dirname "$peer_runtime_compose_file")"
      if using_legacy_docker_compose; then
        env -u COMPOSE_FILE "${DOCKER_COMPOSE_CMD[@]}" --project-directory "$peer_runtime_compose_dir" -f "$peer_runtime_compose_file" up -d --remove-orphans
      else
        env -u COMPOSE_FILE "${DOCKER_COMPOSE_CMD[@]}" --project-directory "$peer_runtime_compose_dir" -f "$peer_runtime_compose_file" up -d --force-recreate --remove-orphans
      fi
    else
      log_warn "Nenhum docker-compose-couch.yaml foi encontrado. A inicializacao local vai continuar sem subir peers/chaincode."
      log_warn "Se quiser esse runtime local, defina COGNUS_PEER_RUNTIME_COMPOSE_FILE=/caminho/para/docker-compose-couch.yaml."
    fi
  fi

  log_info "(Pré-build) Subindo núcleo do orquestrador (cello-v2/bootup/docker-compose-files/docker-compose.dev.yml)"
  if [ -f "cello-v2/bootup/docker-compose-files/docker-compose.dev.yml" ]; then
    compose_up_detached "cello-v2/bootup/docker-compose-files/docker-compose.dev.yml" "${orchestrator_services[@]}"
    log_info "Núcleo do orquestrador (pré-build) iniciado."
  else
    log_error "Arquivo docker-compose.dev.yml não encontrado. Não é possível subir o orquestrador."
  fi

  ensure_gateway_runtime_image_ready
  ensure_backend_runtime_tooling_ready
  validate_backend_runtime_tooling
  enforce_restart_policies

  log_info "Ambiente inicializado. Núcleo do orquestrador disponível em http://localhost:8081"
}

ensure_cello_network() {
  local network="cello-net"
  if docker network inspect "$network" >/dev/null 2>&1; then
    log_info "Rede docker '$network' já existe."
    return
  fi

  log_info "Criando rede docker '$network'."
  if ! docker network create "$network" >/dev/null; then
    log_error "Não foi possível criar a rede '$network'."
    exit 1
  fi
}

ensure_chaincode_external_network() {
  local network="$CHAINCODE_EXTERNAL_NETWORK"
  if docker network inspect "$network" >/dev/null 2>&1; then
    log_info "Rede docker externa '$network' já existe."
    return
  fi

  log_info "Criando rede docker externa '$network' (pré-requisito do compose de peers/chaincode)."
  if ! docker network create "$network" >/dev/null; then
    log_error "Não foi possível criar a rede externa '$network'."
    exit 1
  fi
}

enforce_restart_policies() {
  local -a static_targets=(
    cello-dashboard
    cello-api-engine
    cello-postgres
  )
  local -a dynamic_targets=()
  local name
  local updated=0

  if flag_enabled "$COGNUS_ENABLE_DOCKER_AGENT"; then
    static_targets+=(cello-docker-agent)
  fi

  if flag_enabled "$COGNUS_ENABLE_LOCAL_GATEWAY"; then
    static_targets+=(chaincode-gateway fabric-auto-operator)
  fi

  if flag_enabled "$COGNUS_ENABLE_LOCAL_PEER_RUNTIME"; then
    while IFS= read -r name; do
      [ -n "$name" ] || continue
      dynamic_targets+=("$name")
    done < <(docker ps -a --format '{{.Names}}' | grep -E '^(orderer[0-9]*\..+|peer[0-9]+\..+|couchdb[0-9]*$|ca_[A-Za-z0-9._-]+)$' || true)
  fi

  for name in "${static_targets[@]}" "${dynamic_targets[@]}"; do
    if docker update --restart unless-stopped "$name" >/dev/null 2>&1; then
      updated=$((updated + 1))
    fi
  done

  log_info "Política de reinício persistente validada para $updated container(es)."
}

main "$@"
