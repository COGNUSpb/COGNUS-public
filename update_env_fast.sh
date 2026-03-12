#!/usr/bin/env bash
# Atualização rápida do ambiente COGNUSpb sem rebuild completo.
# Use para aplicar mudanças leves (config, scripts, compose, reinício de serviços).

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

DASHBOARD_DIR="cello-v2/src/dashboard"
DASHBOARD_CONTAINER_NAME="cello-dashboard"
DASHBOARD_NGINX_CONF="cello-v2/dashboard-nginx.conf"
DASHBOARD_URL="http://localhost:8081"
STATE_DIR=".state"
STATE_FILE="$STATE_DIR/update_env_fast_dashboard_last_commit"
EXPECTED_DASHBOARD_ASSET=""
FORCE_DASHBOARD_BUILD=0
SKIP_DASHBOARD_BUILD=0

BACKEND_DIR="cello-v2/src/api-engine"
BACKEND_COMPOSE_FILE="cello-v2/bootup/docker-compose-files/docker-compose.dev.yml"
BACKEND_SERVICE="cello-api-engine"
BACKEND_IMAGE="hyperledger/cello-api-engine"
BACKEND_BUILD_BASE_TAG="${BACKEND_IMAGE}:build-base"
BACKEND_DOCKERFILE="cello-v2/src/api-engine/Dockerfile"
BACKEND_BUILD_CONTEXT="cello-v2/src/api-engine"
BACKEND_STATE_FILE="$STATE_DIR/update_env_fast_backend_last_commit"
FORCE_BACKEND_BUILD=0
SKIP_BACKEND_BUILD=0
BACKEND_REBUILT=0
COGNUS_APIGATEWAY_IMAGE_PRIMARY="cognus/chaincode-gateway:latest"
COGNUS_APIGATEWAY_IMAGE_LEGACY_ALIAS="chaincode-gateway:latest"
GATEWAY_DIR="chaincode-gateway"
GATEWAY_DOCKERFILE="$GATEWAY_DIR/Dockerfile"
GATEWAY_STATE_FILE="$STATE_DIR/update_env_fast_gateway_last_commit"
COGNUS_ENABLE_LOCAL_GATEWAY="${COGNUS_ENABLE_LOCAL_GATEWAY:-0}"
COGNUS_ENABLE_LOCAL_PEER_RUNTIME="${COGNUS_ENABLE_LOCAL_PEER_RUNTIME:-0}"
COGNUS_ENABLE_DOCKER_AGENT="${COGNUS_ENABLE_DOCKER_AGENT:-0}"

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

parse_args() {
  while [[ $# -gt 0 ]]; do
    case "$1" in
      --force-dashboard-build)
        FORCE_DASHBOARD_BUILD=1
        ;;
      --skip-dashboard-build)
        SKIP_DASHBOARD_BUILD=1
        ;;
      --force-backend-build)
        FORCE_BACKEND_BUILD=1
        ;;
      --skip-backend-build)
        SKIP_BACKEND_BUILD=1
        ;;
      -h|--help)
        cat <<'EOF'
Uso: ./update_env_fast.sh [opções]

Opções:
  --force-dashboard-build  Força rebuild/sync do frontend do dashboard
  --skip-dashboard-build   Pula rebuild/sync do frontend do dashboard
  --force-backend-build    Força build local da imagem do backend e recriação do api-engine
  --skip-backend-build     Pula build local do backend
  -h, --help               Exibe esta ajuda
EOF
        exit 0
        ;;
      *)
        log_error "Opção desconhecida: $1"
        exit 1
        ;;
    esac
    shift
  done

  if [[ "$FORCE_DASHBOARD_BUILD" -eq 1 && "$SKIP_DASHBOARD_BUILD" -eq 1 ]]; then
    log_error "Use apenas uma opção: --force-dashboard-build OU --skip-dashboard-build"
    exit 1
  fi

  if [[ "$FORCE_BACKEND_BUILD" -eq 1 && "$SKIP_BACKEND_BUILD" -eq 1 ]]; then
    log_error "Use apenas uma opção: --force-backend-build OU --skip-backend-build"
    exit 1
  fi
}

ensure_command() {
  local cmd="$1"
  local hint="${2:-$1}"
  if ! command -v "$cmd" >/dev/null 2>&1; then
    log_error "Comando '$cmd' não encontrado. Instale/verifique: $hint"
    exit 1
  fi
}

ensure_docker_daemon() {
  if docker info >/dev/null 2>&1; then
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
    log_error "Docker daemon indisponível. Inicie o Docker e tente novamente."
    exit 1
  fi
}

select_compose() {
  if docker compose version >/dev/null 2>&1; then
    COMPOSE=(docker compose)
    return
  fi

  if command -v docker-compose >/dev/null 2>&1; then
    COMPOSE=(docker-compose)
    return
  fi

  log_error "Docker Compose não encontrado (v2 plugin ou docker-compose)."
  exit 1
}

ensure_network() {
  local network="$1"
  if docker network inspect "$network" >/dev/null 2>&1; then
    return
  fi
  log_info "Criando rede '$network'."
  docker network create "$network" >/dev/null
}

up_stack() {
  local compose_file="$1"
  local description="$2"

  if [ ! -f "$compose_file" ]; then
    log_warn "Arquivo não encontrado: $compose_file (pulando $description)."
    return
  fi

  log_info "Subindo $description"
  "${COMPOSE[@]}" -f "$compose_file" up -d --remove-orphans
}

up_peer_runtime_stack() {
  local compose_file="$1"
  local compose_dir
  local compose_name
  local base_compose_file=""
  local fabric_image_tag="${IMAGE_TAG:-${FABRIC_IMAGE_TAG:-2.5}}"

  compose_dir="$(dirname "$compose_file")"
  compose_name="$(basename "$compose_file")"

  case "$compose_name" in
    docker-compose-couch.yaml)
      base_compose_file="$compose_dir/docker-compose-test-net.yaml"
      ;;
    docker-compose-couch-org.yaml)
      base_compose_file="$compose_dir/docker-compose-test-net-org.yaml"
      ;;
  esac

  if [ -n "$base_compose_file" ] && [ -f "$base_compose_file" ]; then
    log_info "Subindo peers/chaincode (base + CouchDB override)"
    IMAGE_TAG="$fabric_image_tag" "${COMPOSE[@]}" -f "$base_compose_file" -f "$compose_file" up -d --remove-orphans
    return
  fi

  up_stack "$compose_file" "peers/chaincode (CouchDB)"
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
  discovered_file="$(find "$SCRIPT_DIR" -maxdepth 8 -type f -path '*/fabric/docker/docker-compose-couch.yaml' 2>/dev/null | head -n 1 || true)"
  if [ -n "$discovered_file" ] && [ -f "$discovered_file" ]; then
    printf '%s\n' "$discovered_file"
    return 0
  fi

  return 1
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

show_status() {
  local compose_file="$1"
  local description="$2"

  if [ ! -f "$compose_file" ]; then
    return
  fi

  log_info "Status: $description"
  "${COMPOSE[@]}" -f "$compose_file" ps || true
}

ensure_frontend_dependencies() {
  ensure_command node Node.js
  ensure_command npm npm

  if [ ! -d "$DASHBOARD_DIR/node_modules" ]; then
    log_info "Instalando dependências do dashboard (npm install --legacy-peer-deps)."
    (cd "$DASHBOARD_DIR" && npm install --legacy-peer-deps)
  fi
}

dashboard_container_running() {
  docker ps --format '{{.Names}}' | grep -qx "$DASHBOARD_CONTAINER_NAME"
}

dashboard_has_uncommitted_changes() {
  if ! command -v git >/dev/null 2>&1; then
    return 1
  fi

  git -C "$SCRIPT_DIR" rev-parse --is-inside-work-tree >/dev/null 2>&1 || return 1
  git -C "$SCRIPT_DIR" status --porcelain -- "$DASHBOARD_DIR" | grep -q .
}

dashboard_latest_commit() {
  git -C "$SCRIPT_DIR" log -1 --format=%H -- "$DASHBOARD_DIR" 2>/dev/null || true
}

dashboard_needs_build() {
  if [[ "$SKIP_DASHBOARD_BUILD" -eq 1 ]]; then
    return 1
  fi

  if [[ "$FORCE_DASHBOARD_BUILD" -eq 1 ]]; then
    return 0
  fi

  if dashboard_has_uncommitted_changes; then
    return 0
  fi

  if ! command -v git >/dev/null 2>&1; then
    return 1
  fi

  local latest_commit
  latest_commit="$(dashboard_latest_commit)"
  [ -n "$latest_commit" ] || return 1

  if [ ! -f "$STATE_FILE" ]; then
    return 0
  fi

  local saved_commit
  saved_commit="$(cat "$STATE_FILE" 2>/dev/null || true)"
  [ "$latest_commit" != "$saved_commit" ]
}

backend_has_uncommitted_changes() {
  if ! command -v git >/dev/null 2>&1; then
    return 1
  fi

  git -C "$SCRIPT_DIR" rev-parse --is-inside-work-tree >/dev/null 2>&1 || return 1
  git -C "$SCRIPT_DIR" status --porcelain -- "$BACKEND_DIR" | grep -q .
}

backend_latest_commit() {
  git -C "$SCRIPT_DIR" log -1 --format=%H -- "$BACKEND_DIR" 2>/dev/null || true
}

backend_needs_build() {
  if [[ "$SKIP_BACKEND_BUILD" -eq 1 ]]; then
    return 1
  fi

  if [[ "$FORCE_BACKEND_BUILD" -eq 1 ]]; then
    return 0
  fi

  if backend_has_uncommitted_changes; then
    return 0
  fi

  if ! command -v git >/dev/null 2>&1; then
    return 1
  fi

  local latest_commit
  latest_commit="$(backend_latest_commit)"
  [ -n "$latest_commit" ] || return 1

  if [ ! -f "$BACKEND_STATE_FILE" ]; then
    return 0
  fi

  local saved_commit
  saved_commit="$(cat "$BACKEND_STATE_FILE" 2>/dev/null || true)"
  [ "$latest_commit" != "$saved_commit" ]
}

record_backend_state() {
  if ! command -v git >/dev/null 2>&1; then
    return
  fi

  local latest_commit
  latest_commit="$(backend_latest_commit)"
  [ -n "$latest_commit" ] || return

  mkdir -p "$STATE_DIR"
  printf '%s\n' "$latest_commit" > "$BACKEND_STATE_FILE"
}

gateway_has_uncommitted_changes() {
  if ! command -v git >/dev/null 2>&1; then
    return 1
  fi

  git -C "$SCRIPT_DIR" rev-parse --is-inside-work-tree >/dev/null 2>&1 || return 1
  git -C "$SCRIPT_DIR" status --porcelain -- "$GATEWAY_DIR" | grep -q .
}

gateway_latest_commit() {
  git -C "$SCRIPT_DIR" log -1 --format=%H -- "$GATEWAY_DIR" 2>/dev/null || true
}

gateway_needs_build() {
  if ! [ -d "$GATEWAY_DIR" ]; then
    return 1
  fi

  if ! docker image inspect "$COGNUS_APIGATEWAY_IMAGE_PRIMARY" >/dev/null 2>&1 && \
     ! docker image inspect "$COGNUS_APIGATEWAY_IMAGE_LEGACY_ALIAS" >/dev/null 2>&1; then
    return 0
  fi

  if gateway_has_uncommitted_changes; then
    return 0
  fi

  if ! command -v git >/dev/null 2>&1; then
    return 1
  fi

  local latest_commit
  latest_commit="$(gateway_latest_commit)"
  [ -n "$latest_commit" ] || return 1

  if [ ! -f "$GATEWAY_STATE_FILE" ]; then
    return 0
  fi

  local saved_commit
  saved_commit="$(cat "$GATEWAY_STATE_FILE" 2>/dev/null || true)"
  [ "$latest_commit" != "$saved_commit" ]
}

record_gateway_state() {
  if ! command -v git >/dev/null 2>&1; then
    return
  fi

  local latest_commit
  latest_commit="$(gateway_latest_commit)"
  [ -n "$latest_commit" ] || return

  mkdir -p "$STATE_DIR"
  printf '%s\n' "$latest_commit" > "$GATEWAY_STATE_FILE"
}

build_gateway_image() {
  if [ ! -d "$GATEWAY_DIR" ]; then
    log_warn "Diretório do gateway não encontrado: $GATEWAY_DIR"
    return
  fi

  if [ ! -f "$GATEWAY_DOCKERFILE" ]; then
    log_warn "Dockerfile do gateway não encontrado: $GATEWAY_DOCKERFILE"
    return
  fi

  log_info "Build da imagem runtime do gateway ($COGNUS_APIGATEWAY_IMAGE_PRIMARY) para uso do runbook SSH."
  docker build -t "$COGNUS_APIGATEWAY_IMAGE_PRIMARY" "$GATEWAY_DIR"
  docker tag "$COGNUS_APIGATEWAY_IMAGE_PRIMARY" "$COGNUS_APIGATEWAY_IMAGE_LEGACY_ALIAS" >/dev/null 2>&1 || true
  record_gateway_state
}

build_backend_image() {
  if [ ! -d "$BACKEND_DIR" ]; then
    log_warn "Diretório do backend não encontrado: $BACKEND_DIR"
    return
  fi

  if [ ! -f "$BACKEND_DOCKERFILE" ]; then
    log_warn "Dockerfile do backend não encontrado: $BACKEND_DOCKERFILE"
    return
  fi

  if [ ! -d "$BACKEND_BUILD_CONTEXT" ]; then
    log_warn "Contexto de build do backend não encontrado: $BACKEND_BUILD_CONTEXT"
    return
  fi

  local base_image="$BACKEND_BUILD_BASE_TAG"

  if ! docker image inspect "$base_image" >/dev/null 2>&1; then
    if docker container inspect "$BACKEND_SERVICE" >/dev/null 2>&1; then
      local backend_container_image_id
      backend_container_image_id="$(docker inspect -f '{{.Image}}' "$BACKEND_SERVICE" 2>/dev/null || true)"
      if [ -n "$backend_container_image_id" ]; then
        log_info "Criando base estável do backend em '$base_image' a partir do container '$BACKEND_SERVICE'."
        docker tag "$backend_container_image_id" "$base_image" >/dev/null 2>&1 || true
      fi
    fi
  fi

  if ! docker image inspect "$base_image" >/dev/null 2>&1; then
    log_warn "Base estável '$base_image' indisponível; fallback para '$BACKEND_IMAGE'."
    base_image="$BACKEND_IMAGE"
  fi

  log_info "Build do backend ($BACKEND_IMAGE) via Dockerfile local (BASE_IMAGE=$base_image)."
  docker build -f "$BACKEND_DOCKERFILE" --build-arg "BASE_IMAGE=$base_image" -t "$BACKEND_IMAGE" "$BACKEND_BUILD_CONTEXT"
  BACKEND_REBUILT=1
  record_backend_state
}

recreate_backend_if_rebuilt() {
  if [[ "$BACKEND_REBUILT" -ne 1 ]]; then
    return
  fi

  if [ ! -f "$BACKEND_COMPOSE_FILE" ]; then
    log_warn "Compose do backend não encontrado: $BACKEND_COMPOSE_FILE (recriação ignorada)."
    return
  fi

  log_info "Recriando serviço '$BACKEND_SERVICE' para aplicar nova imagem local."
  local recreate_output
  recreate_output="$({ "${COMPOSE[@]}" -f "$BACKEND_COMPOSE_FILE" up -d --force-recreate "$BACKEND_SERVICE"; } 2>&1)" || {
    if grep -qi "max depth exceeded" <<<"$recreate_output"; then
      log_warn "Recreate do '$BACKEND_SERVICE' falhou por limite de camadas Docker (max depth). Aplicando fallback via hot-sync no container ativo."
      if docker container inspect "$BACKEND_SERVICE" >/dev/null 2>&1; then
        docker cp "$BACKEND_BUILD_CONTEXT/." "$BACKEND_SERVICE:/var/www/server"
        docker restart "$BACKEND_SERVICE" >/dev/null 2>&1 || true
        log_info "Fallback aplicado: código sincronizado e container '$BACKEND_SERVICE' reiniciado."
        return
      fi
      log_error "Fallback indisponível: container '$BACKEND_SERVICE' não existe para hot-sync."
    fi
    printf '%s\n' "$recreate_output" >&2
    exit 1
  }
  printf '%s\n' "$recreate_output"
}

ensure_cognus_runtime_images() {
  local tagged_any=0

  if docker image inspect "$COGNUS_APIGATEWAY_IMAGE_PRIMARY" >/dev/null 2>&1; then
    docker tag "$COGNUS_APIGATEWAY_IMAGE_PRIMARY" "$COGNUS_APIGATEWAY_IMAGE_LEGACY_ALIAS" >/dev/null 2>&1 || true
    tagged_any=1
  fi

  if docker image inspect "$COGNUS_APIGATEWAY_IMAGE_LEGACY_ALIAS" >/dev/null 2>&1; then
    docker tag "$COGNUS_APIGATEWAY_IMAGE_LEGACY_ALIAS" "$COGNUS_APIGATEWAY_IMAGE_PRIMARY" >/dev/null 2>&1 || true
    tagged_any=1
  fi

  if [[ "$tagged_any" -eq 1 ]]; then
    log_info "Tag de runtime COGNUS garantida: $COGNUS_APIGATEWAY_IMAGE_PRIMARY"
  else
    log_warn "Imagem de runtime COGNUS ainda ausente localmente: $COGNUS_APIGATEWAY_IMAGE_PRIMARY"
  fi
}

backend_runtime_tooling_ready() {
  if ! docker container inspect "$BACKEND_SERVICE" >/dev/null 2>&1; then
    return 1
  fi

  docker exec "$BACKEND_SERVICE" sh -lc 'command -v docker >/dev/null 2>&1 || python -c "import docker,sys; c=docker.from_env(); c.ping(); sys.exit(0)" >/dev/null 2>&1' >/dev/null 2>&1
}

record_dashboard_state() {
  if ! command -v git >/dev/null 2>&1; then
    return
  fi

  local latest_commit
  latest_commit="$(dashboard_latest_commit)"
  [ -n "$latest_commit" ] || return

  mkdir -p "$STATE_DIR"
  printf '%s\n' "$latest_commit" > "$STATE_FILE"
}

build_and_sync_dashboard() {
  if [ ! -d "$DASHBOARD_DIR" ]; then
    log_warn "Diretório do dashboard não encontrado: $DASHBOARD_DIR"
    return
  fi

  if ! dashboard_container_running; then
    log_warn "Container '$DASHBOARD_CONTAINER_NAME' não está em execução; pulando sync de frontend."
    return
  fi

  ensure_frontend_dependencies

  log_info "Build do dashboard (npm run build)."
  (cd "$DASHBOARD_DIR" && npm run build)

  EXPECTED_DASHBOARD_ASSET="$(cd "$DASHBOARD_DIR/dist" && ls -1 umi.*.js 2>/dev/null | head -n 1 || true)"
  if [ -z "$EXPECTED_DASHBOARD_ASSET" ]; then
    log_warn "Não foi possível identificar o bundle umi.*.js para validação pós-sync."
  fi

  log_info "Sincronizando assets para o container '$DASHBOARD_CONTAINER_NAME'."
  docker exec "$DASHBOARD_CONTAINER_NAME" sh -lc 'rm -rf /usr/share/nginx/html/*'
  docker cp "$DASHBOARD_DIR/dist/." "$DASHBOARD_CONTAINER_NAME:/usr/share/nginx/html/"

  record_dashboard_state
}

sync_dashboard_nginx_config() {
  if [ ! -f "$DASHBOARD_NGINX_CONF" ]; then
    log_warn "Configuração nginx do dashboard não encontrada: $DASHBOARD_NGINX_CONF"
    return
  fi

  if ! dashboard_container_running; then
    log_warn "Container '$DASHBOARD_CONTAINER_NAME' não está em execução; pulando sync do nginx."
    return
  fi

  log_info "Aplicando configuração nginx do dashboard no container '$DASHBOARD_CONTAINER_NAME'."
  docker cp "$DASHBOARD_NGINX_CONF" "$DASHBOARD_CONTAINER_NAME:/etc/nginx/conf.d/default.conf"
  docker exec "$DASHBOARD_CONTAINER_NAME" sh -lc 'nginx -t && nginx -s reload'
}

validate_dashboard_serving() {
  local homepage
  homepage="$(curl -fsS "$DASHBOARD_URL/" 2>/dev/null || true)"

  if [ -z "$homepage" ]; then
    log_warn "Dashboard ainda não respondeu em $DASHBOARD_URL (pode estar subindo)."
    return
  fi

  if [ -n "$EXPECTED_DASHBOARD_ASSET" ]; then
    if grep -q "$EXPECTED_DASHBOARD_ASSET" <<<"$homepage"; then
      if curl -fsS -o /dev/null "$DASHBOARD_URL/$EXPECTED_DASHBOARD_ASSET"; then
        log_info "Dashboard servindo o bundle esperado: $EXPECTED_DASHBOARD_ASSET"
      else
        log_warn "Homepage referencia $EXPECTED_DASHBOARD_ASSET, mas o asset não respondeu via HTTP."
      fi
    else
      log_warn "Homepage do dashboard não referencia o bundle esperado: $EXPECTED_DASHBOARD_ASSET"
    fi
    return
  fi

  log_info "Dashboard respondendo em $DASHBOARD_URL"
}

main() {
  parse_args "$@"
  local peer_runtime_compose_file=""
  local -a orchestrator_services=(
    cello-postgres
    cello-api-engine
    cello-dashboard
  )
  local -a orchestrator_services_no_backend=(
    cello-postgres
    cello-dashboard
  )

  if flag_enabled "$COGNUS_ENABLE_DOCKER_AGENT"; then
    orchestrator_services+=(cello-docker-agent)
    orchestrator_services_no_backend+=(cello-docker-agent)
  fi

  ensure_command docker Docker
  ensure_docker_daemon
  select_compose

  ensure_network "cello-net"
  if flag_enabled "$COGNUS_ENABLE_LOCAL_PEER_RUNTIME"; then
    ensure_network "chaincode-blockchain-stcs-net"
    ensure_network "cc-tools-demo-net"
  fi

  if backend_needs_build; then
    log_info "Mudanças detectadas no backend; rebuild da imagem do API Engine."
    build_backend_image
  elif ! backend_runtime_tooling_ready; then
    log_info "API Engine atual sem tooling Docker operacional; forçando rebuild do backend."
    build_backend_image
  else
    log_info "Sem mudanças no backend; rebuild não necessário."
  fi

  if gateway_needs_build; then
    log_info "Mudanças detectadas no chaincode-gateway (ou imagem runtime ausente); rebuild da imagem local exigida pelo runbook SSH."
    build_gateway_image
  else
    log_info "Sem mudanças na imagem runtime do gateway; rebuild não necessário."
  fi

  if flag_enabled "$COGNUS_ENABLE_LOCAL_PEER_RUNTIME"; then
    peer_runtime_compose_file="$(resolve_peer_runtime_compose_file || true)"
    if [ -n "$peer_runtime_compose_file" ] && [ -f "$peer_runtime_compose_file" ]; then
      up_peer_runtime_stack "$peer_runtime_compose_file"
    else
      log_warn "Compose de peers/chaincode não encontrado via descoberta dinâmica; defina COGNUS_PEER_RUNTIME_COMPOSE_FILE para habilitar essa etapa."
    fi
  fi
  if [[ "$BACKEND_REBUILT" -eq 1 ]]; then
    log_info "Subindo serviços centrais do orquestrador sem recriar api-engine (tratado separadamente)."
    "${COMPOSE[@]}" -f "cello-v2/bootup/docker-compose-files/docker-compose.dev.yml" up -d --remove-orphans --no-deps "${orchestrator_services_no_backend[@]}"
  else
    log_info "Subindo núcleo do orquestrador"
    "${COMPOSE[@]}" -f "cello-v2/bootup/docker-compose-files/docker-compose.dev.yml" up -d --remove-orphans "${orchestrator_services[@]}"
  fi
  recreate_backend_if_rebuilt
  if flag_enabled "$COGNUS_ENABLE_LOCAL_GATEWAY"; then
    up_stack "chaincode-gateway/docker-compose.yaml" "chaincode-gateway e auto-operator"
  fi
  ensure_cognus_runtime_images
  enforce_restart_policies

  if dashboard_needs_build; then
    log_info "Mudanças detectadas no frontend do dashboard; aplicando rebuild e sync de assets."
    build_and_sync_dashboard
  else
    log_info "Sem mudanças no frontend do dashboard; rebuild/sync não necessário."
  fi
  sync_dashboard_nginx_config

  show_status "cello-v2/bootup/docker-compose-files/docker-compose.dev.yml" "Orquestrador"
  if flag_enabled "$COGNUS_ENABLE_LOCAL_GATEWAY"; then
    show_status "chaincode-gateway/docker-compose.yaml" "Gateway"
  fi

  validate_dashboard_serving

  log_info "Update rápido concluído."
}

main "$@"
