#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

CLEAN_ENV_FULL_PRUNE="${CLEAN_ENV_FULL_PRUNE:-0}"

PRIMARY_STORAGE="$SCRIPT_DIR/cello-v2/cello-storage"
LEGACY_STORAGE="$SCRIPT_DIR/cello-storage"
STORAGE_TARGETS=("$PRIMARY_STORAGE" "$LEGACY_STORAGE")

log() {
	local level="$1"
	shift
	printf '[%s] %s\n' "$level" "$*"
}

log_info() { log INFO "$@"; }
log_warn() { log AVISO "$@"; }
log_error() { log ERRO "$@"; }

ensure_command() {
	local cmd="$1"
	local pkg="${2:-$1}"
	if ! command -v "$cmd" >/dev/null 2>&1; then
		log_error "Dependência '$cmd' não encontrada. Instale $pkg e execute novamente."
		exit 1
	fi
}

select_docker_compose() {
	if docker compose version >/dev/null 2>&1; then
		DOCKER_COMPOSE_CMD=(docker compose)
		return
	elif command -v docker-compose >/dev/null 2>&1; then
		DOCKER_COMPOSE_CMD=(docker-compose)
		return
	fi
	log_error "Docker Compose não encontrado. Instale o plugin oficial ou o binário docker-compose."
	exit 1
}

docker_remove_container() {
	local name="$1"
	if docker ps -a --format '{{.Names}}' | grep -qx "$name"; then
		log_info "Removendo contêiner '$name'."
		docker rm -f "$name" >/dev/null 2>&1 || true
	fi
}

log_info "Iniciando limpeza do ambiente COGNUSpb/Cello."
ensure_command docker Docker
select_docker_compose

docker_root_rm() {
	local target="$1"
	if [ -z "$target" ] || [ ! -e "$target" ]; then
		return
	fi
	local rel_path="${target#$SCRIPT_DIR/}"
	if [ "$rel_path" = "$target" ]; then
		rel_path="$target"
	fi
	# Usa um contêiner busybox para executar rm com privilégios de root dentro do bind mount.
	docker run --rm -v "$SCRIPT_DIR":/workspace busybox sh -c "cd /workspace && rm -rf \"${rel_path}\"" >/dev/null 2>&1 || true
}

if docker ps >/dev/null 2>&1; then
	if [ -f cello-v2/bootup/docker-compose-files/docker-compose.dev.yml ]; then
		log_info "Desligando a stack Cello principal."
		"${DOCKER_COMPOSE_CMD[@]}" -f cello-v2/bootup/docker-compose-files/docker-compose.dev.yml down --remove-orphans -v || true
	fi

	if [ -f chaincode-gateway/docker-compose.yaml ]; then
		log_info "Desligando a stack chaincode-gateway."
		(cd chaincode-gateway && "${DOCKER_COMPOSE_CMD[@]}" down --remove-orphans) || true
	fi

STACK_CONTAINERS=(
	cello-dashboard
	cello-api-engine
	cello-postgres
	cello-docker-agent
	chaincode-gateway
	fabric-auto-operator
)
for container in "${STACK_CONTAINERS[@]}"; do
	docker_remove_container "$container"
done

# Remove dynamically provisioned Fabric containers (peers/orderers/ccaas and their CouchDBs).
if docker ps -a >/dev/null 2>&1; then
	docker ps -a --format '{{.Names}}' | while read -r dyn_container; do
		[ -n "$dyn_container" ] || continue
		case "$dyn_container" in
			peer[0-9]*.cello.local|orderer[0-9]*.cello.local|dev-peer*|*-cdb)
				log_info "Removendo contêiner dinâmico '$dyn_container'."
				docker rm -f "$dyn_container" >/dev/null 2>&1 || true
				;;
		esac
	done
fi

	if docker network inspect cello-net >/dev/null 2>&1; then
		log_info "Removendo rede docker 'cello-net'."
		docker network rm cello-net >/dev/null 2>&1 || true
	fi

	if [ "$CLEAN_ENV_FULL_PRUNE" = "1" ]; then
		log_warn "CLEAN_ENV_FULL_PRUNE=1 definido: executando prune completo de recursos Docker."
		docker system prune -a -f --volumes || true
	else
		log_info "Removendo volumes locais associados à stack."
		docker volume ls --format '{{.Name}}' | grep -E '^cello-' | xargs -r docker volume rm >/dev/null 2>&1 || true
		docker volume rm -f cello-postgres >/dev/null 2>&1 || true
	fi
else
	log_warn "Docker daemon indisponível; pulando operações Docker."
fi

log_info "Limpando artefatos gerados pelo projeto."
log_info "Removendo arquivos protegidos de todas orgs em chaincode-gateway/data/."
for org_dir in chaincode-gateway/data/*; do
	[ -d "$org_dir" ] || continue
	# signcerts
	if [ -d "$org_dir/msp/signcerts" ]; then
		for f in "$org_dir/msp/signcerts"/*; do
			[ -e "$f" ] || continue
			log_info "Removendo $f via docker-root (busybox) para garantir remoção de arquivos criados por container."
			docker run --rm -v "$SCRIPT_DIR":/workspace busybox sh -c "rm -f /workspace/${f}" || sudo rm -f "$f"
		done
	fi
	# keystore
	if [ -d "$org_dir/msp/keystore" ]; then
		for f in "$org_dir/msp/keystore"/*; do
			[ -e "$f" ] || continue
			log_info "Removendo $f via docker-root (busybox) para garantir remoção de arquivos criados por container."
			docker run --rm -v "$SCRIPT_DIR":/workspace busybox sh -c "rm -f /workspace/${f}" || sudo rm -f "$f"
		done
	fi
	# tls
	if [ -d "$org_dir/tls" ]; then
		for f in "$org_dir/tls"/*; do
			[ -e "$f" ] || continue
			log_info "Removendo $f via docker-root (busybox) para garantir remoção de arquivos criados por container."
			docker run --rm -v "$SCRIPT_DIR":/workspace busybox sh -c "rm -f /workspace/${f}" || sudo rm -f "$f"
		done
	fi
	log_info "Removendo diretório $org_dir via docker-root."
	docker run --rm -v "$SCRIPT_DIR":/workspace busybox sh -c "rm -rf /workspace/${org_dir}" || sudo rm -rf "$org_dir"
done
for storage_dir in "${STORAGE_TARGETS[@]}"; do
	[ -n "$storage_dir" ] || continue
	log_info "Resetando diretórios gerados em $storage_dir."
	if [ -d "$storage_dir" ]; then
		while IFS= read -r path; do
			docker_root_rm "$path"
		done < <(find "$storage_dir" -mindepth 1 -maxdepth 1 -type d ! -name "hyperledger" ! -name "pgdata" 2>/dev/null)
	fi
	docker_root_rm "$storage_dir/hyperledger/fabric"
	docker_root_rm "$storage_dir/hyperledger/production"
	docker_root_rm "$storage_dir/hyperledger/couchdb"
	docker_root_rm "$storage_dir/couchdb"
	mkdir -p "$storage_dir/hyperledger"
	docker_root_rm "$storage_dir/pgdata"
done
docker_root_rm cello-v2/src/api-engine/cello-storage/pgdata
docker_root_rm cello-storage/pgdata
rm -rf automation/resources/crypto-config/
rm -rf automation/resources/configtx/
rm -rf automation/resources/channel-artifacts/
rm -rf cctools-chaincodes/*/ccapi/crypto-config/
rm -rf cctools-chaincodes/*/ccapi/configtx/
rm -rf cctools-chaincodes/*/ccapi/channel-artifacts/
rm -rf chaincodes/*/crypto-config/
rm -rf chaincodes/*/configtx/
rm -rf chaincodes/*/channel-artifacts/

rm -rf chaincode-gateway/data/*
mkdir -p chaincode-gateway/data
if [ -f chaincode-gateway/templates/fabric-tlscas.pem ]; then
	cp chaincode-gateway/templates/fabric-tlscas.pem chaincode-gateway/data/fabric-tlscas.pem
fi

rm -rf cello-v2/bootup/chaincode-gateway/data/*
mkdir -p cello-v2/bootup/chaincode-gateway/data
if [ -f chaincode-gateway/templates/fabric-tlscas.pem ]; then
	cp chaincode-gateway/templates/fabric-tlscas.pem cello-v2/bootup/chaincode-gateway/data/fabric-tlscas.pem
fi

mkdir -p cello-v2/src/dashboard/public
cleaned_at_utc="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
cat > cello-v2/src/dashboard/public/cognus-clean-marker.json <<EOF
{
	"cleaned_at_utc": "$cleaned_at_utc",
	"source": "clean_env.sh"
}
EOF

log_info "Removendo bancos de dados SQLite/Django locais (se existirem)."
find . -name "*.sqlite3" -delete 2>/dev/null || true

log_info "Removendo arquivos de log."
find . -name "*.log" -delete 2>/dev/null || true

log_info "Removendo arquivos pyc e caches Python."
find . -name "*.pyc" -delete 2>/dev/null || true
find . -name "__pycache__" -type d -prune -exec rm -rf {} + 2>/dev/null || true

log_info "Limpeza concluída. Ambiente pronto para nova execução do start_env.sh."
