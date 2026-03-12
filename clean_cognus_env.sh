#!/bin/bash
# Limpeza total de containers, imagens, volumes e dados locais do COGNUS

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"
shopt -s nullglob

collect_mount_sources() {
	local container="$1"
	local -a mounts=()
	if docker ps -a --format '{{.Names}}' | grep -qx "$container"; then
		while IFS= read -r mount; do
			[ -n "$mount" ] && mounts+=("$mount")
		done < <(docker inspect "$container" --format '{{range .Mounts}}{{.Source}}\n{{end}}' 2>/dev/null || true)
	fi
	printf '%s\n' "${mounts[@]}"
}

docker_root_rm() {
	local target="$1"
	[ -z "$target" ] && return

	local abs_target
	if [[ "$target" = /* ]]; then
		abs_target="$target"
	else
		abs_target="$SCRIPT_DIR/$target"
	fi

	[ -e "$abs_target" ] || return
	local rel_target="${abs_target#$SCRIPT_DIR/}"

	# Remove via busybox (root inside container) e cai para sudo rm caso falhe.
	if ! docker run --rm -v "$SCRIPT_DIR":/workspace busybox sh -c "cd /workspace && rm -rf \"${rel_target}\"" >/dev/null 2>&1; then
		sudo rm -rf "$abs_target" || true
	fi
}

# 1. Parar e remover todos os containers
# Captura mounts atuais do postgres antes de remover para garantir limpeza do host.
EXTRA_MOUNTS=(
	$(collect_mount_sources "cello-postgres")
)

sudo docker stop $(sudo docker ps -aq) || true
sudo docker rm -vf $(sudo docker ps -aq) || true

# 2. Remover todas as imagens
sudo docker rmi -f $(sudo docker images -aq) || true

# 3. Remover todos os volumes
sudo docker volume prune -f
sudo docker system prune -a --volumes -f

# 3.1 Remover rede externa caso exista
docker network rm chaincode-blockchain-stcs-net >/dev/null 2>&1 || true

# 4. Remover diretórios de dados persistentes locais (inclui orgs, artefatos e caches)
DATA_DIRS=(
	"cello-v2/cello-storage"
	"cello-v2/src/api-engine/cello-storage"
	"cello-storage"
	"chaincode-gateway/data"
	"cello-v2/bootup/chaincode-gateway/data"
	"cello-v2/bootup/data"
	"cello-v2/bootup/artifacts"
	"cello-v2/chaincode-gateway/data"
	"cello-v2/chaincode-gateway/artifacts"
	".artifacts"
	"artifacts"
	"exporting"
	"reading"
	"writing"
	"transferring"
	"naming"
)
for path in "${DATA_DIRS[@]}" "${EXTRA_MOUNTS[@]}"; do
	docker_root_rm "$path"
done

# 5. Remover cripto/material de orgs previamente criadas
CRYPTO_DIRS=(
	"automation/resources/crypto-config"
	"automation/resources/configtx"
	"automation/resources/channel-artifacts"
	"cctools-chaincodes"/*/ccapi/crypto-config
	"cctools-chaincodes"/*/ccapi/configtx
	"cctools-chaincodes"/*/ccapi/channel-artifacts
	"chaincodes"
)
for path in "${CRYPTO_DIRS[@]}"; do
	docker_root_rm "$path"
done

# 6. Remover arquivos de log e cache
rm -rf cello-v2/src/dashboard/.cache cello-v2/src/dashboard/.next cello-v2/src/dashboard/logs

# 7. Remover arquivos de histórico local
rm -rf *.tar *.log *.pid

# 8. Mensagem final
echo "Ambiente COGNUS limpo. Pronto para start_env.sh do zero."

shopt -u nullglob
