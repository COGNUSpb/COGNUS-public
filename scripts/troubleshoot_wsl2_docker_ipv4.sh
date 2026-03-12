#!/bin/bash
# Script de troubleshooting e correção automática para conectividade IPv4 do Docker Registry no WSL2/Windows
# Executa testes, tenta rotas alternativas, VPN, proxy, e orienta correção definitiva

set -e

# 1. Testa conectividade IPv4 geral
ping -4 -c 2 8.8.8.8 || echo "[ERRO] Sem resposta do Google DNS via IPv4. Verifique rede do Windows/WSL2."
curl -4 https://google.com || echo "[ERRO] Sem resposta do Google via IPv4. Verifique rede do Windows/WSL2."

# 2. Testa DNS e rota para Docker Registry
nslookup registry-1.docker.io || echo "[ERRO] Falha na resolução DNS para registry-1.docker.io."
ping -4 -c 2 registry-1.docker.io || echo "[ERRO] Sem resposta do registry-1.docker.io via IPv4."
curl -4 https://registry-1.docker.io/v2/ || echo "[ERRO] Sem resposta do registry-1.docker.io via IPv4."

# 3. Testa outros hosts críticos
for host in github.com quay.io hub.docker.com google.com; do
  echo "[TESTE] Ping IPv4 para $host..."
  ping -4 -c 2 $host || echo "[ERRO] Sem resposta de $host via IPv4."
  echo "[TESTE] Curl IPv4 para $host..."
  curl -4 -I https://$host || echo "[ERRO] Sem resposta de $host via IPv4."
done

# 4. Tenta forçar rota alternativa via VPN (se disponível)
if command -v nmcli >/dev/null; then
  echo "[INFO] Verificando VPNs disponíveis..."
  nmcli connection show --active | grep vpn && echo "[INFO] VPN ativa detectada. Testando registry-1.docker.io via VPN..." || echo "[INFO] Nenhuma VPN ativa detectada."
  ping -4 -c 2 registry-1.docker.io || echo "[ERRO] Mesmo via VPN, sem resposta do registry-1.docker.io."
fi

# 5. Tenta proxy público temporário para Docker
export HTTP_PROXY="http://proxy.docker.com:80"
export HTTPS_PROXY="http://proxy.docker.com:80"
echo "[INFO] Testando Docker pull via proxy público (se disponível)..."
docker pull hello-world || echo "[ERRO] Docker pull via proxy também falhou."
unset HTTP_PROXY
unset HTTPS_PROXY

# 6. Sugere troca de rede (hotspot, 4G, VPN)
echo "[INFO] Se todos os testes falharem, tente conectar em outra rede (hotspot, 4G, VPN corporativa) e execute novamente."

# 7. Reinicia WSL2 e Docker Desktop automaticamente se possível
if grep -qi microsoft /proc/version; then
  echo "[INFO] Detectado ambiente WSL2. Tentando reiniciar WSL2..."
  wsl.exe --shutdown || echo "[WARN] Falha ao reiniciar WSL2. Faça manualmente no Windows: wsl --shutdown"
  echo "[INFO] Reinicie o Docker Desktop manualmente no Windows."
fi

# 8. Logs e diagnóstico
journalctl -u docker --no-pager | tail -n 20 || true
docker info || true

# 9. Sugestão final
cat <<EOF
---
Se após todos os passos o problema persistir, é limitação da rede do host ou do provedor.
- Tente em outro ambiente (VM Linux nativa, outro computador, rede diferente).
- Consulte administrador de rede ou suporte do provedor.
- Consulte:
  https://learn.microsoft.com/en-us/windows/wsl/networking
  https://docs.docker.com/desktop/wsl/
  https://docs.docker.com/network/proxy/
EOF
