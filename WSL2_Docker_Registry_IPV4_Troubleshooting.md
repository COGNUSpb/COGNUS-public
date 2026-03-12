# Troubleshooting: Docker Registry IPv4 Connectivity on WSL2/Windows

Este checklist ajuda a restaurar conectividade IPv4 para o Docker Registry em ambientes WSL2/Windows, quando o script está correto mas o host não consegue baixar imagens.

## 1. Teste conectividade IPv4 geral

```bash
ping -4 8.8.8.8
curl -4 https://google.com
```
Se falhar, o problema é de rede do WSL2/Windows.

## 2. Teste DNS e rota para Docker Registry

```bash
nslookup registry-1.docker.io
ping -4 registry-1.docker.io
curl -4 https://registry-1.docker.io/v2/
```
Se falhar, pode ser bloqueio de firewall, proxy ou rota.

## 3. Reinicie o WSL2 e Docker Desktop

- Feche todas as instâncias WSL2.
- No Windows, execute:
  - `wsl --shutdown`
  - Reinicie o Docker Desktop.
  - Abra o Ubuntu/WSL2 novamente.

## 4. Verifique regras de firewall/antivírus

- No Windows, abra o Firewall e permita conexões de saída para o WSL2 e Docker Desktop.
- Desative temporariamente antivírus ou VPN para testar.

## 5. Configure proxy IPv4 no Docker (se necessário)

Se sua rede exige proxy, crie/edite `/etc/systemd/system/docker.service.d/http-proxy.conf`:

```
[Service]
Environment="HTTP_PROXY=http://proxy.exemplo.com:3128/"
Environment="HTTPS_PROXY=http://proxy.exemplo.com:3128/"
```
Reinicie o Docker:
```bash
sudo systemctl daemon-reload
sudo systemctl restart docker
```

## 6. Verifique NAT e roteamento do WSL2

- No Windows, execute:
  - `ipconfig` e verifique se há interface vEthernet (WSL).
  - `Get-NetNat` no PowerShell para ver regras NAT.
- Se necessário, reinicie o serviço de rede do WSL2:
  - `wsl --shutdown`
  - Reinicie o computador.

## 7. Teste em VM Linux nativa ou outro host

Se possível, teste o mesmo script em uma VM Linux real para isolar se o problema é do WSL2/Windows.

## 8. Consulte documentação oficial
- [WSL2 Networking](https://learn.microsoft.com/en-us/windows/wsl/networking)
- [Docker Desktop WSL2](https://docs.docker.com/desktop/wsl/)
- [Docker Proxy](https://docs.docker.com/network/proxy/)

## 9. Logs e diagnóstico

- Verifique logs do Docker:
  - `journalctl -u docker`
  - `docker info`
- Verifique logs do Windows Event Viewer para bloqueios de rede.

---
Se após todos os passos o problema persistir, pode ser limitação do ambiente WSL2/Windows. Recomenda-se migrar para VM Linux nativa ou ajustar rede do host.
