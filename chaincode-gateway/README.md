# Chaincode Gateway

Serviço REST para invocar e consultar chaincodes Hyperledger Fabric, compatível com o contrato ccapi (`/api/<channel>/<chaincode>/...`).

No COGNUSpb, o gateway é parte da camada de orquestração (Camada 3) e deve operar em conjunto com o fluxo de mudança governado (OPSSC), mantendo rastreabilidade de execuções e integração com hooks pós-commit.

## Papel no orquestrador

- Expor transações de chaincode para consumo externo.
- Reagir a atualizações de identidade/perfil geradas pelo pipeline de lifecycle.
- Permitir validação operacional automática (smoke tests).
- Suportar correlação com mudanças (`change-id`) no contexto do plano de controle.

## Configuração

Defina as variáveis de ambiente antes de iniciar o serviço:

| Variável                 | Descrição                                                                                 |
|--------------------------|-------------------------------------------------------------------------------------------|
| `PORT`                   | Porta HTTP (padrão `3000`).                                                               |
| `CCP_PATH`               | Caminho padrão para o connection profile (usado como fallback).                           |
| `MSP_ID`                 | MSP padrão.                                                                               |
| `CERT_PATH` / `KEY_PATH` | Certificado e chave padrão.                                                               |
| `IDENTITY_LABEL`         | Label da identidade padrão (opcional).                                                   |
| `DISCOVERY_AS_LOCALHOST` | `true` se estiver rodando ambiente local (padrão `false`).                                |
| `IDENTITY_CONFIG`        | Caminho para JSON com as identidades dinâmicas (ver abaixo).                              |
| `DEFAULT_ORG`            | Organização usada quando nenhuma é informada na requisição (auto detecta quando só existe uma entrada em `identities.json`). |
| `DEFAULT_CHANNEL`        | Canal padrão para as rotas CCAPI sem `:channel` (ex.: `/api/gateway/invoke/...`).         |
| `DEFAULT_CHAINCODE`      | Chaincode padrão para as rotas CCAPI sem `:chaincode`.                                    |
| `WALLET_PATH`            | Diretório do wallet do Fabric Gateway (padrão `.gateway-wallet`).                         |
| `CCAPI_CCP_TEMPLATE`     | (Opcional) Template para gerar `ccpPath`, ex.: `/dados/connection-{org}.json`.            |
| `CCAPI_DEFAULT_CCP`      | (Opcional) Valor padrão para `ccpPath` quando o template não for usado.                   |
| `CHAINCODE_GATEWAY_DIR`  | (Opcional) Diretório base do gateway para o hook do Cello resolver paths automaticamente. |
| `CCAPI_CONTAINER_STORAGE`| (Opcional) Caminho do storage dentro do container (padrão `CELLO_HOME`).                   |
| `CCAPI_HOST_STORAGE`     | (Opcional) Caminho equivalente no host (padrão `CELLO_STORAGE_PATH` ou `CELLO_HOME`).      |

Exemplo `.env` (ver também `.env.example`):

```
PORT=8085
CCP_PATH=
MSP_ID=
CERT_PATH=
KEY_PATH=
IDENTITY_LABEL=
IDENTITY_CONFIG=/workspace/data/identities.json
DEFAULT_ORG=
DEFAULT_CHANNEL=
DEFAULT_CHAINCODE=
DISCOVERY_AS_LOCALHOST=false
```

### identities.json

Arquivo JSON com o mapeamento de organizações → identidades. Exemplo:

```json
{
  "infufg": {
    "ccpPath": "/workspace/data/connection-infufg.json",
    "mspId": "InfufgMSP",
    "certPath": "/workspace/data/infufg/msp/signcerts/Admin@infufg.cello.local-cert.pem",
    "keyPath": "/workspace/data/infufg/msp/keystore/priv_sk",
    "channels": ["stcs-channel", "fakenews-channel-dev"],
    "discoveryAsLocalhost": false
  }
}
```
> Os caminhos acima são placeholders. Se o gateway enxergar diretamente o diretório `cello-v2/cello-storage` (montado como volume), basta garantir permissão de leitura – não é necessário copiar. Caso contrário, copie os certificados/chaves para `data/<org>/msp/...` e gere o arquivo `connection-<org>.json`. Em ambos os casos, o Cello reescreverá `identities.json` após cada commit usando esses caminhos.

O serviço usa `org` (query string ou header `X-Fabric-Org`) para selecionar a identidade. Opcionalmente é possível informar o header `User` (ou query `user=`) para indicar o label do wallet quando múltiplas credenciais estiverem mapeadas em `identities.json`. Caso nenhum valor seja informado, o gateway tenta descobrir automaticamente:

- Se houver apenas uma identidade configurada, ela será utilizada.
- Caso contrário, o gateway procura por uma identidade cujo campo `channels` (ou `channel`, `defaultChannels`, `networkChannels`) contenha o canal requisitado. Se o campo não existir, ele tenta ler a lista de canais do `connection profile` (`ccpPath`) correspondente.
- Persistindo múltiplas opções, é usado `DEFAULT_ORG` (ou o primeiro registro do arquivo) como fallback.

## Execução

```
# Para desenvolvimento local
cd chaincode-gateway
npm install --registry=https://registry.npmjs.org/
# Requer Node >= 18 (use `nvm use 18`).
npm start

# Para execução contínua (recomendado)
cd chaincode-gateway
docker compose up -d --build chaincode-gateway
# Com o operador automático habilitado (default a partir desta versão):
docker compose up -d --build
# (a rede `cello-net` é criada automaticamente pelo stack principal; mantenha o Cello em execução)
```

### Automação completa (`fabric-auto-operator`)

O `docker-compose.yaml` agora publica, além do container Node, o serviço `fabric-auto-operator`. Ele usa o socket Docker para monitorar peers criados pelo Cello e executa todo o fluxo pós-provisionamento sem intervenção manual:

1. Descobre peers `peerN.<org>.cello.local` e valida se o ledger usa CouchDB.
2. Empacota, instala, aprova e comita a versão mais recente de cada chaincode presente em `/opt/cello/chaincode`.
3. Copia MSP/TLS do `Admin@<org>`, gera `connection-<org>.json`, atualiza `identities.json` e limpa o wallet do gateway somente quando algum artefato mudou.
4. O processo roda continuamente; após criação de rede/peer/channel/chaincode, aguarde a convergência para exposição da API REST.

Verifique os logs com `docker logs -f fabric-auto-operator` para acompanhamento e depuração.

## Recomendação de rastreabilidade

Em ambientes operacionais, registre em cada mudança relevante:

- versão de chaincode comitada;
- atualização aplicada em `identities.json`/profiles;
- resultado dos smoke tests;
- evidência de sucesso/falha por etapa.

## Rotas principais

- `POST|PUT|DELETE /api/:channel/:chaincode/invoke/:tx`
- `POST|GET /api/:channel/:chaincode/query/:tx`
- `POST|PUT|DELETE /api/invoke/:tx` e `POST|GET /api/query/:tx` (usa `DEFAULT_CHANNEL` + `DEFAULT_CHAINCODE`)
- Aliases de compatibilidade CCAPI: `/api/gateway/:channel/:chaincode/...` e `/api/gateway/(invoke|query)/:tx`

### Payloads e parâmetros

- **Invokes**: o corpo JSON é serializado e enviado como um único argumento (`string(req)`), exatamente como a ccapi. Caso envie `args` (array) ou `arg` (valor), eles são usados diretamente como lista de argumentos.
- **Transient data**: campos iniciados por `~` (ex.: `~asset`) são removidos do corpo principal e enviados via transient map (`@request`) na transação.
- **Endossadores explícitos**: informe `@endorsers=<base64-json-array>` na query string para replicar `WithEndorsingOrganizations`.
- **Queries GET**: use `@request=<base64-do-json>` para serializar o corpo; se omitido, a transação é avaliada sem argumentos.

### Respostas

- Sucesso (`200`): retorna diretamente o payload JSON proveniente do chaincode (sem wrapper adicional).
- Erro: `{ "status": <código HTTP>, "error": "<mensagem>" }`, seguindo o padrão ccapi.

### Seleção automática da organização

- Cada entrada em `identities.json` pode declarar explicitamente os canais atendidos (`"channels": ["stcs-channel", "fakenews-channel-dev"]`).
- Para compatibilidade, chaves alternativas (`channel`, `networkChannels`, `defaultChannels`) ou objetos (ex.: `{"channels": {"stcs-channel": {...}}}`) também são aceitas; apenas os nomes são lidos.
- Quando não houver metadados, o gateway tenta carregar os canais definidos no `connection profile` (`ccpPath`) e utilizar a primeira correspondência encontrada.
- Se nenhuma correspondência existir, informe `DEFAULT_ORG` via variável de ambiente ou usando `?org=...`/header `X-Fabric-Org`.

## Exemplos

```
# Buscar orders
curl -X POST "http://localhost:8085/api/stcs-channel/stcs-cc/query/search" \
  -H "Content-Type: application/json" \
  -d '{"selector":{"@assetType":"order"},"limit":255,"bookmark":""}'

# Invocar CreateExam
curl -X POST "http://localhost:8085/api/stcs-channel/stcs-cc/invoke/CreateExam" \
  -H "Content-Type: application/json" \
  -d '{"patientId":"123","metaverseId":"m1"}'
```

> (Opcional) Informe a organização via `org` (query string) ou header `X-Fabric-Org`. Caso omisso, o gateway usa `DEFAULT_ORG` (auto-detectado quando há uma única identidade disponível).

## Atualização automática / integração com Cello

- Defina `CCAPI_COMPOSE_FILE=/caminho/para/chaincode-gateway/docker-compose.yaml` no container `cello-api-engine`. O hook de commit chamará `docker compose up -d --build` automaticamente, subindo tanto o gateway quanto o `fabric-auto-operator`.
- Opcionalmente defina:
  - `CHAINCODE_GATEWAY_DIR` para informar explicitamente onde está o gateway (caso a estrutura do repositório seja diferente).
  - `CCAPI_IDENTITIES_FILE`, `CCAPI_CCP_TEMPLATE` e `CCAPI_COMPOSE_FILE` se desejar caminhos específicos.
  - `CCAPI_CONTAINER_STORAGE` e `CCAPI_HOST_STORAGE` quando precisar mapear o caminho gerado dentro do container (`CELLO_HOME`) para o caminho equivalente no host onde o gateway roda.
- Certifique-se de que a rede Docker `cello-net` exista (ela é criada quando o stack principal sobe). O compose do gateway já a declara como rede externa e conecta o serviço automaticamente.
- O serviço precisa estar em execução ao menos uma vez (`npm start` ou `docker compose up -d`). O hook apenas recarrega/recria o container com a configuração mais recente e atualiza `identities.json` automaticamente com certificados/keys gerados pelo Cello.

## Permissões

Os arquivos de chave (`priv_sk`) gerados pelo Cello podem ter permissões restritivas (`600`). Garanta que o usuário que executa o gateway tenha permissão de leitura. Exemplos:

```bash
sudo chmod 644 cello-v2/cello-storage/<org>/crypto-config/peerOrganizations/<org>/users/Admin@<org>/msp/keystore/priv_sk
sudo chmod 644 cello-v2/cello-storage/<org>/crypto-config/peerOrganizations/<org>/users/Admin@<org>/msp/signcerts/*.pem
```

Se optar por copiar os artefatos para `chaincode-gateway/data`, use `sudo cp -a` e ajuste a propriedade (`sudo chown -R <user>:<group> chaincode-gateway/data/<org>`).

## Deploy automático

O Cello pode chamar `docker compose up -d --build <serviço>` após o commit do chaincode. Aponte a variável `CCAPI_COMPOSE_FILE` no container do API Engine para o `docker-compose` que instância este gateway (ex.: serviço `chaincode-gateway`). Desta forma, sempre que um chaincode for commitado, o gateway é atualizado automaticamente.
