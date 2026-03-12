import subprocess
# Utilitário para checar e reiniciar o agente Docker
def ensure_docker_agent_running(log_event=None):
    try:
        result = subprocess.run(["docker", "inspect", "-f", "{{.State.Running}}", "cello-docker-agent"], capture_output=True, text=True)
        if result.returncode != 0 or "false" in result.stdout:
            if log_event:
                log_event("[API] Agente Docker não está rodando. Reiniciando container cello-docker-agent...", level="warning")
            subprocess.run(["docker", "restart", "cello-docker-agent"], check=False)
            if log_event:
                log_event("[API] Container cello-docker-agent reiniciado automaticamente.", level="info")
            return False
        return True
    except Exception as e:
        if log_event:
            log_event(f"[API] Erro ao checar/reiniciar agente Docker: {e}", level="error")
        return False
#
# SPDX-License-Identifier: Apache-2.0
#

from copy import deepcopy
import logging
import json
import os
from pathlib import Path

from rest_framework import viewsets, status
from rest_framework.decorators import action
from rest_framework.response import Response
from rest_framework.parsers import MultiPartParser, FormParser, JSONParser
from rest_framework.permissions import IsAuthenticated

from drf_yasg.utils import swagger_auto_schema

from django.core.exceptions import ObjectDoesNotExist
from django.core.paginator import Paginator

from api.config import CELLO_HOME
from api.common.serializers import PageQuerySerializer
from api.utils.common import (
    with_common_response,
    parse_block_file,
    to_dict,
    json_filter,
    json_add_anchor_peer,
    json_create_envelope,
    init_env_vars,
)
from api.utils.gateway_sync import sync_gateway_artifacts
from api.lib.configtxgen import ConfigTX, ConfigTxGen
from api.lib.peer.channel import Channel as PeerChannel
from api.lib.configtxlator.configtxlator import ConfigTxLator
from api.exceptions import ResourceNotFound, NoResource
from api.models import (
    Channel,
    Node,
    Organization,
)
from api.routes.channel.serializers import (
    ChannelCreateBody,
    ChannelIDSerializer,
    ChannelListResponse,
    ChannelResponseSerializer,
    ChannelUpdateSerializer,
)

from api.common import ok, err
from api.common.enums import (
    NodeStatus,
    FabricNodeType,
)
from api.lib.pki.cryptogen.cryptogen import CryptoGen

LOG = logging.getLogger(__name__)

CFG_JSON = "cfg.json"
CFG_PB = "cfg.pb"
DELTA_PB = "delta.pb"
DELTA_JSON = "delta.json"
UPDATED_CFG_JSON = "update_cfg.json"
UPDATED_CFG_PB = "update_cfg.pb"
CFG_DELTA_ENV_JSON = "cfg_delta_env.json"
CFG_DELTA_ENV_PB = "cfg_delta_env.pb"



class ChannelViewSet(viewsets.ViewSet):
    def _log_event(self, message, level="info"):
        # Loga e registra evento para dashboard (banco de dados)
        if level == "info":
            LOG.info(message)
        elif level == "warning":
            LOG.warning(message)
        elif level == "error":
            LOG.error(message)

        # Tenta registrar evento no banco para dashboard
        try:
            from django.utils import timezone
            # Tenta importar um modelo EventLog, se existir
            try:
                from api.models import EventLog
            except ImportError:
                EventLog = None
            if EventLog:
                EventLog.objects.create(
                    timestamp=timezone.now(),
                    level=level,
                    message=message
                )
        except Exception as e:
            LOG.warning(f"[DASHBOARD] Falha ao registrar evento no banco: {e}")


    def _ensure_crypto_materials(self, org):
        """Garantir que os artefatos cryptogen existam antes da criação de canal."""
        crypto_dir = Path(f"{CELLO_HOME}/{org.name}/crypto-config")
        if crypto_dir.exists() and any(crypto_dir.iterdir()):
            return

        config_yaml = crypto_dir.parent / "crypto-config.yaml"
        if not config_yaml.exists():
            self._log_event(
                f"[API] Arquivo crypto-config.yaml ausente para '{org.name}'. Não é possível regenerar automaticamente.",
                level="error",
            )
            raise FileNotFoundError(config_yaml)

        self._log_event(
            f"[API] Artefatos cryptogen ausentes para '{org.name}'. Recriando via cryptogen.",
            level="warning",
        )
        try:
            CryptoGen(org.name).generate()
            # extend mantém compatibilidade quando diretórios já existiam parcialmente
            CryptoGen(org.name).extend()
        except Exception as exc:  # pragma: no cover
            self._log_event(
                f"[API] Falha ao regenerar artefatos cryptogen para '{org.name}': {exc}",
                level="error",
            )
            raise



    permission_classes = [
        IsAuthenticated,
    ]
    parser_classes = [MultiPartParser, FormParser, JSONParser]

    @swagger_auto_schema(
        query_serializer=PageQuerySerializer,
        responses=with_common_response(
            {status.HTTP_201_CREATED: ChannelListResponse}
        ),
    )
    def list(self, request):
        """List Channels."""
        serializer = PageQuerySerializer(data=request.GET)
        if serializer.is_valid(raise_exception=True):
            page = serializer.validated_data.get("page")
            per_page = serializer.validated_data.get("per_page")

            try:
                org = request.user.organization
                channels = (
                    Channel.objects.filter(organizations=org)
                    .select_related("network")
                    .prefetch_related("organizations")
                    .order_by("create_ts")
                )
                paginator = Paginator(channels, per_page)
                page_obj = paginator.page(page)
                serialized = ChannelResponseSerializer(page_obj, many=True)
                response = ChannelListResponse(
                    {
                        "data": serialized.data,
                        "total": channels.count(),
                    }
                )
                return Response(ok(response.data), status=status.HTTP_200_OK)
            except Exception as exc:  # pragma: no cover - propagate detailed error
                return Response(err(exc.args), status=status.HTTP_400_BAD_REQUEST)

    @swagger_auto_schema(
        request_body=ChannelCreateBody,
        responses=with_common_response(
            {status.HTTP_201_CREATED: ChannelIDSerializer}
        ),
    )
    def create(self, request):
        """Create Channel."""
        try:
            # Garante que o agente Docker está rodando antes de iniciar o fluxo
            ensure_docker_agent_running(self._log_event)
            self._log_event(f"[API] Iniciando criação de canal para request: {request.data}")
            serializer = ChannelCreateBody(data=request.data)
            if serializer.is_valid(raise_exception=True):
                name = serializer.validated_data.get("name")
                peers = serializer.validated_data.get("peers")
                orderers = serializer.validated_data.get("orderers")

                # Se já existe no banco, aborta
                if Channel.objects.filter(name=name).exists():
                    self._log_event(f"[API] Canal '{name}' já existe no sistema. Criação abortada.", level="warning")
                    return Response(
                        err([f"Channel '{name}' já existe no sistema (interface). Criação abortada."]),
                        status=status.HTTP_400_BAD_REQUEST,
                    )

                # Se não existe no banco, mas já existe no Fabric, importa automaticamente e busca artefatos
                from api.lib.peer.channel import Channel as PeerChannel
                try:
                    peer_channel = PeerChannel()
                    code, fabric_channels = peer_channel.list()
                    if code == 0 and name in fabric_channels:
                        org = request.user.organization
                        channel = Channel(name=name, network=org.network)
                        channel.save()
                        channel.organizations.add(org)
                        # Associa o primeiro orderer informado, se houver
                        if orderers:
                            ordering_node = Node.objects.get(id=orderers[0])
                            channel.orderers.add(ordering_node)
                        # Buscar bloco de configuração do canal e salvar no storage
                        try:
                            # Tenta buscar o bloco config do canal
                            # Usa o primeiro peer e orderer disponíveis
                            peer_node = Node.objects.filter(organization=org, type="peer").first()
                            orderer_node = Node.objects.filter(id__in=orderers).first() if orderers else None
                            if peer_node and orderer_node:
                                from api.utils.common import init_env_vars
                                envs = init_env_vars(peer_node, org)
                                peer_channel_fetch = PeerChannel(**envs)
                                # Caminho para salvar o bloco config
                                block_path = channel.get_channel_artifacts_path("config_block.pb")
                                peer_channel_fetch.fetch(
                                    block_path=block_path,
                                    channel=name,
                                    orderer_general_url=f"{orderer_node.name}.{org.name.split('.', 1)[1]}:7050"
                                )
                        except Exception as fetch_exc:
                            self._log_event(f"[API] Falha ao buscar bloco de configuração do canal importado: {fetch_exc}", level="warning")
                        response = ChannelIDSerializer(data=channel.__dict__)
                        if response.is_valid(raise_exception=True):
                            try:
                                sync_gateway_artifacts()
                            except Exception as exc:  # pragma: no cover
                                self._log_event(
                                    f"[GATEWAY_SYNC] Falha ao sincronizar gateway após importar canal '{name}': {exc}",
                                    level="warning",
                                )
                            self._log_event(f"[API] Canal '{name}' importado do Fabric e registrado com sucesso.")
                            return Response(
                                ok(response.validated_data),
                                status=status.HTTP_201_CREATED,
                            )
                except Exception as e:
                    self._log_event(f"[API] Erro ao importar canal do Fabric: {e}", level="warning")
                    # Se erro ao importar, segue para o fluxo normal de criação
                    pass

                import shutil
                import time
                org = request.user.organization
                orderer_nodes = Node.objects.filter(id__in=orderers)
                peer_nodes = Node.objects.filter(id__in=peers)
                max_attempts = 2
                for attempt in range(max_attempts):
                    try:
                        # Garante que o agente Docker está rodando antes de cada tentativa
                        ensure_docker_agent_running(self._log_event)
                        # Recria materiais caso tenham sido limpos em tentativas anteriores
                        self._ensure_crypto_materials(org)
                        if attempt == 0:
                            self._log_event(f"[API] Tentando criar canal '{name}' (1ª tentativa)")
                        else:
                            self._log_event(f"[API] Tentando criar canal '{name}' após limpeza automática (2ª tentativa)")
                            # Feedback explícito para dashboard: limpeza automática foi acionada
                            self._log_event(f"[DASHBOARD] Limpeza automática de artefatos foi acionada para o canal '{name}' e organização '{org.name}'.", level="warning")
                        # validate if all nodes are running
                        validate_nodes(orderer_nodes)
                        validate_nodes(peer_nodes)

                        # assemble transaction config
                        _orderers, _peers = assemble_transaction_config(org)

                        ConfigTX(org.network.name).create(
                            name, org.network.consensus, _orderers, _peers
                        )
                        ConfigTxGen(org.network.name).genesis(
                            profile=name,
                            channelid=name,
                            outputblock="{}.block".format(name),
                        )

                        # osnadmin channel join
                        ordering_node = Node.objects.get(id=orderers[0])
                        try:
                            osn_channel_join(name, ordering_node, org)
                        except Exception as e:
                            self._log_event(f"[API] Erro ao executar osn_channel_join: {e}", level="warning")
                            ensure_docker_agent_running(self._log_event)
                            raise

                        # peer channel join
                        try:
                            peer_channel_join(name, peers, org)
                        except Exception as e:
                            self._log_event(f"[API] Erro ao executar peer_channel_join: {e}", level="warning")
                            ensure_docker_agent_running(self._log_event)
                            raise

                        # set anchor peer
                        anchor_peer = Node.objects.get(id=peers[0])
                        try:
                            set_anchor_peer(name, org, anchor_peer, ordering_node)
                        except Exception as e:
                            self._log_event(f"[API] Erro ao executar set_anchor_peer: {e}", level="warning")
                            ensure_docker_agent_running(self._log_event)
                            raise

                        # save channel to db
                        channel = Channel(name=name, network=org.network)
                        channel.save()
                        channel.organizations.add(org)
                        channel.orderers.add(ordering_node)

                        # serialize and return channel id
                        response = ChannelIDSerializer(data=channel.__dict__)
                        if response.is_valid(raise_exception=True):
                            try:
                                sync_gateway_artifacts()
                            except Exception as exc:  # pragma: no cover
                                self._log_event(
                                    f"[GATEWAY_SYNC] Falha ao sincronizar gateway após criação do canal '{name}': {exc}",
                                    level="warning",
                                )
                            return Response(
                                ok(response.validated_data),
                                status=status.HTTP_201_CREATED,
                            )
                    except Exception as e:
                        self._log_event(f"[API] Erro ao criar canal (tentativa {attempt+1}): {e}", level="warning")
                        # Limpeza automática de artefatos e retry
                        if attempt == 0:
                            try:
                                self._log_event(f"[API] Iniciando limpeza automática de artefatos para canal '{name}' e org '{org.name}'", level="warning")
                                # Limpa artefatos do canal e org
                                channel_path = f"{CELLO_HOME}/{org.network.name}"
                                org_crypto = f"{CELLO_HOME}/{org.name}/crypto-config"
                                if os.path.exists(channel_path):
                                    shutil.rmtree(channel_path)
                                if os.path.exists(org_crypto):
                                    shutil.rmtree(org_crypto)
                                # Reinicia agente docker se necessário
                                import subprocess
                                subprocess.run(["docker", "restart", "cello-docker-agent"], check=False)
                                time.sleep(2)
                                self._log_event(f"[API] Limpeza automática concluída para canal '{name}' e org '{org.name}'", level="info")
                            except Exception as cleanup_exc:
                                self._log_event(f"[API] Erro ao limpar artefatos: {cleanup_exc}", level="error")
                            continue
                        else:
                            self._log_event(f"[API] Falha crítica ao criar canal após limpeza automática: {e}", level="error")
                            # Feedback explícito para dashboard: ação manual sugerida
                            self._log_event(f"[DASHBOARD] Falha crítica ao criar canal após automação. Usuário deve verificar artefatos, certificados e agente Docker manualmente.", level="error")
                            return Response(
                                err(["Falha crítica ao criar canal após limpeza automática. Verifique artefatos, certificados e agente Docker.", str(e)]),
                                status=status.HTTP_400_BAD_REQUEST
                            )
            self._log_event(f"[API] Fim do processamento de criação de canal para request: {request.data}")
        except Exception as e:
            self._log_event(f"[API] Erro inesperado no fluxo de criação de canal: {e}", level="error")
            self._log_event(f"[DASHBOARD] Erro inesperado no backend. Ação manual pode ser necessária.", level="error")
            return Response(
                err(["Erro inesperado no backend. Verifique logs e dependências.", str(e)]),
                status=status.HTTP_500_INTERNAL_SERVER_ERROR
            )

    @swagger_auto_schema(
        responses=with_common_response(
            {status.HTTP_200_OK: ChannelResponseSerializer}
        ),
    )
    def retrieve(self, request, pk=None):
        """Retrieve channel."""
        try:
            channel = Channel.objects.get(id=pk)
            response = ChannelResponseSerializer(instance=channel)
            return Response(ok(response.data), status=status.HTTP_200_OK)

        except ObjectDoesNotExist:
            LOG.exception("channel not found")
            raise ResourceNotFound

    @swagger_auto_schema(
        request_body=ChannelUpdateSerializer,
        responses=with_common_response({status.HTTP_202_ACCEPTED: "Accepted"}),
    )
    def update(self, request, pk=None):
        """Update channel."""
        serializer = ChannelUpdateSerializer(data=request.data)
        if serializer.is_valid(raise_exception=True):
            channel = Channel.objects.get(id=pk)
            org = request.user.organization
            try:
                # Read uploaded file in cache without saving it on disk.
                file = request.FILES.get("data").read()
                json_data = file.decode("utf8").replace("'", '"')
                data = json.loads(json_data)
                msp_id = serializer.validated_data.get("msp_id")
                org_type = serializer.validated_data.get("org_type")
                # Validate uploaded config file
                try:
                    config = data["config"]["channel_group"]["groups"][
                        org_type
                    ]["groups"][msp_id]
                except KeyError:
                    LOG.exception("config file not found")
                    raise ResourceNotFound

                try:
                    # Read current channel config from local disk
                    with open(
                        channel.get_channel_artifacts_path(CFG_JSON),
                        "r",
                        encoding="utf-8",
                    ) as f:
                        LOG.info("load current config success")
                        current_config = json.load(f)
                except FileNotFoundError:
                    LOG.exception("current config file not found")
                    raise ResourceNotFound

                # Create a new org
                new_org = Organization.objects.create(
                    name=org.name,
                )
                LOG.info("new org created")
                updated_config = deepcopy(current_config)
                updated_config["channel_group"]["groups"]["Application"][
                    "groups"
                ][msp_id] = config
                LOG.info("update config success", updated_config)

                # Update and save the config with new org
                with open(
                    channel.get_channel_artifacts_path(UPDATED_CFG_JSON),
                    "w",
                    encoding="utf-8",
                ) as f:
                    LOG.info("save updated config success")
                    json.dump(updated_config, f, sort_keys=False)

                # Encode it into pb.
                ConfigTxLator().proto_encode(
                    input=channel.get_channel_artifacts_path(UPDATED_CFG_JSON),
                    type="common.Config",
                    output=channel.get_channel_artifacts_path(UPDATED_CFG_PB),
                )
                LOG.info("encode config to pb success")

                # Calculate the config delta between pb files
                ConfigTxLator().compute_update(
                    original=channel.get_channel_artifacts_path(CFG_PB),
                    updated=channel.get_channel_artifacts_path(UPDATED_CFG_PB),
                    channel_id=channel.name,
                    output=channel.get_channel_artifacts_path(DELTA_PB),
                )
                LOG.info("compute config delta success")
                # Decode the config delta pb into json
                config_update = ConfigTxLator().proto_decode(
                    input=channel.get_channel_artifacts_path(DELTA_PB),
                    type="common.ConfigUpdate",
                )
                LOG.info("decode config delta to json success")
                # Wrap the config update as envelope
                updated_config = {
                    "payload": {
                        "header": {
                            "channel_header": {
                                "channel_id": channel.name,
                                "type": 2,
                            }
                        },
                        "data": {"config_update": to_dict(config_update)},
                    }
                }
                with open(
                    channel.get_channel_artifacts_path(CFG_JSON),
                    "w",
                    encoding="utf-8",
                ) as f:
                    LOG.info("save config to json success")
                    json.dump(updated_config, f, sort_keys=False)

                # Encode the config update envelope into pb
                ConfigTxLator().proto_encode(
                    input=channel.get_channel_artifacts_path(CFG_JSON),
                    type="common.Envelope",
                    output=channel.get_channel_artifacts_path(
                        CFG_DELTA_ENV_PB
                    ),
                )
                LOG.info("Encode the config update envelope success")

                # Peers to send the update transaction
                nodes = Node.objects.filter(
                    organization=org,
                    type=FabricNodeType.Peer.name.lower(),
                    status=NodeStatus.Running.name.lower(),
                )

                for node in nodes:
                    dir_node = "{}/{}/crypto-config/peerOrganizations".format(
                        CELLO_HOME, org.name
                    )
                    env = {
                        "FABRIC_CFG_PATH": "{}/{}/peers/{}/".format(
                            dir_node, org.name, node.name + "." + org.name
                        ),
                    }
                    cli = PeerChannel(**env)
                    cli.signconfigtx(
                        channel.get_channel_artifacts_path(CFG_DELTA_ENV_PB)
                    )
                    LOG.info("Peers to send the update transaction success")

                # Save a new organization to db.
                new_org.save()
                LOG.info("new_org save success")
                return Response(ok(None), status=status.HTTP_202_ACCEPTED)
            except ObjectDoesNotExist:
                LOG.exception("channel not found")
                raise ResourceNotFound

    @swagger_auto_schema(
        responses=with_common_response({status.HTTP_200_OK: "Accepted"}),
    )
    @action(methods=["get"], detail=True, url_path="configs")
    def get_channel_org_config(self, request, pk=None):
        try:
            org = request.user.organization
            channel = Channel.objects.get(id=pk)
            peer = Node.objects.filter(
                organization=org,
                type=FabricNodeType.Peer.name.lower(),
                status=NodeStatus.Running.name.lower(),
            ).first()
            orderer = Node.objects.filter(
                organization=org,
                type=FabricNodeType.Orderer.name.lower(),
                status=NodeStatus.Running.name.lower()
            ).first()

            peer_channel_fetch(channel.name, org, peer, orderer)

            # Decode block to JSON
            ConfigTxLator().proto_decode(
                input=channel.get_channel_artifacts_path("config_block.pb"),
                type="common.Block",
                output=channel.get_channel_artifacts_path("config_block.json"),
            )

            # Get the config data from the block
            json_filter(
                input=channel.get_channel_artifacts_path("config_block.json"),
                output=channel.get_channel_artifacts_path("config.json"),
                expression=".data.data[0].payload.data.config"
            )

            # Prepare return data
            with open(channel.get_channel_artifacts_path("config.json"), 'r', encoding='utf-8') as f:
                data = {
                    "config": json.load(f),
                    "organization": org.name,
                    # TODO: create a method on Organization or Node to return msp_id
                    "msp_id": '{}'.format(org.name.split(".")[0].capitalize())
                }
            return Response(data=data, status=status.HTTP_200_OK)
        except ObjectDoesNotExist:
            LOG.exception("channel org not found")
            raise ResourceNotFound


def validate_nodes(nodes):
    """Validate if all nodes are running."""
    for node in nodes:
        if node.status != NodeStatus.Running.name.lower():
            raise NoResource("Node {} is not running".format(node.name))


def assemble_transaction_config(org):
    """Assemble transaction config for the channel."""
    _orderers = [{"name": org.name, "hosts": []}]
    _peers = [{"name": org.name, "hosts": []}]
    nodes = Node.objects.filter(organization=org)
    for node in nodes:
        if node.type == "peer":
            _peers[0]["hosts"].append({"name": node.name})
        elif node.type == "orderer":
            _orderers[0]["hosts"].append({"name": node.name})

    return _orderers, _peers


def osn_channel_join(name, ordering_node, org):
    """Join ordering node to the channel."""
    envs = init_env_vars(ordering_node, org)
    peer_channel_cli = PeerChannel(**envs)
    peer_channel_cli.create(
        channel=name,
        orderer_admin_url="{}.{}:{}".format(
            ordering_node.name, org.name.split(".", 1)[1], str(7053)
        ),
        block_path="{}/{}/{}.block".format(CELLO_HOME, org.network.name, name),
    )


def peer_channel_join(name, peers, org):
    """Join peer nodes to the channel."""
    for i in range(len(peers)):
        peer_node = Node.objects.get(id=peers[i])
        envs = init_env_vars(peer_node, org)
        peer_channel_cli = PeerChannel(**envs)
        peer_channel_cli.join(
            block_path="{}/{}/{}.block".format(
                CELLO_HOME, org.network.name, name
            )
        )


def set_anchor_peer(name, org, anchor_peer, ordering_node):
    """Set anchor peer for the channel."""
    org_msp = "{}".format(org.name.split(".", 1)[0].capitalize())
    channel_artifacts_path = "{}/{}".format(CELLO_HOME, org.network.name)

    # Fetch the channel block from the orderer
    peer_channel_fetch(name, org, anchor_peer, ordering_node)

    # Decode block to JSON
    ConfigTxLator().proto_decode(
        input="{}/config_block.pb".format(channel_artifacts_path),
        type="common.Block",
        output="{}/config_block.json".format(channel_artifacts_path),
    )

    # Get the config data from the block
    json_filter(
        input="{}/config_block.json".format(channel_artifacts_path),
        output="{}/config.json".format(channel_artifacts_path),
        expression=".data.data[0].payload.data.config",
    )

    # add anchor peer config
    anchor_peer_config = {
        "AnchorPeers": {
            "mod_policy": "Admins",
            "value": {
                "anchor_peers": [
                    {"host": anchor_peer.name + "." + org.name, "port": 7051}
                ]
            },
            "version": 0,
        }
    }

    json_add_anchor_peer(
        input="{}/config.json".format(channel_artifacts_path),
        output="{}/modified_config.json".format(channel_artifacts_path),
        anchor_peer_config=anchor_peer_config,
        org_msp=org_msp,
    )

    ConfigTxLator().proto_encode(
        input="{}/config.json".format(channel_artifacts_path),
        type="common.Config",
        output="{}/config.pb".format(channel_artifacts_path),
    )

    ConfigTxLator().proto_encode(
        input="{}/modified_config.json".format(channel_artifacts_path),
        type="common.Config",
        output="{}/modified_config.pb".format(channel_artifacts_path),
    )

    ConfigTxLator().compute_update(
        original="{}/config.pb".format(channel_artifacts_path),
        updated="{}/modified_config.pb".format(channel_artifacts_path),
        channel_id=name,
        output="{}/config_update.pb".format(channel_artifacts_path),
    )

    ConfigTxLator().proto_decode(
        input="{}/config_update.pb".format(channel_artifacts_path),
        type="common.ConfigUpdate",
        output="{}/config_update.json".format(channel_artifacts_path),
    )

    # Create config update envelope
    json_create_envelope(
        input="{}/config_update.json".format(channel_artifacts_path),
        output="{}/config_update_in_envelope.json".format(
            channel_artifacts_path
        ),
        channel=name,
    )

    ConfigTxLator().proto_encode(
        input="{}/config_update_in_envelope.json".format(
            channel_artifacts_path
        ),
        type="common.Envelope",
        output="{}/config_update_in_envelope.pb".format(
            channel_artifacts_path
        ),
    )

    # Update the channel of anchor peer
    peer_channel_update(
        name, org, anchor_peer, ordering_node, channel_artifacts_path
    )


def peer_channel_fetch(name, org, anchor_peer, ordering_node):
    """Fetch the channel block from the orderer."""
    PeerChannel(**{**init_env_vars(ordering_node, org), **init_env_vars(anchor_peer, org)}).fetch(
        block_path="{}/{}/config_block.pb".format(CELLO_HOME, org.network.name),
        channel=name, orderer_general_url="{}.{}:{}".format(
            ordering_node.name,
            org.name.split(".", 1)[1],
            str(7050)
        )
    )


def peer_channel_update(
    name, org, anchor_peer, ordering_node, channel_artifacts_path
):
    """Update the channel."""
    envs = init_env_vars(anchor_peer, org)
    peer_channel_cli = PeerChannel(**envs)
    peer_channel_cli.update(
        channel=name,
        channel_tx="{}/config_update_in_envelope.pb".format(
            channel_artifacts_path
        ),
        orderer_url="{}.{}:{}".format(
            ordering_node.name, org.name.split(".", 1)[1], str(7050)
        ),
    )
