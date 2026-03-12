#
# SPDX-License-Identifier: Apache-2.0
#
from rest_framework import viewsets, status
from rest_framework.response import Response
from rest_framework.decorators import action
from rest_framework.permissions import IsAuthenticated
import os
import subprocess
import json as jsonlib
import tempfile
import shutil
import tarfile
import json
from pathlib import Path

from drf_yasg.utils import swagger_auto_schema
from api.config import FABRIC_CHAINCODE_STORE
from api.config import CELLO_HOME
from api.models import Node, ChainCode, Channel
from api.utils.common import make_uuid
from django.core.paginator import Paginator

from api.lib.peer.chaincode import ChainCode as PeerChainCode
from api.common.serializers import PageQuerySerializer
from api.utils.common import with_common_response, init_env_vars
from api.exceptions import ResourceNotFound

from api.routes.chaincode.serializers import (
    ChainCodePackageBody,
    ChainCodeIDSerializer,
    ChainCodeCommitBody,
    ChainCodeApproveForMyOrgBody,
    ChaincodeListResponse,
)
from api.common import ok, err
import hashlib
import logging


LOG = logging.getLogger(__name__)


class ChainCodeViewSet(viewsets.ViewSet):
    """Class represents Channel related operations."""

    permission_classes = [
        IsAuthenticated,
    ]

    def _resolve_chaincode_creator(self, user):
        organization = getattr(user, "organization", None)
        organization_name = str(getattr(organization, "name", "") or "").strip()
        if organization_name:
            return organization_name

        username = str(getattr(user, "username", "") or "").strip()
        if username:
            return username

        email = str(getattr(user, "email", "") or "").strip()
        if email:
            return email

        return str(getattr(user, "pk", "") or "").strip()

    def _read_cc_pkg(self, pk, filename, ccpackage_path):
        """
        read and extract chaincode package meta info
        :pk: chaincode id
        :filename: uploaded chaincode package filename
        :ccpackage_path: chaincode package path
        """
        try:
            meta_path = os.path.join(ccpackage_path, "metadata.json")
            # extract metadata file
            with tarfile.open(
                os.path.join(ccpackage_path, filename)
            ) as tared_file:
                metadata_file = None
                for member in tared_file.getmembers():
                    if member.name.endswith("metadata.json"):
                        metadata_file = member
                        break

                if metadata_file is not None:
                    # Extract the metadata file
                    metadata_content = (
                        tared_file.extractfile(metadata_file)
                        .read()
                        .decode("utf-8")
                    )
                    metadata = json.loads(metadata_content)
                    language = metadata["type"]
                    label = metadata["label"]

            if os.path.exists(meta_path):
                os.remove(meta_path)

            chaincode = ChainCode.objects.get(id=pk)
            chaincode.package_id = chaincode.package_id
            chaincode.language = language
            chaincode.label = label
            chaincode.save()

        except Exception as e:
            LOG.exception("Could not read Chaincode Package")
            raise e

    def _build_package_upload_payload(
        self,
        package_id,
        label,
        language,
        file_name,
        package_path,
        already_exists=False,
    ):
        safe_package_path = str(package_path or "").strip()
        return {
            "package_id": str(package_id or "").strip(),
            "label": str(label or "").strip(),
            "language": str(language or "").strip(),
            "file_name": str(file_name or "").strip(),
            "artifact_path": safe_package_path,
            "artifact_ref": "local-file:{}".format(safe_package_path)
            if safe_package_path
            else "",
            "already_exists": bool(already_exists),
        }

    def _is_package_installed(
        self, peer_channel_cli, package_id, timeout="5s"
    ):
        """
        Check if a given package_id already exists on the peer so installs
        become idempotent (useful for automation re-runs).
        """
        try:
            res, payload = peer_channel_cli.lifecycle_query_installed(timeout)
        except Exception as exc:  # noqa: BLE001 - log and continue best-effort
            LOG.warning(
                "Unable to query installed chaincodes before install: %s", exc
            )
            return False

        if res != 0 or not isinstance(payload, dict):
            LOG.warning(
                "Peer returned rc=%s while listing installed chaincodes: %s",
                res,
                payload,
            )
            return False

        installed_chaincodes = payload.get("installed_chaincodes") or []
        return any(
            cc.get("package_id") == package_id for cc in installed_chaincodes
        )

    @swagger_auto_schema(
        query_serializer=PageQuerySerializer,
        responses=with_common_response(
            {status.HTTP_201_CREATED: ChaincodeListResponse}
        ),
    )
    def list(self, request):
        """
        List Chaincodes
        :param request: org_id
        :return: chaincode list
        :rtype: list
        """
        serializer = PageQuerySerializer(data=request.GET)
        if serializer.is_valid(raise_exception=True):
            page = serializer.validated_data.get("page")
            per_page = serializer.validated_data.get("per_page")

            try:
                creator_scope = self._resolve_chaincode_creator(request.user)
                chaincodes = ChainCode.objects.filter(
                    creator=creator_scope
                ).order_by("create_ts")
                p = Paginator(chaincodes, per_page)
                chaincodes_pages = p.page(page)
                chanincodes_list = [
                    {
                        "id": chaincode.id,
                        "package_id": chaincode.package_id,
                        "label": chaincode.label,
                        "creator": chaincode.creator,
                        "language": chaincode.language,
                        "create_ts": chaincode.create_ts,
                        "description": chaincode.description,
                    }
                    for chaincode in chaincodes_pages
                ]
                response = ChaincodeListResponse(
                    {"data": chanincodes_list, "total": chaincodes.count()}
                )
                return Response(
                    data=ok(response.data), status=status.HTTP_200_OK
                )
            except Exception as e:
                return Response(
                    err(e.args), status=status.HTTP_400_BAD_REQUEST
                )

    @swagger_auto_schema(
        method="post",
        query_serializer=PageQuerySerializer,
        responses=with_common_response(
            {status.HTTP_201_CREATED: ChainCodeIDSerializer}
        ),
    )
    @action(detail=False, methods=["post"], url_path="chaincodeRepo")
    def package(self, request):
        serializer = ChainCodePackageBody(data=request.data)
        if serializer.is_valid(raise_exception=True):
            file = serializer.validated_data.get("file")
            description = serializer.validated_data.get("description")
            uuid = make_uuid()
            temp_cc_path = ""
            try:
                fd, temp_cc_path = tempfile.mkstemp()
                # try to calculate packageid
                with os.fdopen(fd, "wb") as f:
                    for chunk in file.chunks():
                        f.write(chunk)

                with tarfile.open(temp_cc_path, "r:gz") as tar:
                    # Locate the metadata file
                    metadata_file = None
                    for member in tar.getmembers():
                        if member.name.endswith("metadata.json"):
                            metadata_file = member
                            break

                    if metadata_file is not None:
                        # Extract the metadata file
                        metadata_content = (
                            tar.extractfile(metadata_file)
                            .read()
                            .decode("utf-8")
                        )
                        metadata = json.loads(metadata_content)
                        label = metadata.get("label")
                        language = metadata.get("type")
                        if not label:
                            return Response(
                                err("Chaincode package metadata.label is required."),
                                status=status.HTTP_400_BAD_REQUEST,
                            )
                        if not language:
                            language = ""
                    else:
                        return Response(
                            err(
                                "Metadata file not found in the chaincode package."
                            ),
                            status=status.HTTP_400_BAD_REQUEST,
                        )

                creator_scope = self._resolve_chaincode_creator(request.user)
                # qs = Node.objects.filter(type="peer", organization=org)
                # if not qs.exists():
                #     return Response(
                #         err("at least 1 peer node is required for the chaincode package upload."),
                #         status=status.HTTP_400_BAD_REQUEST
                #     )
                # peer_node = qs.first()
                # envs = init_env_vars(peer_node, org)
                # peer_channel_cli = PeerChainCode("v2.5.10", **envs)
                # return_code, content = peer_channel_cli.lifecycle_calculatepackageid(temp_cc_path)
                # if (return_code != 0):
                #     return Response(
                #         err("calculate packageid failed for {}.".format(content)),
                #         status=status.HTTP_400_BAD_REQUEST
                #     )
                # packageid = content.strip()

                # manually calculate the package id
                sha256_hash = hashlib.sha256()
                with open(temp_cc_path, "rb") as f:
                    for byte_block in iter(lambda: f.read(4096), b""):
                        sha256_hash.update(byte_block)
                packageid = label + ":" + sha256_hash.hexdigest()

                ccpackage_path = os.path.join(FABRIC_CHAINCODE_STORE, packageid)
                if not os.path.exists(ccpackage_path):
                    os.makedirs(ccpackage_path)
                ccpackage = os.path.join(ccpackage_path, file.name)

                existing_chaincode = ChainCode.objects.filter(package_id=packageid).first()
                if existing_chaincode:
                    if not os.path.exists(ccpackage):
                        shutil.copy(temp_cc_path, ccpackage)
                    changed = False
                    if label and existing_chaincode.label != label:
                        existing_chaincode.label = label
                        changed = True
                    if language and existing_chaincode.language != language:
                        existing_chaincode.language = language
                        changed = True
                    if description and existing_chaincode.description != description:
                        existing_chaincode.description = description
                        changed = True
                    if changed:
                        existing_chaincode.save()
                    return Response(
                        ok(
                            self._build_package_upload_payload(
                                package_id=packageid,
                                label=label,
                                language=language,
                                file_name=file.name,
                                package_path=ccpackage,
                                already_exists=True,
                            )
                        ),
                        status=status.HTTP_200_OK,
                    )

                chaincode = ChainCode(
                    id=uuid,
                    package_id=packageid,
                    label=label,
                    language=language,
                    creator=creator_scope,
                    description=description,
                )
                chaincode.save()

                shutil.copy(temp_cc_path, ccpackage)
                return Response(
                    ok(
                        self._build_package_upload_payload(
                            package_id=packageid,
                            label=label,
                            language=language,
                            file_name=file.name,
                            package_path=ccpackage,
                        )
                    ),
                    status=status.HTTP_200_OK,
                )
            except Exception as e:
                return Response(
                    err(e.args), status=status.HTTP_400_BAD_REQUEST
                )
            finally:
                if temp_cc_path and os.path.exists(temp_cc_path):
                    os.remove(temp_cc_path)

    @swagger_auto_schema(
        method="post",
        responses=with_common_response(
            {status.HTTP_201_CREATED: ChainCodeIDSerializer}
        ),
    )
    @action(detail=False, methods=["post"])
    def install(self, request):
        chaincode_identifier = request.data.get("id")
        # Get the selected node ID from request
        node_id = request.data.get("node")
        try:
            if not chaincode_identifier:
                return Response(
                    err("chaincode identifier is required."),
                    status=status.HTTP_400_BAD_REQUEST,
                )

            package_id = chaincode_identifier
            cc_dir = Path(FABRIC_CHAINCODE_STORE) / package_id

            if not cc_dir.exists():
                chaincode_obj = ChainCode.objects.filter(
                    package_id=chaincode_identifier
                ).first()
                if not chaincode_obj:
                    chaincode_obj = ChainCode.objects.filter(
                        id=chaincode_identifier
                    ).first()
                if not chaincode_obj:
                    return Response(
                        err(
                            f"chaincode package '{chaincode_identifier}' not found."
                        ),
                        status=status.HTTP_404_NOT_FOUND,
                    )
                package_id = chaincode_obj.package_id
                cc_dir = Path(FABRIC_CHAINCODE_STORE) / package_id

            if not cc_dir.exists():
                return Response(
                    err(
                        f"chaincode package '{package_id}' is missing from storage."
                    ),
                    status=status.HTTP_404_NOT_FOUND,
                )

            cc_targz = next(
                (str(p.resolve()) for p in cc_dir.iterdir() if p.suffix == ".gz"),
                None,
            )
            if not cc_targz:
                return Response(
                    err(
                        f"chaincode archive for '{package_id}' is missing. "
                        "Upload the package again."
                    ),
                    status=status.HTTP_404_NOT_FOUND,
                )

            org = request.user.organization

            # If node_id is provided, get that specific node
            if node_id:
                try:
                    peer_node = Node.objects.get(
                        id=node_id, type="peer", organization=org
                    )
                except Node.DoesNotExist:
                    return Response(
                        err("Selected peer node not found or not authorized."),
                        status=status.HTTP_404_NOT_FOUND,
                    )
            else:
                # Fallback to first peer if no node selected
                qs = Node.objects.filter(type="peer", organization=org)
                if not qs.exists():
                    raise ResourceNotFound
                peer_node = qs.first()

            envs = init_env_vars(peer_node, org)
            peer_channel_cli = PeerChainCode(**envs)
            if self._is_package_installed(peer_channel_cli, package_id):
                LOG.info(
                    "Chaincode %s already present on peer %s. Skipping install.",
                    package_id,
                    peer_node.name,
                )
                return Response(
                    ok("chaincode already installed on selected peer"),
                    status=status.HTTP_200_OK,
                )

            res_code, stdout, stderr = peer_channel_cli.lifecycle_install(
                cc_targz
            )
            if res_code != 0:
                detail = stderr.strip() or stdout.strip() or ""
                LOG.warning(
                    "Failed to install chaincode %s on %s: %s",
                    package_id,
                    peer_node.name,
                    detail,
                )
                return Response(
                    err(detail or "install chaincode failed."),
                    status=status.HTTP_400_BAD_REQUEST,
                )
        except Exception as e:
            return Response(err(e.args), status=status.HTTP_400_BAD_REQUEST)
        return Response(ok("success"), status=status.HTTP_200_OK)

    @swagger_auto_schema(
        method="get",
        responses=with_common_response(
            {status.HTTP_201_CREATED: ChainCodeIDSerializer}
        ),
    )
    @action(detail=False, methods=["get"])
    def query_installed(self, request):
        try:
            org = request.user.organization
            qs = Node.objects.filter(type="peer", organization=org)
            if not qs.exists():
                raise ResourceNotFound("Peer Does Not Exist")
            peer_node = qs.first()
            envs = init_env_vars(peer_node, org)

            timeout = "5s"
            peer_channel_cli = PeerChainCode(**envs)
            res, installed_chaincodes = (
                peer_channel_cli.lifecycle_query_installed(timeout)
            )
            if res != 0:
                return Response(
                    err("query installed chaincode failed."),
                    status=status.HTTP_400_BAD_REQUEST,
                )
        except Exception as e:
            return Response(err(e.args), status=status.HTTP_400_BAD_REQUEST)
        return Response(ok(installed_chaincodes), status=status.HTTP_200_OK)

    @swagger_auto_schema(
        method="get",
        responses=with_common_response(
            {status.HTTP_201_CREATED: ChainCodeIDSerializer}
        ),
    )
    @action(detail=False, methods=["get"])
    def get_installed_package(self, request):
        try:
            org = request.user.organization
            qs = Node.objects.filter(type="peer", organization=org)
            if not qs.exists():
                raise ResourceNotFound("Peer Does Not Exist")
            peer_node = qs.first()
            envs = init_env_vars(peer_node, org)

            timeout = "5s"
            peer_channel_cli = PeerChainCode(**envs)
            res = peer_channel_cli.lifecycle_get_installed_package(timeout)
            if res != 0:
                return Response(
                    err("get installed package failed."),
                    status=status.HTTP_400_BAD_REQUEST,
                )

        except Exception as e:
            return Response(err(e.args), status=status.HTTP_400_BAD_REQUEST)
        return Response(ok("success"), status=status.HTTP_200_OK)

    @swagger_auto_schema(
        method="post",
        responses=with_common_response(
            {status.HTTP_201_CREATED: ChainCodeIDSerializer}
        ),
    )
    @action(detail=False, methods=["post"])
    def approve_for_my_org(self, request):
        serializer = ChainCodeApproveForMyOrgBody(data=request.data)
        if serializer.is_valid(raise_exception=True):
            try:
                channel_name = serializer.validated_data.get("channel_name")
                chaincode_name = serializer.validated_data.get(
                    "chaincode_name"
                )
                chaincode_version = serializer.validated_data.get(
                    "chaincode_version"
                )
                policy = serializer.validated_data.get("policy", "")
                sequence = serializer.validated_data.get("sequence")
                init_flag = serializer.validated_data.get("init_flag", False)

                org = request.user.organization
                qs = Node.objects.filter(type="orderer", organization=org)
                if not qs.exists():
                    raise ResourceNotFound("Orderer Does Not Exist")
                orderer_node = qs.first()
                orderer_url = (
                    orderer_node.name
                    + "."
                    + org.name.split(".", 1)[1]
                    + ":"
                    + str(7050)
                )
                orderer_tls_dir = os.path.join(
                    CELLO_HOME,
                    org.name,
                    "crypto-config",
                    "ordererOrganizations",
                    org.name.split(".", 1)[1],
                    "orderers",
                    orderer_node.name + "." + org.name.split(".", 1)[1],
                    "msp",
                    "tlscacerts",
                )
                orderer_tls_root_cert = ""
                if os.path.isdir(orderer_tls_dir):
                    for _, _, files in os.walk(orderer_tls_dir):
                        if files:
                            orderer_tls_root_cert = os.path.join(
                                orderer_tls_dir, files[0]
                            )
                            break
                if not orderer_tls_root_cert:
                    raise ResourceNotFound(
                        "Orderer TLS CA not found for {}".format(
                            orderer_node.name
                        )
                    )

                qs = Node.objects.filter(type="peer", organization=org)
                if not qs.exists():
                    raise ResourceNotFound("Peer Does Not Exist")
                peer_node = qs.first()
                envs = init_env_vars(peer_node, org)
                envs["ORDERER_CA"] = orderer_tls_root_cert

                peer_channel_cli = PeerChainCode(**envs)
                code, content = peer_channel_cli.lifecycle_approve_for_my_org(
                    orderer_url,
                    channel_name,
                    chaincode_name,
                    chaincode_version,
                    sequence,
                    policy,
                    init_flag,
                )
                if code != 0:
                    return Response(
                        err(
                            " lifecycle_approve_for_my_org failed. err: "
                            + content
                        ),
                        status=status.HTTP_400_BAD_REQUEST,
                    )
            except Exception as e:
                return Response(
                    err(e.args), status=status.HTTP_400_BAD_REQUEST
                )
            return Response(ok("success"), status=status.HTTP_200_OK)

    @swagger_auto_schema(
        method="get",
        responses=with_common_response(
            {status.HTTP_201_CREATED: ChainCodeIDSerializer}
        ),
    )
    @action(detail=False, methods=["get"])
    def query_approved(self, request):
        try:
            org = request.user.organization
            qs = Node.objects.filter(type="peer", organization=org)
            if not qs.exists():
                raise ResourceNotFound("Peer Does Not Exist")
            peer_node = qs.first()
            envs = init_env_vars(peer_node, org)

            channel_name = request.data.get("channel_name")
            cc_name = request.data.get("chaincode_name")

            peer_channel_cli = PeerChainCode(**envs)
            code, content = peer_channel_cli.lifecycle_query_approved(
                channel_name, cc_name
            )
            if code != 0:
                return Response(
                    err("query_approved failed."),
                    status=status.HTTP_400_BAD_REQUEST,
                )

        except Exception as e:
            return Response(err(e.args), status=status.HTTP_400_BAD_REQUEST)
        return Response(ok(content), status=status.HTTP_200_OK)

    @swagger_auto_schema(
        method="post",
        responses=with_common_response(
            {status.HTTP_201_CREATED: ChainCodeIDSerializer}
        ),
    )
    @action(detail=False, methods=["post"])
    def check_commit_readiness(self, request):
        serializer = ChainCodeApproveForMyOrgBody(data=request.data)
        if serializer.is_valid(raise_exception=True):
            try:
                channel_name = serializer.validated_data.get("channel_name")
                chaincode_name = serializer.validated_data.get(
                    "chaincode_name"
                )
                chaincode_version = serializer.validated_data.get(
                    "chaincode_version"
                )
                sequence = serializer.validated_data.get("sequence")
                org = request.user.organization
                # Build CLI env with ORDERER_CA present
                peer_qs = Node.objects.filter(type="peer", organization=org)
                if not peer_qs.exists():
                    raise ResourceNotFound("Peer Does Not Exist")
                peer_node = peer_qs.first()

                orderer_qs = Node.objects.filter(type="orderer", organization=org)
                if not orderer_qs.exists():
                    raise ResourceNotFound("Orderer Does Not Exist")

                envs = init_env_vars(peer_node, org)
                envs.update(init_env_vars(orderer_qs.first(), org))

                peer_channel_cli = PeerChainCode(**envs)
                # Wrapper reads TLS vars from env; only pass channel/name/version/sequence
                code, content = peer_channel_cli.lifecycle_check_commit_readiness(
                    channel_name,
                    chaincode_name,
                    chaincode_version,
                    sequence,
                )
                if code != 0:
                    return Response(
                        err("check_commit_readiness failed."),
                        status=status.HTTP_400_BAD_REQUEST,
                    )

            except Exception as e:
                return Response(
                    err(e.args), status=status.HTTP_400_BAD_REQUEST
                )
            return Response(ok(content), status=status.HTTP_200_OK)

    def _get_orderer_url(self, org):
        qs = Node.objects.filter(type="orderer", organization=org)
        if not qs.exists():
            raise ResourceNotFound("Orderer Does Not Exist")
        return (
            qs.first().name + "." + org.name.split(".", 1)[1] + ":" + str(7050)
        )

    def _get_peer_channel_cli(self, org):
        qs = Node.objects.filter(type="peer", organization=org)
        if not qs.exists():
            raise ResourceNotFound("Peer Does Not Exist")
        envs = init_env_vars(qs.first(), org)
        orderer_qs = Node.objects.filter(type="orderer", organization=org)
        if not orderer_qs.exists():
            raise ResourceNotFound("Orderer Does Not Exist")
        orderer_envs = init_env_vars(orderer_qs.first(), org)
        # ensure ORDERER_CA (and related TLS vars) are available for lifecycle commands
        envs.update(orderer_envs)

        if not envs.get("ORDERER_CA"):
            org_domain = org.name.split(".", 1)[1]
            orderer_node = orderer_qs.first()
            orderer_tls_dir = (
                Path(CELLO_HOME)
                / org.name
                / "crypto-config"
                / "ordererOrganizations"
                / org_domain
                / "orderers"
                / f"{orderer_node.name}.{org_domain}"
                / "msp"
                / "tlscacerts"
            )
            try:
                envs["ORDERER_CA"] = next(orderer_tls_dir.glob("*.pem")).as_posix()
            except StopIteration:
                raise ResourceNotFound("Orderer TLS CA not found")

        LOG.info("Peer channel CLI env resolved ORDERER_CA=%s", envs["ORDERER_CA"])
        return PeerChainCode(**envs)

    def _get_approved_organizations_by_channel_and_chaincode(
        self,
        peer_channel_cli,
        channel_name,
        chaincode_name,
        chaincode_version,
        sequence,
    ):
        code, readiness_result = (
            peer_channel_cli.lifecycle_check_commit_readiness(
                channel_name, chaincode_name, chaincode_version, sequence
            )
        )
        if code != 0:
            raise Exception(
                f"Check commit readiness failed: {readiness_result}"
            )

        # Check approved status
        approvals = readiness_result.get("approvals", {})
        approved_msps = [
            org_msp for org_msp, approved in approvals.items() if approved
        ]
        if not approved_msps:
            raise Exception("No organizations have approved this chaincode")

        LOG.info(f"Approved organizations: {approved_msps}")

        try:
            channel = Channel.objects.get(name=channel_name)
            channel_orgs = channel.organizations.all()
        except Channel.DoesNotExist:
            raise Exception(f"Channel {channel_name} not found")

        # find the corresponding organization by MSP ID
        # MSP ID format: Org1MSP, Org2MSP -> organization name format: org1.xxx, org2.xxx
        approved_orgs = []
        for msp_id in approved_msps:
            if msp_id.endswith("MSP"):
                org_prefix = msp_id[
                    :-3
                ].lower()  # remove "MSP" and convert to lowercase
                # find the corresponding organization in the channel
                for channel_org in channel_orgs:
                    if channel_org.name.split(".")[0] == org_prefix:
                        approved_orgs.append(channel_org)
                        LOG.info(
                            f"Found approved organization: {channel_org.name} (MSP: {msp_id})"
                        )
                        break

        if not approved_orgs:
            raise Exception("No approved organizations found in this channel")
        return approved_orgs

    def _get_peer_addresses_and_certs_by_organizations(self, orgs):
        addresses = []
        certs = []
        for org in orgs:
            qs = Node.objects.filter(type="peer", organization=org)
            if not qs.exists():
                LOG.warning(
                    f"No peer nodes found for organization: {org.name}"
                )
                continue

            # select the first peer node for each organization
            peer = qs.first()
            peer_tls_cert = "{}/{}/crypto-config/peerOrganizations/{}/peers/{}/tls/ca.crt".format(
                CELLO_HOME, org.name, org.name, peer.name + "." + org.name
            )
            peer_address = peer.name + "." + org.name + ":" + str(7051)
            LOG.info(f"Added peer from org {org.name}: {peer_address}")

            addresses.append(peer_address)
            certs.append(peer_tls_cert)

        if not addresses:
            raise Exception("No peer nodes found for specified organizations")
        return addresses, certs

    @swagger_auto_schema(
        method="post",
        responses=with_common_response(
            {status.HTTP_201_CREATED: ChainCodeIDSerializer}
        ),
    )
    @action(detail=False, methods=["post"])
    def commit(self, request):
        serializer = ChainCodeCommitBody(data=request.data)
        if serializer.is_valid(raise_exception=True):
            try:
                channel_name = serializer.validated_data.get("channel_name")
                chaincode_name = serializer.validated_data.get(
                    "chaincode_name"
                )
                chaincode_version = serializer.validated_data.get(
                    "chaincode_version"
                )
                policy = serializer.validated_data.get("policy")
                sequence = serializer.validated_data.get("sequence")
                init_flag = serializer.validated_data.get("init_flag", False)
                org = request.user.organization

                orderer_url = self._get_orderer_url(org)

                # Step 1: Check commit readiness, find all approved organizations
                peer_channel_cli = self._get_peer_channel_cli(org)
                approved_organizations = (
                    self._get_approved_organizations_by_channel_and_chaincode(
                        peer_channel_cli,
                        channel_name,
                        chaincode_name,
                        chaincode_version,
                        sequence,
                    )
                )

                # Step 2: Get peer nodes and root certs
                peer_address_list, peer_root_certs = (
                    self._get_peer_addresses_and_certs_by_organizations(
                        approved_organizations
                    )
                )

                # Step 3: Commit chaincode
                code = peer_channel_cli.lifecycle_commit(
                    orderer_url,
                    channel_name,
                    chaincode_name,
                    chaincode_version,
                    sequence,
                    policy,
                    peer_address_list,
                    peer_root_certs,
                    init_flag,
                )
                if code != 0:
                    return Response(
                        err("Commit chaincode failed"),
                        status=status.HTTP_400_BAD_REQUEST,
                    )

                LOG.info(f"Chaincode {chaincode_name} committed successfully")

                # Step 4: Query committed chaincode
                code, committed_result = (
                    peer_channel_cli.lifecycle_query_committed(
                        channel_name, chaincode_name
                    )
                )
                if code == 0:
                    LOG.info(committed_result)
                    self._update_gateway_identity(org)
                    self._deploy_ccapi_gateway(
                        chaincode_name, channel_name
                    )
                    return Response(
                        ok(committed_result), status=status.HTTP_200_OK
                    )
                else:
                    return Response(
                        err("Query committed failed."),
                        status=status.HTTP_400_BAD_REQUEST,
                    )

            except Exception as e:
                LOG.error(f"Commit chaincode failed: {str(e)}")
                return Response(
                    err(f"Commit chaincode failed: {str(e)}"),
                    status=status.HTTP_400_BAD_REQUEST,
                )

    def _deploy_ccapi_gateway(self, chaincode_name, channel_name):
        compose_path = self._resolve_gateway_path(
            "CCAPI_COMPOSE_FILE", "docker-compose.yaml"
        )
        service_name = os.getenv(
            "CCAPI_SERVICE_NAME", "chaincode-gateway"
        )
        if compose_path is None:
            LOG.info(
                "CCAPI autodeploy skipped: unable to resolve compose file path"
            )
            return
        compose_path = compose_path.expanduser().resolve()
        if not compose_path.exists():
            LOG.warning(
                "CCAPI autodeploy skipped: compose file %s does not exist",
                compose_path,
            )
            return

        cmd_options = [
            [
                "docker",
                "compose",
                "-f",
                str(compose_path),
                "up",
                "-d",
                "--build",
                service_name,
            ],
            [
                "docker-compose",
                "-f",
                str(compose_path),
                "up",
                "-d",
                "--build",
                service_name,
            ],
        ]

        for cmd in cmd_options:
            try:
                completed = subprocess.run(
                    cmd,
                    check=True,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    text=True,
                )
                LOG.info(
                    "CCAPI redeploy triggered for %s (%s). Output: %s",
                    chaincode_name,
                    channel_name,
                    completed.stdout.strip(),
                )
                if completed.stderr.strip():
                    LOG.debug("CCAPI redeploy stderr: %s", completed.stderr)
                return
            except FileNotFoundError:
                LOG.debug("Command not found: %s", cmd[0])
                continue
            except subprocess.CalledProcessError as exc:
                LOG.error(
                    "CCAPI redeploy failed via %s: %s",
                    cmd[0],
                    exc.stderr,
                )
                return

        LOG.warning(
            "CCAPI redeploy skipped: docker compose binaries not available"
        )

    def _update_gateway_identity(self, org):
        identities_path = self._resolve_gateway_path(
            "CCAPI_IDENTITIES_FILE", "identities.json"
        )
        if identities_path is None:
            LOG.info(
                "CCAPI identities update skipped: unable to resolve identities path"
            )
            return

        gateway_root = os.getenv("CHAINCODE_GATEWAY_DIR")
        gateway_root_path = (
            Path(gateway_root).expanduser().resolve()
            if gateway_root
            else None
        )
        if gateway_root_path is None:
            default_root = self._resolve_gateway_path(None)
            if default_root is not None:
                gateway_root_path = default_root.expanduser().resolve()
        container_storage = Path(
            os.getenv("CCAPI_CONTAINER_STORAGE", "/opt/cello")
        ).expanduser().resolve()

        def _gateway_path(path_like):
            path_obj = Path(path_like).expanduser().resolve()
            try:
                path_obj.relative_to(container_storage)
                return str(path_obj).replace("\\", "/")
            except ValueError:
                if gateway_root_path:
                    try:
                        rel = path_obj.relative_to(gateway_root_path)
                        rel_str = str(rel).replace("\\", "/")
                        return f"/workspace/{rel_str}"
                    except ValueError:
                        pass
            return str(path_obj).replace("\\", "/")

        def _normalize_container(path_str):
            if not path_str:
                return path_str
            _p = path_str.replace("\\", "/")
            if _p.startswith("./"):
                _p = _p[2:]
            if not _p.startswith("/"):
                _p = f"/workspace/{_p}"
            return _p

        try:
            identities_path = identities_path.expanduser().resolve()
            current_data = {}
            if os.path.exists(identities_path):
                try:
                    with open(identities_path, "r", encoding="utf-8") as fh:
                        current_data = jsonlib.load(fh)
                except jsonlib.JSONDecodeError:
                    LOG.warning("Could not parse %s; starting fresh", identities_path)

            org_key = org.name.split(".")[0]
            domain = org.name.split(".", 1)[1] if "." in org.name else org.name

            peer_qs = Node.objects.filter(type="peer", organization=org)
            if not peer_qs.exists():
                raise ResourceNotFound("Peer Does Not Exist")
            peer_node = peer_qs.first()
            orderer_qs = Node.objects.filter(type="orderer", organization=org)
            if not orderer_qs.exists():
                raise ResourceNotFound("Orderer Does Not Exist")
            orderer_node = orderer_qs.first()

            admin_path = (
                container_storage
                / org.name
                / "crypto-config"
                / "peerOrganizations"
                / org.name
                / "users"
                / f"Admin@{org.name}"
                / "msp"
            )
            cert_path = admin_path / "signcerts"
            key_path = admin_path / "keystore"

            def _first_file(directory):
                for root, _, files in os.walk(directory):
                    for f in files:
                        return os.path.join(root, f)
                raise FileNotFoundError(directory)

            entry = current_data.get(org_key, {})
            template = os.getenv("CCAPI_CCP_TEMPLATE")
            if template:
                entry["ccpPath"] = template.format(
                    org=org_key, domain=domain, full=org.name
                )
            elif "ccpPath" not in entry:
                default_ccp = os.getenv("CCAPI_DEFAULT_CCP")
                if default_ccp:
                    entry["ccpPath"] = default_ccp.format(
                        org=org_key, domain=domain, full=org.name
                    )
            entry.setdefault("mspId", f"{org_key.capitalize()}MSP")
            entry["certPath"] = _normalize_container(
                _gateway_path(_first_file(cert_path))
            )
            entry["keyPath"] = _normalize_container(
                _gateway_path(_first_file(key_path))
            )

            if "ccpPath" not in entry:
                LOG.warning(
                    "No connection profile template set; entry for org %s lacks ccpPath",
                    org_key,
                )

            self._ensure_connection_profile(
                org,
                org_key,
                domain,
                entry,
                container_storage,
                gateway_root_path,
                _gateway_path,
                peer_node,
                orderer_node,
            )

            if "ccpPath" in entry:
                entry["ccpPath"] = _normalize_container(entry["ccpPath"])

            current_data[org_key] = entry
            if "org1" in current_data:
                snapshot = jsonlib.dumps(current_data["org1"])
                if "connection-org1.json" in snapshot:
                    del current_data["org1"]
            identities_path.parent.mkdir(parents=True, exist_ok=True)
            with identities_path.open("w", encoding="utf-8") as fh:
                jsonlib.dump(current_data, fh, indent=2)
            LOG.info("Updated gateway identities for org %s", org_key)
        except Exception as exc:
            LOG.error("Failed to update gateway identities: %s", exc)

    def _resolve_gateway_path(self, env_var, relative_name=None):
        value = None
        if env_var:
            value = os.getenv(env_var)
        if value:
            return Path(value)

        base_dir = os.getenv("CHAINCODE_GATEWAY_DIR")
        if base_dir:
            base = Path(base_dir)
            if relative_name:
                return base / relative_name
            return base

        root_path = os.getenv("ROOT_PATH")
        if root_path:
            base = Path(root_path)
        else:
            base = Path(__file__).resolve()
            for _ in range(6):
                if base.parent == base:
                    break
                base = base.parent

        gateway_root = base / "chaincode-gateway"
        if relative_name:
            rel_path = Path(relative_name)
            if not rel_path.is_absolute() and rel_path.parts and rel_path.parts[0] != "data":
                rel_path = Path("data") / rel_path
            return gateway_root / rel_path
        return gateway_root

    def _ensure_connection_profile(
        self,
        org,
        org_key,
        domain,
        entry,
        container_storage,
        gateway_root_path,
        path_mapper,
        peer_node,
        orderer_node,
    ):
        template = os.getenv("CCAPI_CCP_TEMPLATE")
        auto_generate = os.getenv("CCAPI_AUTO_GENERATE_CCP", "true").lower()
        auto_generate = auto_generate in ("true", "1", "yes")

        if template:
            ccp_candidate = template.format(
                org=org_key, domain=domain, full=org.name
            )
            ccp_path = Path(ccp_candidate)
            if not ccp_path.is_absolute() and gateway_root_path:
                ccp_path = gateway_root_path / ccp_path
        else:
            ccp_path = self._resolve_gateway_path(
                None, f"data/connection-{org_key}.json"
            )

        if ccp_path is None:
            return

        ccp_path = ccp_path.expanduser().resolve()

        ccp_container = path_mapper(ccp_path)

        if ccp_path.exists():
            entry["ccpPath"] = ccp_container
            return

        if not auto_generate:
            return

        try:
            peer_name = f"{peer_node.name}.{org.name}"
            orderer_name = f"{orderer_node.name}.{domain}"

            peer_tls_ca = (
                container_storage
                / org.name
                / "crypto-config"
                / "peerOrganizations"
                / org.name
                / "peers"
                / peer_name
                / "tls"
                / "ca.crt"
            ).expanduser().resolve()
            orderer_tls_ca = (
                container_storage
                / org.name
                / "crypto-config"
                / "ordererOrganizations"
                / domain
                / "orderers"
                / orderer_name
                / "tls"
                / "ca.crt"
            ).expanduser().resolve()
        except Exception as exc:
            LOG.warning(
                "Unable to auto-generate connection profile for %s: %s",
                org_key,
                exc,
            )
            return

        profile = {
            "name": f"{org_key}-gateway",
            "version": "1.0.0",
            "client": {"organization": org_key},
            "organizations": {
                org_key: {
                    "mspid": entry["mspId"],
                    "peers": [peer_name],
                }
            },
            "peers": {
                peer_name: {
                    "url": f"grpcs://{peer_name}:7051",
                    "tlsCACerts": {"path": path_mapper(peer_tls_ca)},
                }
            },
            "orderers": {
                orderer_name: {
                    "url": f"grpcs://{orderer_name}:7050",
                    "tlsCACerts": {"path": path_mapper(orderer_tls_ca)},
                }
            },
        }

        ccp_path.parent.mkdir(parents=True, exist_ok=True)
        with ccp_path.open("w", encoding="utf-8") as fh:
            json.dump(profile, fh, indent=2)
        entry["ccpPath"] = ccp_container
        LOG.info(
            "Generated connection profile for org %s at %s", org_key, ccp_path
        )

    @swagger_auto_schema(
        method="get",
        responses=with_common_response(
            {status.HTTP_201_CREATED: ChainCodeIDSerializer}
        ),
    )
    @action(detail=False, methods=["get"])
    def query_committed(self, request):
        try:
            channel_name = request.data.get("channel_name")
            chaincode_name = request.data.get("chaincode_name")
            org = request.user.organization
            qs = Node.objects.filter(type="peer", organization=org)
            if not qs.exists():
                raise ResourceNotFound("Peer Does Not Exist")
            peer_node = qs.first()
            envs = init_env_vars(peer_node, org)
            peer_channel_cli = PeerChainCode(**envs)
            code, chaincodes_commited = (
                peer_channel_cli.lifecycle_query_committed(
                    channel_name, chaincode_name
                )
            )
            if code != 0:
                return Response(
                    err("query committed failed."),
                    status=status.HTTP_400_BAD_REQUEST,
                )
        except Exception as e:
            LOG.exception("Could Not Commit Query")
            return Response(err(e.args), status=status.HTTP_400_BAD_REQUEST)
        return Response(ok(chaincodes_commited), status=status.HTTP_200_OK)
