from flask import Flask, jsonify, request
import docker
from docker.errors import NotFound, APIError
import sys
import logging
import shutil
import os
import ast

logging.basicConfig(level=logging.INFO)

app = Flask(__name__)
PASS_CODE = "OK"
FAIL_CODE = "Fail"

docker_url = os.getenv("DOCKER_URL")
storage_path = os.getenv("STORAGE_PATH", "")
host_base_path = os.getenv("HOST_BASE_PATH", "/")

if storage_path and not os.path.isabs(storage_path):
    storage_path = os.path.abspath(os.path.join(host_base_path, storage_path))

if not storage_path:
    storage_path = os.path.join(host_base_path, "cello-storage", "hyperledger")

COUCHDB_IMAGE = os.getenv("COUCHDB_IMAGE", "couchdb:3.3.3")
COUCHDB_USER = os.getenv("COUCHDB_USER", "couchdb")
COUCHDB_PASSWORD = os.getenv("COUCHDB_PASSWORD", "couchdbpw")
storage_root = os.path.abspath(os.path.join(storage_path, os.pardir))

logging.info("Docker agent using storage path %s", storage_path)

client = docker.DockerClient(docker_url)
res = {"code": "", "data": {}, "msg": ""}


def ensure_directory(path, mode=0o775):
    """Create a directory (and parents) ensuring it is writable by service users."""
    os.makedirs(path, exist_ok=True)
    try:
        os.chmod(path, mode)
    except PermissionError:
        logging.warning("Permission denied adjusting mode %s for %s", oct(mode), path)
    except OSError as exc:  # noqa: BLE001
        logging.warning("Unable to adjust permissions for %s: %s", path, exc)


@app.route("/api/v1/networks", methods=["GET"])
def get_network():
    container_list = client.containers.list()
    containers = {}
    for container in container_list:
        containers[container.id] = {
            "id": container.id,
            "short_id": container.short_id,
            "name": container.name,
            "status": container.status,
            "image": str(container.image),
            "attrs": container.attrs,
        }
    res = {"code": PASS_CODE, "data": containers, "msg": ""}
    return jsonify({"res": res})


@app.route("/api/v1/nodes", methods=["POST"])
def create_node():
    node_name = request.form.get("name")
    database = (request.form.get("database") or "").lower()
    if database not in {"couchdb", "leveldb"}:
        logging.info(
            "Database selection missing or unsupported for node %s; forcing CouchDB.",
            node_name,
        )
        database = "couchdb"
    couchdb_container_name = f"{node_name.replace('.', '-')}-cdb"
    env = {
        "HLF_NODE_MSP": request.form.get("msp"),
        "HLF_NODE_TLS": request.form.get("tls"),
        "HLF_NODE_PEER_CONFIG": request.form.get("peer_config_file"),
        "HLF_NODE_ORDERER_CONFIG": request.form.get("orderer_config_file"),
        "platform": "linux/amd64",
        "FABRIC_TLS_CLIENT_PQ_ENABLED": "false",
        "FABRIC_TLS_SERVER_PQ_ENABLED": "false",
    }
    port_map = ast.literal_eval(request.form.get("port_map"))
    fabric_host_dir = "{}/fabric/{}".format(storage_path, node_name)
    production_host_dir = "{}/production/{}".format(storage_path, node_name)

    for target in (fabric_host_dir, production_host_dir):
        if os.path.isdir(target):
            shutil.rmtree(target, ignore_errors=True)
        ensure_directory(target, mode=0o775)

    if request.form.get("type") == "orderer":
        ensure_directory(
            os.path.join(production_host_dir, "orderer"), mode=0o775
        )
        ensure_directory(
            os.path.join(production_host_dir, "orderer", "chains"),
            mode=0o775,
        )
    else:
        ensure_directory(
            os.path.join(production_host_dir, "ledgersData"), mode=0o775
        )

    volumes = [
        "{}:/etc/hyperledger/fabric".format(fabric_host_dir),
        "{}:/var/hyperledger/production".format(production_host_dir),
        "/var/run/docker.sock:/host/var/run/docker.sock",
    ]
    if os.path.isdir(storage_root):
        volumes.append("{}:/opt/cello".format(storage_root))
    if request.form.get("type") == "peer":
        peer_envs = {
            "CORE_VM_ENDPOINT": "unix:///host/var/run/docker.sock",
            "CORE_VM_DOCKER_HOSTCONFIG_NETWORKMODE": "cello-net",
            "FABRIC_LOGGING_SPEC": "INFO",
            "CORE_PEER_TLS_ENABLED": "true",
            "CORE_PEER_TLS_CLIENT_PQ_ENABLED": "false",
            "CORE_PEER_TLS_SERVER_PQ_ENABLED": "false",
            "CORE_PEER_TLS_CERT_FILE": "/etc/hyperledger/fabric/tls/server.crt",
            "CORE_PEER_TLS_KEY_FILE": "/etc/hyperledger/fabric/tls/server.key",
            "CORE_PEER_TLS_ROOTCERT_FILE": "/etc/hyperledger/fabric/tls/ca.crt",
            "CORE_PEER_PROFILE_ENABLED": "false",
            "CORE_PEER_ID": node_name,
            "CORE_PEER_ADDRESS": node_name + ":7051",
            "CORE_PEER_LISTENADDRESS": "0.0.0.0:7051",
            "CORE_PEER_CHAINCODEADDRESS": node_name + ":7052",
            "CORE_PEER_CHAINCODELISTENADDRESS": "0.0.0.0:7052",
            "CORE_PEER_GOSSIP_BOOTSTRAP": node_name + ":7051",
            "CORE_PEER_GOSSIP_EXTERNALENDPOINT": node_name + ":7051",
            "CORE_PEER_LOCALMSPID": node_name.split(".")[1].capitalize()
            + "MSP",
            "CORE_PEER_MSPCONFIGPATH": "/etc/hyperledger/fabric/msp",
            "CORE_OPERATIONS_LISTENADDRESS": node_name + ":9444",
            "CORE_METRICS_PROVIDER": "prometheus",
        }
        if database == "couchdb":
            peer_envs.update(
                {
                    "CORE_LEDGER_STATE_STATEDATABASE": "CouchDB",
                    "CORE_LEDGER_STATE_COUCHDBCONFIG_COUCHDBADDRESS": couchdb_container_name
                    + ":5984",
                    "CORE_LEDGER_STATE_COUCHDBCONFIG_USERNAME": COUCHDB_USER,
                    "CORE_LEDGER_STATE_COUCHDBCONFIG_PASSWORD": COUCHDB_PASSWORD,
                }
            )
            couchdb_host_path = os.path.join(storage_path, "couchdb", node_name)
            ensure_directory(couchdb_host_path, mode=0o777)
            couchdb_volume = f"{couchdb_host_path}:/opt/couchdb/data"
            try:
                couchdb_container = client.containers.get(couchdb_container_name)
                if couchdb_container.status != "running":
                    couchdb_container.start()
            except NotFound:
                client.containers.run(
                    COUCHDB_IMAGE,
                    detach=True,
                    name=couchdb_container_name,
                    network="cello-net",
                    environment={
                        "COUCHDB_USER": COUCHDB_USER,
                        "COUCHDB_PASSWORD": COUCHDB_PASSWORD,
                    },
                    volumes=[couchdb_volume],
                    restart_policy={"Name": "unless-stopped"},
                )
        env.update(peer_envs)
    else:
        order_envs = {
            "FABRIC_LOGGING_SPEC": "INFO",
            "ORDERER_GENERAL_LISTENADDRESS": "0.0.0.0",
            "ORDERER_GENERAL_LISTENPORT": "7050",
            "ORDERER_GENERAL_LOCALMSPID": "OrdererMSP",
            "ORDERER_GENERAL_LOCALMSPDIR": "/etc/hyperledger/fabric/msp",
            "ORDERER_GENERAL_TLS_ENABLED": "true",
            "ORDERER_TLS_CLIENT_PQ_ENABLED": "false",
            "ORDERER_TLS_SERVER_PQ_ENABLED": "false",
            "ORDERER_GENERAL_TLS_PRIVATEKEY": "/etc/hyperledger/fabric/tls/server.key",
            "ORDERER_GENERAL_TLS_CERTIFICATE": "/etc/hyperledger/fabric/tls/server.crt",
            "ORDERER_GENERAL_TLS_ROOTCAS": "[/etc/hyperledger/fabric/tls/ca.crt]",
            "ORDERER_GENERAL_CLUSTER_CLIENTCERTIFICATE": "/etc/hyperledger/fabric/tls/server.crt",
            "ORDERER_GENERAL_CLUSTER_CLIENTPRIVATEKEY": "/etc/hyperledger/fabric/tls/server.key",
            "ORDERER_GENERAL_CLUSTER_ROOTCAS": "[/etc/hyperledger/fabric/tls/ca.crt]",
            "ORDERER_GENERAL_BOOTSTRAPMETHOD": "none",
            "ORDERER_CHANNELPARTICIPATION_ENABLED": "true",
            "ORDERER_ADMIN_TLS_ENABLED": "true",
            "ORDERER_ADMIN_TLS_CERTIFICATE": "/etc/hyperledger/fabric/tls/server.crt",
            "ORDERER_ADMIN_TLS_PRIVATEKEY": "/etc/hyperledger/fabric/tls/server.key",
            "ORDERER_ADMIN_TLS_ROOTCAS": "[/etc/hyperledger/fabric/tls/ca.crt]",
            "ORDERER_ADMIN_TLS_CLIENTROOTCAS": "[/etc/hyperledger/fabric/tls/ca.crt]",
            "ORDERER_ADMIN_LISTENADDRESS": "0.0.0.0:7053",
            "ORDERER_OPERATIONS_LISTENADDRESS": node_name + ":9443",
            "ORDERER_METRICS_PROVIDER": "prometheus",
        }
        env.update(order_envs)
    try:
        try:
            container = client.containers.get(node_name)
            container.reload()
            logging.info(
                "Removing existing container %s (%s) before provisioning",
                node_name,
                container.id,
            )
            if container.status == "running":
                container.stop(timeout=15)
            container.remove(force=True)
        except NotFound:
            pass
        except APIError:
            logging.exception(
                "Failed to cleanup existing container %s", node_name
            )
            raise

        # same as `docker run -dit yeasy/hyperledge-fabric:2.2.0 -e VARIABLES``
        container = client.containers.run(
            request.form.get("img"),
            request.form.get("cmd"),
            detach=True,
            tty=True,
            stdin_open=True,
            network="cello-net",
            name=request.form.get("name"),
            dns_search=["."],
            volumes=volumes,
            environment=env,
            ports=port_map,
        )
    except:
        res["code"] = FAIL_CODE
        res["data"] = sys.exc_info()[0]
        res["msg"] = "creation failed"
        logging.error(res)
        raise

    res["code"] = PASS_CODE
    res["data"] = {}
    res["data"]["id"] = container.id
    res["data"]["status"] = "created"
    res["data"][
        "public-grpc"
    ] = "127.0.0.1:7050"  # TODO: read the info from config file
    res["data"]["public-raft"] = "127.0.0.1:7052"
    res["msg"] = "node created"

    return jsonify(res)


@app.route("/api/v1/nodes/<id>", methods=["GET", "POST"])
def operate_node(id):
    container = client.containers.get(id)
    res["data"] = {}
    if request.method == "POST":
        act = request.form.get("action")  # only with POST

        try:
            if act == "start":
                container.start()
                res["msg"] = "node started"
            elif act == "restart":
                container.restart()
                res["msg"] = "node restarted"
            elif act == "stop":
                container.stop()
                res["msg"] = "node stopped"
            elif act == "delete":
                container.remove()
                res["msg"] = "node deleted"
                couchdb_container_name = f"{container.name.replace('.', '-')}-cdb"
                try:
                    client.containers.get(couchdb_container_name).remove(
                        force=True
                    )
                except NotFound:
                    pass
            elif act == "update":

                env = {}

                if "msp" in request.form:
                    env["HLF_NODE_MSP"] = request.form.get("msp")

                if "tls" in request.form:
                    env["HLF_NODE_TLS"] = request.form.get("tls")

                if "bootstrap_block" in request.form:
                    env["HLF_NODE_BOOTSTRAP_BLOCK"] = request.form.get(
                        "bootstrap_block"
                    )

                if "peer_config_file" in request.form:
                    env["HLF_NODE_PEER_CONFIG"] = request.form.get(
                        "peer_config_file"
                    )

                if "orderer_config_file" in request.form:
                    env["HLF_NODE_ORDERER_CONFIG"] = request.form.get(
                        "orderer_config_file"
                    )

                container.exec_run(
                    request.form.get("cmd"),
                    detach=True,
                    tty=True,
                    stdin=True,
                    environment=env,
                )
                container.restart()
                res["msg"] = "node updated"

            else:
                res["msg"] = "undefined action"
        except:
            res["code"] = FAIL_CODE
            res["data"] = sys.exc_info()[0]
            res["msg"] = act + "failed"
            logging.error(res)
            raise
    else:
        # GET
        res["data"]["status"] = container.status

    res["code"] = PASS_CODE
    return jsonify(res)


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5001)
