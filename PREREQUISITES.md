# Prerequisites For start_env.sh

This guide covers how to prepare a fresh Linux VM to run `./start_env.sh`.

The script assumes:

- Linux with `bash`
- a user with `sudo` privileges
- Docker commands available without `sudo`
- outbound network access to package repositories and Docker registries, or local image archives for offline mode

## Required For The Default Flow

These items are required for the default `./start_env.sh` flow:

- `git`
- Docker Engine
- Docker Compose v2 plugin, or legacy `docker-compose`
- Node.js with `npm`

These are not always hard-blocking, but are strongly recommended because the script uses them during startup and recovery steps:

- `curl`
- `systemctl` or `service`
- `sysctl`

`make` is optional. If it is not installed, `start_env.sh` falls back to direct `docker build` commands for the supported image targets.

## Required For Optional Modes

- `jq`: required when `COGNUS_ENABLE_LOCAL_GATEWAY=1`, because the script merges generated identity JSON files.
- `skopeo`: optional fallback for pulling or importing Docker images when normal `docker pull` fails.
- `osnadmin`: optional. If absent, the script tries to use `hyperledger/fabric-tools:2.5` in Docker for channel join operations.

## Configuracao Do Ambiente

On Ubuntu or Debian, the practical baseline is:

```bash
sudo apt-get update
sudo apt-get install -y git curl jq ca-certificates gnupg lsb-release make
```

## Clean Up Old Docker Installations

If the VM already had Docker installed in different ways, clean old packages first to avoid conflicts.

```bash
sudo apt-get remove -y docker docker-engine docker.io docker-ce docker-ce-cli containerd runc || true
sudo apt-get purge -y docker.io docker-ce docker-ce-cli || true
sudo apt-get autoremove -y --purge docker.io || true
sudo rm -rf /var/lib/docker
sudo rm -rf /etc/docker
sudo rm -f /usr/bin/docker
sudo rm -f /usr/bin/dockerd
sudo rm -f /usr/local/bin/docker-compose
sudo rm -f /snap/bin/docker
sudo rm -f /etc/apt/sources.list.d/docker.list
sudo groupdel docker || true
```

Run this cleanup only if the host has conflicting Docker packages or broken previous installations.

## Install Docker Engine

The commands below detect the distribution from `/etc/os-release` and configure the official Docker repository accordingly.

```bash
sudo install -m 0755 -d /etc/apt/keyrings
source /etc/os-release

curl -fsSL "https://download.docker.com/linux/${ID}/gpg" | sudo gpg --dearmor -o /etc/apt/keyrings/docker.gpg
sudo chmod a+r /etc/apt/keyrings/docker.gpg

echo \
  "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.gpg] https://download.docker.com/linux/${ID} ${VERSION_CODENAME} stable" | \
  sudo tee /etc/apt/sources.list.d/docker.list > /dev/null

sudo apt-get update
sudo apt-get install -y \
  docker-ce \
  docker-ce-cli \
  containerd.io \
  docker-buildx-plugin \
  docker-compose-plugin
```

If you prefer the distro package instead of the official Docker repository, the simpler alternative is:

```bash
sudo apt-get update
sudo apt-get install -y docker.io containerd docker-compose-v2
```

Use one installation path or the other, not both.

## Configure Docker Permissions

After installing Docker, allow the current user to run Docker without `sudo`:

```bash
sudo groupadd docker || true
sudo usermod -aG docker "$USER"
newgrp docker
```

Basic validation:

```bash
docker --version
docker ps
docker run hello-world
```

If Docker still fails with permission errors, check the socket and service state:

```bash
ls -l /var/run/docker.sock
grep docker /etc/group
sudo chown :docker /var/run/docker.sock
sudo chmod 660 /var/run/docker.sock
sudo service docker stop || true
sudo service docker start || true
docker ps
```

Temporary workaround only when the VM is disposable and permissions are still blocked:

```bash
sudo chmod 666 /var/run/docker.sock
```

Do not keep `666` on the Docker socket as a permanent configuration on shared or persistent hosts.

## Docker Compose

Preferred option:

- Docker Compose v2 plugin via `docker compose`

Validation:

```bash
docker compose version
```

Legacy fallback, only if the plugin is unavailable and you specifically need `docker-compose`:

```bash
sudo apt-get remove -y docker-compose || true
sudo rm -f /usr/local/bin/docker-compose

sudo curl -L "https://github.com/docker/compose/releases/download/1.29.2/docker-compose-$(uname -s)-$(uname -m)" -o /usr/local/bin/docker-compose
sudo chmod +x /usr/local/bin/docker-compose
export PATH="$PATH:/usr/local/bin"
export PATH="$PATH:/snap/bin"

docker-compose -v
```

For this repository, prefer `docker compose` whenever possible.

## Install Node.js And npm

For the current public snapshot, use a modern Node.js LTS release. Node.js 20 is the recommended baseline.

If the VM has an old Node.js installation and you want to remove it first:

```bash
sudo rm -rf /usr/local/bin/node /usr/local/lib/node_modules
sudo apt-get remove -y nodejs npm || true
sudo rm -f /usr/bin/node
sudo rm -f /usr/bin/npm
```

Recommended installation with NodeSource:

```bash
curl -fsSL https://deb.nodesource.com/setup_20.x | sudo -E bash -
sudo apt-get install -y nodejs
```

Validation:

```bash
node --version
npm --version
```

Legacy note:

- Older internal environments may have used Node.js 12.x and npm 6.x.
- That is not the recommended baseline for this public snapshot.
- Use Node.js 20 unless you are reproducing a legacy private environment on purpose.

## Optional Tools

`jq`:

```bash
sudo apt-get install -y jq
jq --version
```

`skopeo`:

```bash
sudo apt-get install -y skopeo
skopeo --version
```

If you also plan to run Python-based auxiliary scripts outside `start_env.sh`:

```bash
sudo apt-get install -y python3 python3-pip
python3 --version
pip3 --version
```

Only if you explicitly need to update Paramiko for external automation scripts:

```bash
pip3 install --upgrade paramiko
```

## Validation Checklist

Before running the environment, validate the host:

```bash
docker --version
docker compose version
node --version
npm --version
git --version
docker info
```

If you plan to use local gateway mode:

```bash
jq --version
```

If anything still behaves strangely, reboot the VM and re-run the checks above.

## Running On A Fresh VM

After the prerequisites are installed:

```bash
git clone <your-repository-url> COGNUS-public
cd COGNUS-public
chmod +x start_env.sh clean_env_zero.sh
./start_env.sh
```

If you want a clean reset first:

```bash
./clean_env_zero.sh
./start_env.sh
```

## Offline Notes

If the VM cannot reach Docker Hub, `start_env.sh` looks for preloaded image archives in these directories:

- `./`
- `./docker-images/`
- `./images/`
- `./artifacts/`

Expected file naming pattern:

- `python_3.8.tar`
- `postgres_12.0.tar`
- `node_20.15.tar`

The script also attempts IPv4-only Docker registry handling automatically, which is why it may request `sudo` during startup to adjust Docker and host network settings.