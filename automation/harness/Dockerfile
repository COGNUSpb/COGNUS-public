FROM ubuntu:22.04

ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get update && \
    apt-get install -y --no-install-recommends openssh-server openssl python3 python3-pip ca-certificates && \
    mkdir -p /var/run/sshd /srv && \
    useradd -m -s /bin/bash cognus && echo "cognus:cognus" | chpasswd && \
    mkdir -p /home/cognus/.ssh && chown cognus:cognus /home/cognus/.ssh && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

COPY entrypoint.sh /usr/local/bin/entrypoint.sh
COPY prepare_certs.sh /usr/local/bin/prepare_certs.sh
RUN chmod +x /usr/local/bin/entrypoint.sh /usr/local/bin/prepare_certs.sh

EXPOSE 22 8080

ENTRYPOINT ["/usr/local/bin/entrypoint.sh"]
