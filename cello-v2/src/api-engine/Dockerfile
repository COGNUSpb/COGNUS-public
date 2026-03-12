# SPDX-License-Identifier: Apache-2.0
# Estende a imagem oficial para incluir o CLI do Docker necessário ao seed de runtimes.
ARG BASE_IMAGE=hyperledger/cello-api-engine
FROM ${BASE_IMAGE}

USER root
RUN apt-get update \
    && apt-get install -y --no-install-recommends docker.io ca-certificates \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /var/www/server
COPY . /var/www/server
