#!/usr/bin/env bash
# Apache Kafka (KRaft) at /opt/kafka plus OpenJDK 17. Runs as root.
set -euo pipefail
export DEBIAN_FRONTEND=noninteractive

: "${KAFKA_VERSION:?KAFKA_VERSION not set}"
: "${KAFKA_SCALA_BINARY:?KAFKA_SCALA_BINARY not set}"

apt-get install -y --no-install-recommends openjdk-17-jre-headless

ARCHIVE="kafka_${KAFKA_SCALA_BINARY}-${KAFKA_VERSION}.tgz"
URL="https://archive.apache.org/dist/kafka/${KAFKA_VERSION}/${ARCHIVE}"
WORKDIR="$(mktemp -d)"
trap 'rm -rf "${WORKDIR}"' EXIT

curl -fsSL "${URL}" -o "${WORKDIR}/${ARCHIVE}"
tar -xzf "${WORKDIR}/${ARCHIVE}" -C "${WORKDIR}"
rm -rf /opt/kafka
mv "${WORKDIR}/kafka_${KAFKA_SCALA_BINARY}-${KAFKA_VERSION}" /opt/kafka
chown -R root:root /opt/kafka
install -d -m 700 /var/lib/kafka/data

[[ -x /opt/kafka/bin/kafka-server-start.sh ]]
[[ -x /opt/kafka/bin/kafka-storage.sh ]]
command -v java >/dev/null
