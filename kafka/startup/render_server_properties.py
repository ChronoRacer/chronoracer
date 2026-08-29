#!/usr/bin/env python3
"""Render Kafka server.properties from contract JSON + runtime values."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
import os


def load_json(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def render(
    *,
    platform: dict,
    private_host: str,
    public_host: str,
    keystore_path: str,
    keystore_password: str,
    log_dirs: str,
) -> str:
    private_port = platform["kafka_private_port"]
    public_port = platform["kafka_public_port"]
    jwks = platform["kafka_oauth_jwks_url"]
    issuer = platform["kafka_oauth_issuer"]
    audience = platform["kafka_oauth_audience"]

    jaas = "org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule required;"
    validator = (
        "org.apache.kafka.common.security.oauthbearer.OAuthBearerValidatorCallbackHandler"
    )

    lines = [
        "process.roles=broker,controller",
        "node.id=1",
        f"log.dirs={log_dirs}",
        "controller.quorum.voters=1@127.0.0.1:9093",
        "controller.listener.names=CONTROLLER",
        "inter.broker.listener.name=PRIVATE",
        f"listeners=PRIVATE://0.0.0.0:{private_port},PUBLIC://0.0.0.0:{public_port},CONTROLLER://127.0.0.1:9093",
        f"advertised.listeners=PRIVATE://{private_host}:{private_port},PUBLIC://{public_host}:{public_port}",
        "listener.security.protocol.map=PRIVATE:PLAINTEXT,PUBLIC:SASL_SSL,CONTROLLER:PLAINTEXT",
        "listener.name.public.ssl.keystore.location=" + keystore_path,
        "listener.name.public.ssl.keystore.password=" + keystore_password,
        "listener.name.public.ssl.key.password=" + keystore_password,
        "listener.name.public.ssl.keystore.type=PKCS12",
        "listener.name.public.ssl.client.auth=none",
        "listener.name.public.sasl.enabled.mechanisms=OAUTHBEARER",
        "listener.name.public.oauthbearer.sasl.server.callback.handler.class=" + validator,
        "listener.name.public.oauthbearer.sasl.jaas.config=" + jaas,
        f"listener.name.public.oauthbearer.sasl.oauthbearer.jwks.endpoint.url={jwks}",
        f"listener.name.public.oauthbearer.sasl.oauthbearer.expected.issuer={issuer}",
        f"listener.name.public.oauthbearer.sasl.oauthbearer.expected.audience={audience}",
        "offsets.topic.replication.factor=1",
        "transaction.state.log.replication.factor=1",
        "transaction.state.log.min.isr=1",
        "auto.create.topics.enable=false",
        "num.partitions=1",
        "group.initial.rebalance.delay.ms=0",
    ]

    num_cores = os.cpu_count()
    lines.extend(
        [
            f"num.network.threads={max(4, num_cores)}",
            f"num.io.threads={max(8, num_cores * 2)}",
            "socket.send.buffer.bytes=1048576",
            "socket.receive.buffer.bytes=1048576",
            "socket.request.max.bytes=104857600",
            "num.recovery.threads.per.data.dir=4",
            "log.segment.bytes=1073741824",
            "log.retention.hours=48",
            "log.segment.delete.delay.ms=60000",
            "offsets.retention.minutes=1440",
        ]
    )
    return "\n".join(lines) + "\n"


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--platform", type=Path, required=True)
    parser.add_argument("--private-host", required=True)
    parser.add_argument("--public-host", required=True)
    parser.add_argument("--keystore-path", required=True)
    parser.add_argument("--keystore-password-file", type=Path, required=True)
    parser.add_argument("--log-dirs", required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    keystore_password = args.keystore_password_file.read_text(encoding="utf-8").strip()
    if not keystore_password:
        raise SystemExit(f"keystore password file is empty: {args.keystore_password_file}")
    text = render(
        platform=load_json(args.platform),
        private_host=args.private_host,
        public_host=args.public_host,
        keystore_path=args.keystore_path,
        keystore_password=keystore_password,
        log_dirs=args.log_dirs,
    )
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(text, encoding="utf-8")
    args.output.chmod(0o600)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
