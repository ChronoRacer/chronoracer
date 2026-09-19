from pathlib import Path

from render_server_properties import render


PLATFORM = {
    "kafka_private_port": 9092,
    "kafka_public_port": 9094,
    "kafka_oauth_jwks_url": "https://chronoracer.eu.auth0.com/.well-known/jwks.json",
    "kafka_oauth_issuer": "https://chronoracer.eu.auth0.com/",
    "kafka_oauth_audience": "https://api.chronoracer.com",
}


def test_render_listeners_and_oauth():
    text = render(
        platform=PLATFORM,
        private_host="kafka.europe-west1-b.c.proj.internal",
        public_host="ingest.chronoracer.live",
        keystore_path="/dev/shm/kafka-keystore.p12",
        keystore_password="secret-pass",
        log_dirs="/var/lib/kafka/data",
    )
    assert (
        "listeners=PRIVATE://0.0.0.0:9092,PUBLIC://0.0.0.0:9094,CONTROLLER://127.0.0.1:9093"
        in text
    )
    assert (
        "advertised.listeners=PRIVATE://kafka.europe-west1-b.c.proj.internal:9092,PUBLIC://ingest.chronoracer.live:9094"
        in text
    )
    assert "listener.security.protocol.map=PRIVATE:PLAINTEXT,PUBLIC:SASL_SSL,CONTROLLER:PLAINTEXT" in text
    assert "inter.broker.listener.name=PRIVATE" in text
    assert "listener.name.public.ssl.keystore.location=/dev/shm/kafka-keystore.p12" in text
    assert "listener.name.public.ssl.keystore.password=secret-pass" in text
    assert "OAuthBearerValidatorCallbackHandler" in text
    assert (
        "listener.name.public.oauthbearer.sasl.oauthbearer.jwks.endpoint.url=https://chronoracer.eu.auth0.com/.well-known/jwks.json"
        in text
    )
    assert (
        "listener.name.public.oauthbearer.sasl.oauthbearer.expected.issuer=https://chronoracer.eu.auth0.com/"
        in text
    )
    assert (
        "listener.name.public.oauthbearer.sasl.oauthbearer.expected.audience=https://api.chronoracer.com"
        in text
    )
    assert "auto.create.topics.enable=false" in text


def test_render_writes_file(tmp_path: Path):
    from render_server_properties import main
    import sys

    out = tmp_path / "server.properties"
    platform = tmp_path / "platform.json"
    pass_file = tmp_path / "keystore.pass"
    pass_file.write_text("p\n", encoding="utf-8")
    platform.write_text(
        '{"kafka_private_port":9092,"kafka_public_port":9094,'
        '"kafka_oauth_jwks_url":"https://example/jwks",'
        '"kafka_oauth_issuer":"https://example/",'
        '"kafka_oauth_audience":"https://api.example"}',
        encoding="utf-8",
    )
    argv = sys.argv
    sys.argv = [
        "render_server_properties.py",
        "--platform",
        str(platform),
        "--private-host",
        "kafka.internal",
        "--public-host",
        "kafka.public",
        "--keystore-path",
        "/tmp/k.p12",
        "--keystore-password-file",
        str(pass_file),
        "--log-dirs",
        "/data",
        "--output",
        str(out),
    ]
    try:
        assert main() == 0
    finally:
        sys.argv = argv
    body = out.read_text(encoding="utf-8")
    assert "PUBLIC://kafka.public:9094" in body
