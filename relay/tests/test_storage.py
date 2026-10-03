import pytest
from app.gcp.storage import GCSObjectStore, iam_sign_params
from types import SimpleNamespace
from unittest.mock import MagicMock


class _Creds:
    def __init__(self, email, token=None):
        self.service_account_email = email
        self.token = token

    def refresh(self, _request):
        self.token = "access-token"


def test_iam_sign_params_refreshes_and_returns_email_token(monkeypatch):
    creds = _Creds("relay@proj.iam.gserviceaccount.com")
    monkeypatch.setattr("app.gcp.storage.google.auth.default", lambda: (creds, "proj"))
    _, email, token = iam_sign_params()
    assert email == "relay@proj.iam.gserviceaccount.com"
    assert token == "access-token"


def test_iam_sign_params_rejects_missing_email(monkeypatch):
    creds = _Creds("default", token="already")
    monkeypatch.setattr("app.gcp.storage.google.auth.default", lambda: (creds, "proj"))
    with pytest.raises(RuntimeError, match="cannot sign GCS URLs"):
        iam_sign_params()


def test_iam_sign_params_refreshes_expired_token(monkeypatch):
    creds = _Creds('relay@proj.iam.gserviceaccount.com', token='expired-token')
    creds.expired = True
    monkeypatch.setattr('app.gcp.storage.google.auth.default', lambda: (creds, 'proj'))
    assert iam_sign_params()[2] == 'access-token'


def test_signed_export_url_passes_iam_signblob_params(monkeypatch):
    creds = _Creds("relay@proj.iam.gserviceaccount.com")
    monkeypatch.setattr("app.gcp.storage.google.auth.default", lambda: (creds, "proj"))

    blob = MagicMock()
    blob.name = "race1/export/db/sensor.parquet"
    blob.generate_signed_url.return_value = "https://signed.example/object"
    placeholder = SimpleNamespace(name="race1/export/")

    bucket = MagicMock()
    bucket.list_blobs.return_value = [placeholder, blob]
    client = MagicMock()
    client.bucket.return_value = bucket
    monkeypatch.setattr("app.gcp.storage.storage.Client", lambda **kwargs: client)

    urls = GCSObjectStore("proj", "chronoracer-race")._signed_export_url_sync("race1/export/")

    assert urls == {"race1/export/db/sensor.parquet": "https://signed.example/object"}
    kwargs = blob.generate_signed_url.call_args.kwargs
    assert kwargs["version"] == "v4"
    assert kwargs["method"] == "GET"
    assert kwargs["service_account_email"] == "relay@proj.iam.gserviceaccount.com"
    assert kwargs["access_token"] == "access-token"
