from controller.names import race_bucket
from pathlib import Path


def test_race_bucket_from_contract():
    path = Path(__file__).resolve().parents[2] / "infra" / "contract" / "names.json"
    assert race_bucket(path) == "chronoracer-race"
