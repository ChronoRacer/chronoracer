from pathlib import Path

from controller.names import race_bucket


def test_race_bucket_from_contract():
    path = Path(__file__).resolve().parents[2] / "infra" / "contract" / "names.json"
    assert race_bucket(path) == "chronoracer-race"
