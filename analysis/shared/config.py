import os
import uuid

from dotenv import load_dotenv, find_dotenv
from loguru import logger
from pydantic import  BaseModel

REQUIRED_ENV = (
    "CORE_TELEMETRY_URL",
    #CLIENT_ID
    "TELEMETRY_URL",
)



# This is config for analysis module, it is loaded from env variables
# this is not only for env but for all the configuration needs of the analysis module
# this is not place to define default value for rules config !!!
class Config(BaseModel):
    CORE_TELEMETRY_URL: str
    TELEMETRY_URL : str
    CLIENT_ID : str
    KAFKA_PRIVATE_BOOTSTRAP: str  # host:port of the private Kafka listener (e.g. kafka.zone.c.project.internal:9092)
    ANALYSIS_INGEST_TOKEN: str = "TOKEN"

def load_config() -> Config | None:
    load_dotenv(find_dotenv())
    for required in REQUIRED_ENV:
        if required not in os.environ:
            logger.error("Missing env: {}", required)
            return None

    logger.info("Analysis config loaded")

    telemetry_url = os.environ["TELEMETRY_URL"]
    if "://" not in telemetry_url and "https://" not in telemetry_url:
        telemetry_url = "http://" + telemetry_url
    return Config(
        CORE_TELEMETRY_URL=os.environ["CORE_TELEMETRY_URL"],
        CLIENT_ID="analysis_",
        TELEMETRY_URL=telemetry_url,
        KAFKA_PRIVATE_BOOTSTRAP=os.environ.get("KAFKA_PRIVATE_BOOTSTRAP", "127.0.0.1:9092"),
        ANALYSIS_INGEST_TOKEN=os.environ.get("ANALYSIS_INGEST_TOKEN", "TOKEN"),
    )
