import os

from loguru import logger
from pydantic import  BaseModel

REQUIRED_ENV = (
    "CORE_TELEMETRY_URL",
    #CLIENT_ID
)



# This is config for analysis module, it is loaded from env variables
# this is not only for env but for all the configuration needs of the analysis module
# this is not place to define default value for rules config !!!
class Config(BaseModel):
    CORE_TELEMETRY_URL: str
    CLIENT_ID : str

def load_config() -> Config | None:
    for required in REQUIRED_ENV:
        if required not in os.environ:
            logger.error("Missing env: {}", required)
            return None

    logger.info("Analysis config loaded")

    return Config(
        CORE_TELEMETRY_URL=os.environ["CORE_TELEMETRY_URL"],
        CLIENT_ID=""
    )
