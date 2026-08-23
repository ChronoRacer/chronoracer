import uvicorn
from loguru import logger

from .main import app

if __name__ == "__main__":
    logger.info("Starting Relay server")
    uvicorn.run(app, host=app.state.config.server.host, port=app.state.config.server.port,
                log_level=app.state.config.server.log_level)
