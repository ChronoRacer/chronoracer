from pathlib import Path
from grpc_tools import protoc
from loguru import logger

REPO_ROOT = Path(__file__).resolve().parent.parent.parent.parent
PROTO_SRC = REPO_ROOT / "core" / "proto"
PROTO_DEST = REPO_ROOT / "analysis" / "shared" / "proto"


def build_protos():
    if not PROTO_SRC.exists():
        logger.error("Can't find proto source directory: {}", PROTO_SRC)
        return

    PROTO_DEST.mkdir(parents=True, exist_ok=True)

    proto_files = list(PROTO_SRC.glob("*.proto"))
    if not proto_files:
        logger.warning("No .proto files found in source directory: {}", PROTO_SRC)
        return

    command = [
                  "grpc_tools.protoc",
                  f"-I={PROTO_SRC}",
                  f"--python_out={PROTO_DEST}",
                  f"--grpc_python_out={PROTO_DEST}",
              ] + [str(p) for p in proto_files]

    exit_code = protoc.main(command)
    if exit_code != 0:
        logger.error("Failed to compile proto files. Exit code: {}", exit_code)
        return


if __name__ == "__main__":
    build_protos()