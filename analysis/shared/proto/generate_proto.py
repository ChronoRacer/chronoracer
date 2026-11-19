from pathlib import Path

import grpc_tools
from grpc_tools import protoc
from loguru import logger


def find_repo_root(current_path: Path) -> Path:
    for parent in [current_path] + list(current_path.parents):
        if (parent / ".git").exists() or (parent / "analysis").exists():
            return parent
    return current_path.resolve().parents[3]


REPO_ROOT = find_repo_root(Path(__file__).resolve())
PROTO_SRC = REPO_ROOT / "core" / "proto"
PYTHON_PROTO_PACKAGE = "analysis.shared.proto"
PROTO_DEST = REPO_ROOT / Path(*PYTHON_PROTO_PACKAGE.split("."))


def build_protos() -> None:
    if not PROTO_SRC.exists():
        logger.error("Can't find proto source directory: {}", PROTO_SRC)
        return

    PROTO_DEST.mkdir(parents=True, exist_ok=True)

    init_file = PROTO_DEST / "__init__.py"
    if not init_file.exists():
        init_file.touch()

    proto_files = list(PROTO_SRC.glob("*.proto"))
    if not proto_files:
        logger.warning("No .proto files found in source directory: {}", PROTO_SRC)
        return

    grpc_include = Path(grpc_tools.__file__).parent / "_proto"

    command = [
        "grpc_tools.protoc",
        # Map the physical source directory to its Python import path. This makes
        # protoc generate package-qualified imports without patching its output.
        f"-I{PYTHON_PROTO_PACKAGE}={PROTO_SRC}",
        f"-I={grpc_include}",
        f"--python_out={REPO_ROOT}",
        f"--grpc_python_out={REPO_ROOT}",
    ] + [str(proto_file) for proto_file in proto_files]

    exit_code = protoc.main(command)
    if exit_code != 0:
        logger.error("Failed to compile proto files. Exit code: {}", exit_code)
        return

    logger.success("Successfully generated protobuf files in {}", PROTO_DEST)


if __name__ == "__main__":
    build_protos()
