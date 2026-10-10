import re
from pathlib import Path
import grpc_tools
from grpc_tools import protoc
from loguru import logger


# Ustalenie katalogu głównego repozytorium (szukanie katalogu z .git lub markerem projektu)
def find_repo_root(current_path: Path) -> Path:
    for parent in [current_path] + list(current_path.parents):
        if (parent / ".git").exists() or (parent / "analysis").exists():
            return parent
    return current_path.resolve().parents[3]


REPO_ROOT = find_repo_root(Path(__file__).resolve())
PROTO_SRC = REPO_ROOT / "core" / "proto"
PROTO_DEST = REPO_ROOT / "analysis" / "shared" / "proto"


def fix_generated_imports(output_dir: Path) -> None:
    import_pattern = re.compile(r"^import\s+(\w+_pb2)\s+as\s+", re.MULTILINE)

    for py_file in output_dir.glob("*.py"):
        if py_file.name == "__init__.py":
            continue
        content = py_file.read_text(encoding="utf-8")
        updated_content = import_pattern.sub(r"from . import \1 as ", content)
        if content != updated_content:
            py_file.write_text(updated_content, encoding="utf-8")
            logger.info("Fixed imports in: {}", py_file.name)


def build_protos() -> None:
    if not PROTO_SRC.exists():
        logger.error("Can't find proto source directory: {}", PROTO_SRC)
        return

    PROTO_DEST.mkdir(parents=True, exist_ok=True)

    # Zapewnienie istnienia pakietu pythonowego
    init_file = PROTO_DEST / "__init__.py"
    if not init_file.exists():
        init_file.touch()

    proto_files = list(PROTO_SRC.glob("*.proto"))
    if not proto_files:
        logger.warning("No .proto files found in source directory: {}", PROTO_SRC)
        return

    # Dolaczenie wbudowanych definicji proto z biblioteki grpc_tools
    grpc_include = Path(grpc_tools.__file__).parent / "_proto"

    command = [
                  "grpc_tools.protoc",
                  f"-I={PROTO_SRC}",
                  f"-I={grpc_include}",
                  f"--python_out={PROTO_DEST}",
                  f"--grpc_python_out={PROTO_DEST}",
              ] + [p.name for p in proto_files]

    # Uruchomienie kompilacji w kontekscie katalogu ze zrodlami proto
    exit_code = protoc.main(command)
    if exit_code != 0:
        logger.error("Failed to compile proto files. Exit code: {}", exit_code)
        return

    # Post-processing plikow w celu naprawy sciezek importu
    fix_generated_imports(PROTO_DEST)
    logger.success("Successfully generated and patched protobuf files in {}", PROTO_DEST)


if __name__ == "__main__":
    build_protos()