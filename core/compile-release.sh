#!/usr/bin/env bash
set -euo pipefail

BINARY_NAME="chronoracer-core"
WSL_SRC_TMP="/tmp/rust_src_core"
WSL_CACHE_DIR="$HOME/.cache/rust_targets/core"
OUTPUT_DIR="$HOME/bin"

cleanup() {
    rm -rf "$WSL_SRC_TMP"
}
trap cleanup EXIT

mkdir -p "$WSL_SRC_TMP" "$WSL_CACHE_DIR" "$OUTPUT_DIR"

rsync -a --delete \
    --exclude 'target' \
    --exclude '.git' \
    ./ "$WSL_SRC_TMP/"

RUSTFLAGS="-C target-cpu=native" CARGO_TARGET_DIR="$WSL_CACHE_DIR" cargo build --profile release --manifest-path "$WSL_SRC_TMP/Cargo.toml"

cp -f "$WSL_CACHE_DIR/release/$BINARY_NAME" "$OUTPUT_DIR/$BINARY_NAME"
chmod +x "$OUTPUT_DIR/$BINARY_NAME"

echo "Ok. Binary installed at: $OUTPUT_DIR/$BINARY_NAME"