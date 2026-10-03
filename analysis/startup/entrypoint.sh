#!/usr/bin/env bash
# Infrastructure-only startup overlay baked at /chronoracer/entrypoint.sh.
set -euo pipefail
umask 077

export CHRONORACER_DIR="/chronoracer"
"${CHRONORACER_DIR}/enable-ops-agent.sh"
