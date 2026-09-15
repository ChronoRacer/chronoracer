#!/usr/bin/env bash
set -euo pipefail

cat >/etc/sysctl.d/99-kafka.conf <<'EOF'
vm.max_map_count=1048576
vm.swappiness=1
EOF

sysctl -p /etc/sysctl.d/99-kafka.conf
