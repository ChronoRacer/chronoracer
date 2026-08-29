#!/usr/bin/env bash

sudo tee /etc/sysctl.d/99-kafka.conf << 'EOF'
vm.max_map_count=1048576
vm.swappiness=1
EOF