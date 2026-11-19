#!/usr/bin/env bash
# Tight clock sync against the GCE metadata server. Core latency metrics use producer timestamps,
# so clock offset between the Kafka and Core VMs must stay well below 1 ms. Runs as root.
set -euo pipefail

install -d -m 755 /etc/systemd/timesyncd.conf.d
cat >/etc/systemd/timesyncd.conf.d/99-chronoracer.conf <<'EOF'
[Time]
NTP=metadata.google.internal
PollIntervalMinSec=16
PollIntervalMaxSec=32
EOF
systemctl enable systemd-timesyncd
