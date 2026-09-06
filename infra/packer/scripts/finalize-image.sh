#!/usr/bin/env bash
# Remove builder-only state after all packages and overlays are installed.
set -euo pipefail

apt-get clean
rm -rf /var/lib/apt/lists/* /var/cache/apt/archives/*.deb

# Avoid carrying transient builder logs into every race VM.
if [[ -d /var/log ]]; then
  find /var/log -type f -exec truncate -s 0 {} +
fi
