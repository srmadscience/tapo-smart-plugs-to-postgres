#!/usr/bin/env bash
#
# Remove the tapo-watcher systemd timer + service. Run with sudo:
#   sudo ./systemd/uninstall-service.sh
#
# Leaves the secrets file (/etc/tapo-watcher/tapo-watcher.env), the repo, and
# the virtualenv in place — delete those by hand if you want a clean slate.
#
set -euo pipefail

if [[ $EUID -ne 0 ]]; then
  echo "error: run with sudo" >&2
  exit 1
fi

UNIT_DIR=/etc/systemd/system

systemctl disable --now tapo-watcher.timer 2>/dev/null || true
systemctl stop tapo-watcher.service 2>/dev/null || true

rm -f "${UNIT_DIR}/tapo-watcher.timer" "${UNIT_DIR}/tapo-watcher.service"
systemctl daemon-reload

echo "Removed tapo-watcher.timer and tapo-watcher.service."
echo "Kept /etc/tapo-watcher/tapo-watcher.env (delete manually if desired)."
