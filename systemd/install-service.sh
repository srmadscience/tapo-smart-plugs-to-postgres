#!/usr/bin/env bash
#
# Install tapo-watcher as a systemd timer-driven service on a Raspberry Pi
# (or any systemd Linux host). Run from a clone of this repo with sudo:
#
#   sudo ./systemd/install-service.sh
#
# It is idempotent — safe to re-run after a `git pull` to pick up unit changes.
#
# Overridable via environment:
#   SERVICE_USER=pi          who runs the watcher (default: invoking sudo user,
#                            else the owner of the repo)
#   TAPO_ONCALENDAR=*:0/15   when to poll (systemd OnCalendar; default every 15 min)
#
# What it does:
#   1. checks Python >= 3.11 (tapo needs it), creates the venv + installs the package
#   2. creates /etc/tapo-watcher/tapo-watcher.env (chmod 600) for the secrets
#   3. renders + installs the .service and .timer units
#   4. enables and starts the timer
#
set -euo pipefail

if [[ $EUID -ne 0 ]]; then
  echo "error: run with sudo (needs to write ${UNIT_DIR:-/etc/systemd/system})" >&2
  exit 1
fi

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_DIR="$(cd "${HERE}/.." && pwd)"

SERVICE_USER="${SERVICE_USER:-${SUDO_USER:-$(stat -c '%U' "${REPO_DIR}")}}"
ONCALENDAR="${TAPO_ONCALENDAR:-*:0/15}"
ENVDIR=/etc/tapo-watcher
ENVFILE="${ENVDIR}/tapo-watcher.env"
UNIT_DIR=/etc/systemd/system

if ! id "$SERVICE_USER" >/dev/null 2>&1; then
  echo "error: user '$SERVICE_USER' does not exist (set SERVICE_USER=...)" >&2
  exit 1
fi

echo "Repo:     $REPO_DIR"
echo "User:     $SERVICE_USER"
echo "Schedule: $ONCALENDAR"
echo

# 0. Preflight: confirm `python3 -m venv` (with ensurepip) actually works. On
#    Debian / Raspberry Pi OS the venv + bundled-pip support ships in a SEPARATE
#    `python3-venv` package; without it `python3 -m venv` dies inside ensurepip
#    with a bare "returned non-zero exit status 1" (and the timer never gets
#    installed). Probe with a throwaway venv created AS the service user (same
#    conditions as step 1) and fail here with a fix hint instead of a traceback.
echo "Checking python3 version + venv support..."
if ! python3 -c 'import sys; sys.exit(sys.version_info < (3, 11))'; then
  echo "error: python3 is $(python3 --version 2>&1); the tapo library needs >= 3.11" >&2
  echo "(Raspberry Pi OS bookworm ships 3.11.)" >&2
  exit 1
fi
PYVER="$(python3 -c 'import sys; print(f"{sys.version_info.major}.{sys.version_info.minor}")' 2>/dev/null || true)"
VENV_PROBE_DIR="$(sudo -u "$SERVICE_USER" mktemp -d)"
if ! sudo -u "$SERVICE_USER" python3 -m venv "${VENV_PROBE_DIR}/v" >/tmp/tapo-venv-probe 2>&1; then
  rm -rf "$VENV_PROBE_DIR"
  echo "error: 'python3 -m venv' failed -- venv/ensurepip support is missing." >&2
  echo "On Debian / Raspberry Pi OS, install it and re-run this installer:" >&2
  echo "    sudo apt update && sudo apt install -y python3-venv python3-pip${PYVER:+   # or python${PYVER}-venv}" >&2
  echo "Also clear any half-built venv first: rm -rf '${REPO_DIR}/.venv'" >&2
  echo "--- probe output ---" >&2
  cat /tmp/tapo-venv-probe >&2
  exit 1
fi
rm -rf "$VENV_PROBE_DIR"
echo "  venv support ok"
echo

# 1. virtualenv + package (as the service user, never root)
#    PIP_INDEX_URL pins PyPI: Raspberry Pi OS adds piwheels in /etc/pip.conf,
#    which has served empty/broken wheels for some pure-Python packages.
echo "Creating virtualenv + installing package as ${SERVICE_USER}..."
sudo -u "$SERVICE_USER" PIP_INDEX_URL="https://pypi.org/simple" bash -c "
  cd '${REPO_DIR}' &&
  python3 -m venv .venv &&
  .venv/bin/pip install --upgrade pip &&
  .venv/bin/pip install --no-cache-dir -e .
"

# 1a. Assert the package actually imports before we enable anything -- a broken
#     wheel (empty install) must fail loudly here, not at the first timer tick.
echo "Verifying imports..."
if ! sudo -u "$SERVICE_USER" "${REPO_DIR}/.venv/bin/python" \
     -c "import tapo_watcher.cli, tapo, psycopg, requests, confluent_kafka" 2>/tmp/tapo-import-err; then
  echo "error: the venv is missing required modules:" >&2
  cat /tmp/tapo-import-err >&2
  echo "Fix the install (see README) before re-running; the timer was NOT enabled." >&2
  exit 1
fi
echo "  imports ok (tapo_watcher, tapo, psycopg, requests, confluent_kafka)"

# 2. secrets EnvironmentFile
mkdir -p "$ENVDIR"
if [[ ! -f "$ENVFILE" ]]; then
  install -m 600 "${HERE}/tapo-watcher.env.example" "$ENVFILE"
  echo "Created ${ENVFILE} from template."
  NEEDS_SECRETS=1
else
  echo "${ENVFILE} already exists — leaving it untouched"
  chmod 600 "$ENVFILE"
fi

# 3. render + install units
for unit in tapo-watcher.service tapo-watcher.timer; do
  sed -e "s|@REPO_DIR@|${REPO_DIR}|g" \
      -e "s|@USER@|${SERVICE_USER}|g" \
      -e "s|@ENVFILE@|${ENVFILE}|g" \
      -e "s|@ONCALENDAR@|${ONCALENDAR}|g" \
      "${HERE}/${unit}" > "${UNIT_DIR}/${unit}"
  echo "Installed ${UNIT_DIR}/${unit}"
done

# 4. enable + start the timer
systemctl daemon-reload
systemctl enable --now tapo-watcher.timer

echo
echo "=== Timer ==="
systemctl list-timers --no-pager tapo-watcher.timer || true

if [[ "${NEEDS_SECRETS:-0}" == "1" ]]; then
  echo
  echo "!! ACTION REQUIRED: edit ${ENVFILE} and set TAPO_USERNAME, TAPO_PASSWORD and TAPO_PG_DSN."
  echo "   Until then the service will fail. After editing, no restart is needed —"
  echo "   the next timer tick (or 'sudo systemctl start tapo-watcher.service') picks it up."
fi

echo
echo "Useful commands:"
echo "  sudo systemctl start tapo-watcher.service     # run one poll now"
echo "  journalctl -u tapo-watcher.service -f         # follow logs"
echo "  systemctl list-timers tapo-watcher.timer      # next scheduled run"
