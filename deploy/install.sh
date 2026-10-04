#!/usr/bin/env bash
# TeleVK installer. Offline wheelhouse is the default and recommended path.
# --resume is only for an interrupted pre-configuration install.
# --online explicitly allows pip to use the configured package index.
set -Eeuo pipefail
umask 022

SOURCE="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd -P)"
PYTHON_BIN="${PYTHON_BIN:-python3}"
RESUME=0
ONLINE=0

usage() {
  cat <<'USAGE'
Usage: sudo bash deploy/install.sh [--resume] [--online]

Default: install Python dependencies only from ./wheelhouse (no PyPI access).
  --resume  Continue an interrupted install only if config/database/unit do not exist.
  --online  Allow pip to use the configured package index instead of wheelhouse.
USAGE
}

for arg in "$@"; do
  case "$arg" in
    --resume) RESUME=1 ;;
    --online) ONLINE=1 ;;
    -h|--help) usage; exit 0 ;;
    *) echo "Unknown argument: $arg" >&2; usage >&2; exit 2 ;;
  esac
done

if [[ ${EUID} -ne 0 ]]; then
  echo 'Run with sudo: sudo bash deploy/install.sh' >&2
  exit 1
fi

for tool in "$PYTHON_BIN" curl systemctl useradd install cp find sha256sum cmp; do
  command -v "$tool" >/dev/null || { echo "Missing command: $tool" >&2; exit 1; }
done
"$PYTHON_BIN" -c 'import sys, venv; assert sys.version_info >= (3, 11), "Python 3.11+ required"'

# Config/database/unit mean this is no longer a harmless partial first install.
CONFIG_EXISTS=0
[[ -e /etc/televk/config.json ]] && CONFIG_EXISTS=1
[[ -e /var/lib/televk/bridge.sqlite3 ]] && CONFIG_EXISTS=1
[[ -e /etc/systemd/system/televk.service ]] && CONFIG_EXISTS=1

if [[ -e /opt/televk ]]; then
  if [[ $RESUME -ne 1 ]]; then
    echo 'Existing /opt/televk detected. For the interrupted first install use: sudo bash deploy/install.sh --resume' >&2
    exit 1
  fi
  if [[ $CONFIG_EXISTS -ne 0 ]]; then
    echo '--resume is intentionally refused because config/database/systemd unit already exists. Use a release upgrade procedure instead.' >&2
    exit 1
  fi
  echo 'Removing only the incomplete /opt/televk tree; /etc/televk and /var/lib/televk are preserved.'
  rm -rf -- /opt/televk
elif [[ $RESUME -eq 1 && $CONFIG_EXISTS -ne 0 ]]; then
  echo '--resume is not applicable: configured installation artifacts already exist.' >&2
  exit 1
fi

if [[ $CONFIG_EXISTS -ne 0 ]]; then
  echo 'Existing TeleVK installation detected. Refusing to overwrite configuration/state.' >&2
  exit 1
fi

if [[ $ONLINE -eq 0 ]]; then
  if ! compgen -G "$SOURCE/wheelhouse/*.whl" >/dev/null; then
    cat >&2 <<'ERR'
Offline wheelhouse is empty.
On a PC with PyPI access run tools/prepare-wheelhouse.ps1 (Windows) or tools/prepare-wheelhouse.sh,
commit wheelhouse/*.whl and wheelhouse/SHA256SUMS to GitHub, pull the repository on the HTPC, then rerun this installer.
ERR
    exit 1
  fi
  if [[ ! -s "$SOURCE/wheelhouse/EXPECTED_SHA256SUMS" || ! -s "$SOURCE/wheelhouse/SHA256SUMS" ]]; then
    echo 'wheelhouse hash manifests are incomplete. Rebuild the wheelhouse with the provided preparation script.' >&2
    exit 1
  fi
  cmp -s "$SOURCE/wheelhouse/EXPECTED_SHA256SUMS" "$SOURCE/wheelhouse/SHA256SUMS" || {
    echo 'wheelhouse/SHA256SUMS differs from the release manifest. Rebuild wheelhouse; do not install.' >&2; exit 1;
  }
  echo 'Verifying wheelhouse checksums against the release manifest...'
  (cd "$SOURCE/wheelhouse" && sha256sum -c EXPECTED_SHA256SUMS)
fi

if ! id televk >/dev/null 2>&1; then
  useradd --system --user-group --home-dir /var/lib/televk --no-create-home --shell /usr/sbin/nologin televk
fi
getent group televk >/dev/null || { echo 'Group televk missing; inspect existing account.' >&2; exit 1; }

install -d -m 0755 -o root -g root /opt/televk
for item in televk tests docs deploy tools README.md CHANGELOG.md requirements.txt constraints.txt config.example.json .gitignore; do
  cp -R -- "$SOURCE/$item" /opt/televk/
done
if [[ -d "$SOURCE/wheelhouse" ]]; then
  cp -R -- "$SOURCE/wheelhouse" /opt/televk/
fi
find /opt/televk -type d -name __pycache__ -prune -exec rm -rf -- {} +
chown -R root:root /opt/televk
chmod -R u=rwX,go=rX /opt/televk
install -d -m 0700 -o televk -g televk /etc/televk /var/lib/televk

"$PYTHON_BIN" -m venv /opt/televk/.venv

if [[ $ONLINE -eq 1 ]]; then
  echo 'Installing dependencies from configured package index (--online requested)...'
  /opt/televk/.venv/bin/python -m pip install \
    --disable-pip-version-check --timeout 120 --retries 3 \
    -r /opt/televk/requirements.txt -c /opt/televk/constraints.txt
else
  echo 'Installing dependencies from local wheelhouse only (network disabled for pip)...'
  (cd /opt/televk/wheelhouse && sha256sum -c EXPECTED_SHA256SUMS)
  /opt/televk/.venv/bin/python -m pip install \
    --disable-pip-version-check --no-index --find-links=/opt/televk/wheelhouse \
    -r /opt/televk/requirements.txt -c /opt/televk/constraints.txt
fi

/opt/televk/.venv/bin/python -m pip check
(cd /opt/televk && PYTHONDONTWRITEBYTECODE=1 .venv/bin/python -m unittest discover -s tests -v)
install -m 0644 /opt/televk/deploy/televk.service /etc/systemd/system/televk.service
systemctl daemon-reload

cat <<'DONE'

Files and unit installed. Service was NOT started or enabled.
Next commands (interactive secrets are entered locally, not into chat):
  cd /opt/televk
  sudo -u televk .venv/bin/python -m televk --config /etc/televk/config.json init
When asked for the state directory, enter: /var/lib/televk
Then:
  sudo -u televk .venv/bin/python -m televk --config /etc/televk/config.json check
  sudo -u televk .venv/bin/python -m televk --config /etc/televk/config.json probe
  sudo -u televk .venv/bin/python -m televk --config /etc/televk/config.json probe --send-test
Only after successful probes and review of docs/ACCEPTANCE.md:
  sudo systemctl enable --now televk
DONE
