#!/usr/bin/env bash
# Installs the persistent systemd units for the app host:
#   bench7-data.service                 re-creates /data when the ephemeral local NVMe is blank (enabled)
#   postgresql@18-main drop-in          waits for /data, fails loudly when the cluster is gone
#   bench7-app.service                  the app server; framework picked by /data/run/bench7-app.env (not enabled:
#                                       benchmarks start/stop it via ./scripts/app.sh up|down)
# Usage: sudo ./scripts/install-units.sh
set -euo pipefail
[ "$(id -u)" = 0 ] || { echo "run as root" >&2; exit 1; }
ROOT=$(cd "$(dirname "$0")/.." && pwd)
S=$ROOT/deploy/systemd
if systemctl is-active -q bench7-app && [ ! -f /etc/systemd/system/bench7-app.service ]; then
  echo "a transient bench7-app is running; stop it first: ./scripts/app.sh down" >&2; exit 1
fi
systemctl reset-failed bench7-app 2>/dev/null || true
U=${SUDO_USER:-$(stat -c %U "$ROOT")}
G=$(id -gn "$U")
for f in bench7-app.service bench7-data.service; do
  sed -e "s|@USER@|$U|g" -e "s|@GROUP@|$G|g" -e "s|@ROOT@|$ROOT|g" "$S/$f" >"/etc/systemd/system/$f"
  chmod 0644 "/etc/systemd/system/$f"
done
install -d /etc/systemd/system/postgresql@18-main.service.d
install -m 0644 "$S/postgresql@18-main.service.d/bench7.conf" /etc/systemd/system/postgresql@18-main.service.d/
chmod +x "$ROOT/scripts/app.sh" "$ROOT/scripts/prepare-data.sh"
systemctl daemon-reload
systemctl enable bench7-data.service >/dev/null 2>&1
systemctl enable postgresql@18-main.service >/dev/null 2>&1 || true   # not installed yet: install-pg.sh enables it
systemctl start bench7-data.service
systemctl --no-pager --lines=0 status bench7-data postgresql@18-main 2>/dev/null | grep -E '●|Active:' || true
echo "installed; bench7-app: $(systemctl is-enabled bench7-app 2>/dev/null || true)"
