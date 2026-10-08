#!/usr/bin/env bash
# Undo everything bench7 did to this machine: app/sampler/k6 processes, systemd units, Postgres (packages,
# cluster, config), the /data partition on the local NVMe, Linux tuning, toolchains, k6, ssh keys, shell rc
# lines, and the untracked files in this checkout (bench7.env, results/, builds). The OS disk is not touched
# except for those files; the local NVMe partition is wiped only when it is the one bench7 created.
#   sudo ./reset.sh            asks for confirmation
#   sudo ./reset.sh -y         no question
#   sudo ./reset.sh -y --all   also delete this checkout
# Kernel settings already applied stay active until the next reboot (sudo reboot to get the defaults back).
set -u
[ "$(id -u)" = 0 ] || { echo "run as root: sudo ./reset.sh" >&2; exit 1; }
ROOT=$(cd "$(dirname "$0")" && pwd)
YES=0 ALL=0
for a in "$@"; do
  case "$a" in
    -y | --yes) YES=1 ;;
    --all) ALL=1 ;;
    *) echo "unknown option $a (-y, --all)" >&2; exit 2 ;;
  esac
done
U=${SUDO_USER:-$(stat -c %U "$ROOT")}
[ "$U" = root ] && UH=/root || UH=$(getent passwd "$U" | cut -d: -f6)
if [ "$YES" = 0 ]; then
  read -rp "Remove Postgres + its data, /data, toolchains, k6 and the bench7 tuning from $(hostname)? [y/N] " ok
  [ "$ok" = y ] || [ "$ok" = Y ] || exit 1
fi
echo "=== bench7 reset on $(hostname) $(date -u +%FT%TZ) ==="

echo "--- processes + units"
pkill -f '[s]cripts/sampler.py' 2>/dev/null
pkill -f 'db/(re)?seed\.sh' 2>/dev/null
pkill -x k6 2>/dev/null
pkill -f '[l]oadtest.py' 2>/dev/null
for u in $(systemctl list-units --all 'bench7*' --no-legend --plain 2>/dev/null | awk '{print $1}') \
         $(systemctl list-unit-files 'bench7*' --no-legend 2>/dev/null | awk '{print $1}'); do
  systemctl disable --now "$u" >/dev/null 2>&1
  systemctl stop "$u" >/dev/null 2>&1
done
systemctl stop 'postgresql*' >/dev/null 2>&1
rm -rf /etc/systemd/system/bench7* /etc/systemd/system/postgresql@18-main.service.d
systemctl daemon-reload
systemctl reset-failed >/dev/null 2>&1

echo "--- /data on the local NVMe"
if mountpoint -q /data; then
  fuser -km /data >/dev/null 2>&1
  umount /data || umount -l /data
fi
sed -i '\|^PARTLABEL=data /data |d' /etc/fstab
dev=$(lsblk -dn -o NAME,MODEL | awk '/NVMe Direct Disk/{print "/dev/"$1; exit}')
if [ -n "$dev" ] && [ "$(blkid -p -o value -s PART_ENTRY_NAME "${dev}p1" 2>/dev/null)" = data ]; then
  wipefs -aq "${dev}p1"
  wipefs -aq "$dev"
  partprobe "$dev" 2>/dev/null
  echo "wiped $dev"
fi
rm -rf /data /etc/bench7

echo "--- Linux tuning"
sysctl -qw vm.nr_hugepages=0
rm -f /etc/sysctl.d/90-bench7*.conf /etc/sysctl.d/91-bench7*.conf /etc/modules-load.d/bench7.conf \
  /etc/security/limits.d/90-bench7.conf /etc/systemd/system.conf.d/90-bench7.conf \
  /etc/tmpfiles.d/90-bench7-thp.conf /etc/udev/rules.d/90-bench7-nvme.rules

echo "--- toolchains + k6"
rm -rf /usr/local/go /usr/local/dotnet /opt/maven /opt/apache-maven-* \
  /usr/local/bin/{go,dotnet,mvn,bun,uv,uvx,k6,node,npm,npx,corepack} \
  /usr/local/lib/node_modules /usr/local/include/node /usr/local/share/doc/node \
  /usr/local/share/man/man1/node.1 /usr/local/share/systemtap/tapset/node.stp /tmp/dotnet-install.sh
sed -i -e '/^DOTNET_ROOT=/d' -e '/^DOTNET_CLI_TELEMETRY_OPTOUT=/d' /etc/environment
for h in "$UH" /root; do
  rm -rf "$h"/{pg-seed*,.bun,.cargo,.rustup,.m2,.npm,.nuget,.dotnet,.cache,go,.config/go} \
    "$h"/.local/share/uv "$h"/.local/bin/{uv,uvx,env,env.fish}
  for rc in "$h"/.bashrc "$h"/.profile "$h"/.zshrc "$h"/.bash_profile; do
    [ -f "$rc" ] && sed -i -e '\#\.cargo/env#d' -e '\#\.local/bin/env#d' -e '/BUN_INSTALL/d' -e '/^# bun$/d' "$rc"
  done
done

echo "--- apt packages (Postgres, Temurin, Node.js, build tools)"
export DEBIAN_FRONTEND=noninteractive
APT=(apt-get -y -qq -o DPkg::Lock::Timeout=600)   # wait for unattended-upgrades instead of failing
apt_quiet() { local log; log=$("${APT[@]}" "$@" 2>&1) || { echo "apt-get $* FAILED:"; echo "$log" | tail -5; }; }
# installed (or config-only) packages matching the patterns: apt-get purge fails as a whole when one glob
# matches nothing in the apt cache (temurin-* once its repo is gone), so purge by exact name
installed() { dpkg-query -W -f='${db:Status-Abbrev} ${Package}\n' "$@" 2>/dev/null | awk '$1 != "un" && $1 != "pn" {print $2}'; }
# the Postgres maintainer scripts source /usr/share/postgresql-common: when an older reset deleted it,
# put it back first or the purge fails half-way
if dpkg-query -W -f='${Status}' postgresql-common 2>/dev/null | grep -q installed && [ ! -d /usr/share/postgresql-common ]; then
  echo "repairing postgresql-common (its files are missing)"
  apt_quiet install --reinstall postgresql-common postgresql-client-common
  if [ ! -d /usr/share/postgresql-common ]; then
    # that version is no longer downloadable (PGDG repo removed): skip the scripts that need the files
    for f in /var/lib/dpkg/info/postgresql*.{prerm,postrm} /var/lib/dpkg/info/libpq*.{prerm,postrm}; do
      [ -f "$f" ] && grep -q postgresql-common "$f" && printf '#!/bin/sh\nexit 0\n' >"$f"
    done
  fi
fi
mapfile -t PKGS < <(installed 'postgresql*' 'libpq*' 'temurin-*' nodejs fio sysstat jq build-essential pkg-config)
[ "${#PKGS[@]}" -gt 0 ] && apt_quiet purge "${PKGS[@]}"
apt_quiet autoremove --purge
# only config/data dirs: package-owned files (/usr/share/postgresql-common) must go through dpkg
rm -rf /etc/postgresql /etc/postgresql-common /var/lib/postgresql /var/log/postgresql \
  /etc/apt/sources.list.d/pgdg* /etc/apt/sources.list.d/adoptium* /etc/apt/sources.list.d/nodesource* \
  /etc/apt/keyrings/adoptium* /etc/apt/keyrings/nodesource* /usr/share/keyrings/adoptium* \
  /usr/share/keyrings/nodesource* /etc/apt/trusted.gpg.d/apt.postgresql.org* /etc/apt/preferences.d/nodejs* \
  /etc/apt/preferences.d/nsolid*
apt-get update -qq >/dev/null 2>&1

echo "--- background timers/services that tune.sh turned off"
for u in snapd.socket snapd.service unattended-upgrades.service apt-daily.timer apt-daily-upgrade.timer \
  motd-news.timer man-db.timer fwupd-refresh.timer update-notifier-download.timer \
  update-notifier-motd.timer ua-timer.timer esm-cache.service; do
  systemctl enable --now "$u" >/dev/null 2>&1
done

echo "--- ssh keys made by bench7 (your own login keys stay)"
rm -f "$UH"/.ssh/campaign_ed25519* "$UH"/.ssh/bench7_k6*
[ -f "$UH/.ssh/authorized_keys" ] && sed -i -e '/campaign/d' -e '/bench7-k6@/d' "$UH/.ssh/authorized_keys"

echo "--- checkout $ROOT"
if [ ! -d "$ROOT/.git" ] || [ ! -f "$ROOT/run_axum.sh" ]; then
  echo "not a bench7 checkout: left alone"
elif [ "$ALL" = 1 ]; then
  rm -rf "$ROOT"
  echo "deleted"
else
  sudo -u "$U" git -C "$ROOT" clean -fdxq
  rm -rf "$ROOT"/results "$ROOT"/seed.log "$ROOT"/bench7.env   # in case root owned some of them
  echo "untracked files removed (bench7.env, results/, builds)"
fi

echo "--- left over"
left=0
for c in psql postgres cargo rustc go dotnet java mvn node bun uv k6 jq fio; do
  if p=$(PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin:$UH/.cargo/bin:$UH/.bun/bin:$UH/.local/bin command -v $c 2>/dev/null); then
    echo "still present: $c -> $p"
    left=1
  fi
done
mountpoint -q /data && { echo "still mounted: /data"; left=1; }
pk=$(installed 'postgresql*' 'libpq*' 'temurin-*' nodejs | xargs)
[ -n "$pk" ] && { echo "still installed: $pk"; left=1; }
[ "$left" = 0 ] && echo "nothing left"
echo "=== bench7 reset done (reboot to get the kernel defaults back) ==="
