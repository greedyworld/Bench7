#!/usr/bin/env bash
# Step 5, on the main load generator (the one with this repo): k6 on this machine and on every extra
# generator, plus an ssh key so ./run_k6.sh can start k6 on them. The extra generators need no repo:
# k6 and the Linux tuning are installed on them over ssh. Safe to run again.
#   ./connect_agent.sh                                   this machine only (forgets saved agents)
#   ./connect_agent.sh <user>@<gen-2-private-ip> [user@host ...]
# Saved in k6-agents.env: ./run_k6.sh then uses these generators without --agents / --ssh-key.
# First login to a new generator uses whatever already works from here (log in with ssh -A to forward
# your laptop's key); otherwise the script prints the public key to add on the generator.
# env: K6_VERSION (default 2.3.0)
set -euo pipefail
cd "$(dirname "$0")"
[ "${1:-}" = -h ] || [ "${1:-}" = --help ] && { sed -n '2,10p' "$0"; exit 0; }
KEY=$HOME/.ssh/bench7_k6
SSHO=(-o BatchMode=yes -o ConnectTimeout=10 -o StrictHostKeyChecking=accept-new)
withkey() { ssh "${SSHO[@]}" -o IdentitiesOnly=yes -i "$KEY" "$@"; }

# k6 (pinned), python3, open-file and network limits for many concurrent connections. Runs here and,
# sent over ssh, on each extra generator.
install_k6() {
  set -euo pipefail
  local v=${K6_VERSION:-2.3.0} arch tmp
  if ! k6 version 2>/dev/null | grep -q "v$v "; then
    arch=$(dpkg --print-architecture)   # amd64 | arm64
    tmp=$(mktemp -d)
    curl -fsSL -o "$tmp/k6.tgz" "https://github.com/grafana/k6/releases/download/v$v/k6-v$v-linux-$arch.tar.gz"
    tar -xzf "$tmp/k6.tgz" -C "$tmp"
    sudo install -m 0755 "$tmp/k6-v$v-linux-$arch/k6" /usr/local/bin/k6
    rm -rf "$tmp"
  fi
  command -v python3 >/dev/null || { sudo apt-get update -qq && sudo apt-get install -y -qq python3; }
  sudo tee /etc/sysctl.d/90-bench7-loadgen.conf >/dev/null <<'EOF'
net.ipv4.ip_local_port_range = 1024 65535
net.ipv4.tcp_tw_reuse = 1
net.core.somaxconn = 65535
net.core.netdev_max_backlog = 65535
fs.file-max = 2097152
fs.nr_open = 2097152
EOF
  sudo sysctl -q -p /etc/sysctl.d/90-bench7-loadgen.conf
  printf '* soft nofile 1048576\n* hard nofile 1048576\n' | sudo tee /etc/security/limits.d/90-bench7.conf >/dev/null
  echo "$(k6 version | head -1) | $(python3 --version)"
}
remote_install() { printf 'K6_VERSION=%q\n' "${K6_VERSION:-2.3.0}"; declare -f install_k6; echo install_k6; }

echo "== k6 on this machine"
install_k6
if [ $# = 0 ]; then
  rm -f k6-agents.env
  echo "no extra generators (./run_k6.sh runs k6 on this machine only)"
  exit 0
fi

[ -f "$KEY" ] || ssh-keygen -q -t ed25519 -N "" -C "bench7-k6@$(hostname)" -f "$KEY"
missing=()
for h in "$@"; do
  echo "== $h"
  if ! withkey "$h" true 2>/dev/null; then
    if ssh "${SSHO[@]}" "$h" 'umask 077; mkdir -p ~/.ssh; cat >>~/.ssh/authorized_keys' <"$KEY.pub" 2>/dev/null &&
      withkey "$h" true 2>/dev/null; then
      echo "ssh key added"
    else
      echo "cannot log in"
      missing+=("$h")
      continue
    fi
  fi
  echo "-- tuning"
  withkey "$h" 'sudo bash -s' <scripts/tune.sh | tail -3
  echo "-- k6"
  remote_install | withkey "$h" 'bash -s' | tail -3
done

if [ ${#missing[@]} -gt 0 ]; then
  cat >&2 <<EOF

Could not log in to: ${missing[*]}
Add this line to ~/.ssh/authorized_keys on each of them, then run this again:
$(cat "$KEY.pub")
(or log in to this machine with "ssh -A" so your laptop's key is forwarded)
EOF
  exit 1
fi
umask 077
printf 'AGENTS=%s\nSSH_KEY=%s\n' "$(IFS=,; echo "$*")" "$KEY" >k6-agents.env
echo
echo "connected: $*  (saved in k6-agents.env; ./run_k6.sh uses them, --agents '' to run without)"
