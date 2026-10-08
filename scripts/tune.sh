#!/usr/bin/env bash
# Max Linux tuning for the app + Postgres box. Idempotent; settings persist
# across reboots (sysctl.d, systemd drop-ins, tmpfiles). No resource limits are
# set anywhere: the app and Postgres take whatever they need.
# Usage: sudo ./scripts/tune.sh
set -euo pipefail
[ "$(id -u)" = 0 ] || { echo "run as root" >&2; exit 1; }

# ---- kernel / network ----
cat >/etc/sysctl.d/90-bench7.conf <<'EOF'
# listen backlog + accept queue for many keep-alive connections
net.core.somaxconn = 65535
net.ipv4.tcp_max_syn_backlog = 65535
net.core.netdev_max_backlog = 250000
net.core.netdev_budget = 600
net.core.netdev_budget_usecs = 8000
# ephemeral ports + fast TIME_WAIT reuse (app -> 127.0.0.1:5432 and generator)
net.ipv4.ip_local_port_range = 1024 65535
net.ipv4.tcp_tw_reuse = 1
net.ipv4.tcp_fin_timeout = 10
net.ipv4.tcp_max_tw_buckets = 2000000
# socket buffers
net.core.rmem_max = 16777216
net.core.wmem_max = 16777216
net.core.rmem_default = 262144
net.core.wmem_default = 262144
net.ipv4.tcp_rmem = 4096 87380 16777216
net.ipv4.tcp_wmem = 4096 65536 16777216
net.ipv4.tcp_mem = 786432 1048576 1572864
# keep cwnd on idle keep-alive connections, fast open, fq + bbr
net.ipv4.tcp_slow_start_after_idle = 0
net.ipv4.tcp_fastopen = 3
net.ipv4.tcp_no_metrics_save = 1
net.ipv4.tcp_mtu_probing = 1
net.core.default_qdisc = fq
net.ipv4.tcp_congestion_control = bbr
net.ipv4.tcp_syncookies = 1
# files
fs.file-max = 4194304
fs.nr_open = 4194304
# memory: never swap, flush dirty pages early and in small steps (Postgres)
vm.swappiness = 1
vm.dirty_background_ratio = 3
vm.dirty_ratio = 10
vm.dirty_expire_centisecs = 1000
vm.vfs_cache_pressure = 50
vm.overcommit_memory = 0
vm.max_map_count = 1048576
# scheduler: no autogroup (all tasks compete fairly), no NUMA balancing (1 node)
kernel.sched_autogroup_enabled = 0
kernel.numa_balancing = 0
EOF
modprobe tcp_bbr 2>/dev/null || true
echo tcp_bbr >/etc/modules-load.d/bench7.conf
sysctl -q --system

# ---- open-file limits (login sessions + every systemd unit) ----
cat >/etc/security/limits.d/90-bench7.conf <<'EOF'
* soft nofile 1048576
* hard nofile 1048576
root soft nofile 1048576
root hard nofile 1048576
EOF
mkdir -p /etc/systemd/system.conf.d
cat >/etc/systemd/system.conf.d/90-bench7.conf <<'EOF'
[Manager]
DefaultLimitNOFILE=1048576
DefaultTasksMax=infinity
DefaultCPUAccounting=yes
DefaultMemoryAccounting=yes
DefaultIOAccounting=yes
DefaultIPAccounting=yes
EOF
systemctl daemon-reexec

# ---- transparent huge pages: madvise (Postgres uses explicit huge pages,
#      JVM/.NET/Go opt in where they want) ----
cat >/etc/tmpfiles.d/90-bench7-thp.conf <<'EOF'
w /sys/kernel/mm/transparent_hugepage/enabled - - - - madvise
w /sys/kernel/mm/transparent_hugepage/defrag - - - - defer+madvise
EOF
systemd-tmpfiles --create /etc/tmpfiles.d/90-bench7-thp.conf

# ---- CPU: performance governor if the VM exposes cpufreq (most cloud VMs do not) ----
for g in /sys/devices/system/cpu/cpu*/cpufreq/scaling_governor; do
  [ -w "$g" ] && echo performance >"$g" || true
done

# ---- block devices: no scheduler on NVMe, small read-ahead for random 8 KB reads ----
for q in /sys/block/nvme*/queue; do
  echo none >"$q/scheduler" 2>/dev/null || true
  echo 64 >"$q/read_ahead_kb" 2>/dev/null || true
done
cat >/etc/udev/rules.d/90-bench7-nvme.rules <<'EOF'
ACTION=="add|change", KERNEL=="nvme[0-9]*n[0-9]*", ATTR{queue/scheduler}="none", ATTR{queue/read_ahead_kb}="64"
EOF

# ---- stop background noise that would steal CPU during a run ----
for u in snapd.service snapd.socket unattended-upgrades.service apt-daily.timer apt-daily-upgrade.timer \
         motd-news.timer man-db.timer fwupd-refresh.timer update-notifier-download.timer \
         update-notifier-motd.timer ua-timer.timer esm-cache.service sysstat-collect.timer; do
  systemctl disable --now "$u" >/dev/null 2>&1 || true
done

echo "tuning applied:"
sysctl net.core.somaxconn net.ipv4.tcp_congestion_control net.ipv4.tcp_tw_reuse vm.swappiness fs.nr_open
cat /sys/kernel/mm/transparent_hugepage/enabled
