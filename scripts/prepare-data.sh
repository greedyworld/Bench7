#!/usr/bin/env bash
# Make sure /data is mounted from the ephemeral local NVMe disk.
# That disk can come back blank after the VM is stopped; this re-creates partition + XFS ONLY when the
# disk is completely blank (no partition table, no filesystem). Never touches a disk with data.
# Usage: sudo ./scripts/prepare-data.sh        (also run at boot by bench7-data.service)
set -euo pipefail
[ "$(id -u)" = 0 ] || { echo "run as root" >&2; exit 1; }
SIZE=${DATA_SIZE:-$(cat /etc/bench7/data-size 2>/dev/null || echo 40GiB)}   # saved by setup_postgres.sh

mkrun() { install -d -m 0777 /data/run; [ -d /data/pg ] || install -d -o postgres -g postgres /data/pg; }

if mountpoint -q /data; then mkrun; echo "/data already mounted"; exit 0; fi

dev=${DATA_DEV:-}
if [ -z "$dev" ]; then
  dev=$(lsblk -dn -o NAME,MODEL | awk '/NVMe Direct Disk/{print "/dev/"$1; exit}')
fi
[ -b "${dev:-}" ] || { echo "no local NVMe direct disk found" >&2; exit 1; }
part=${dev}p1

if [ ! -b "$part" ]; then
  # blank disk only: no partition table signature and no filesystem on the raw device
  if [ -n "$(blkid -p -o value -s PTTYPE "$dev" 2>/dev/null)" ] || [ -n "$(blkid -p -o value -s TYPE "$dev" 2>/dev/null)" ]; then
    echo "$dev has a signature but no ${part}; refusing to touch it" >&2; exit 1
  fi
  echo "partitioning blank $dev (${SIZE})"
  parted -s "$dev" mklabel gpt mkpart data xfs 1MiB "$SIZE"
  udevadm settle
fi
if [ -z "$(blkid -p -o value -s TYPE "$part" 2>/dev/null)" ]; then
  echo "mkfs.xfs $part"
  mkfs.xfs -q "$part"
fi
mkdir -p /data
# NVMe device names can swap across a deallocate/start (the OS disk may become nvme0n1), so mount by the
# GPT partition label and drop any older /dev/nvme* line for /data
sed -i '\#^/dev/nvme[0-9]*n1p1 /data #d' /etc/fstab
grep -q "^PARTLABEL=data /data " /etc/fstab || echo "PARTLABEL=data /data xfs defaults,noatime,nofail 0 2" >>/etc/fstab
mountpoint -q /data || mount /data
mkrun
df -h /data | tail -1
