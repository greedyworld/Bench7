#!/usr/bin/env bash
# Toolchains to build and run the apps natively, pinned to the exact versions the
# published results were produced with (override with the env vars below).
# Usage: sudo ./scripts/install-toolchains.sh [part...]     (user tools go to $SUDO_USER)
#   parts: base pgclient rust node bun java dotnet go python     (none given = all)
#   run_<fw>.sh calls this with only the parts its framework needs; installed parts are skipped.
set -euo pipefail
[ "$(id -u)" = 0 ] || { echo "run as root" >&2; exit 1; }
U=${SUDO_USER:?run with sudo from your normal user}
UH=$(getent passwd "$U" | cut -d: -f6)
asuser() { sudo -u "$U" -H bash -lc "$1"; }
apt_install() { DEBIAN_FRONTEND=noninteractive apt-get install -y -qq "$@" >/dev/null; }

RUST_VER=${RUST_VER:-1.99.0}
NODE_VER=${NODE_VER:-24.21.0}
BUN_VER=${BUN_VER:-1.4.2}
TEMURIN_VER=${TEMURIN_VER:-25.0.4.1.0+1-0}   # apt version of temurin-25-jdk
MAVEN_VER=${MAVEN_VER:-3.9.16}
DOTNET_SDK_VER=${DOTNET_SDK_VER:-10.0.401}
GO_VER=${GO_VER:-go1.27.1}
UV_VER=${UV_VER:-0.12.21}
PY_VER=${PY_VER:-3.13.15}
PG_MAJOR=${PG_MAJOR:-18}

PARTS=${*:-base pgclient rust node bun java dotnet go python}

base() { # compilers for native crates/modules + what the scripts use
  local p need=""
  for p in build-essential pkg-config curl ca-certificates gnupg python3; do dpkg -s "$p" >/dev/null 2>&1 || need="$need $p"; done
  [ -z "$need" ] || { apt-get update -qq; apt_install $need; }
}

pgclient() { # psql + pg_isready from PGDG, same major as the server (the app host may have no server)
  command -v psql >/dev/null && command -v pg_isready >/dev/null && return 0
  apt_install postgresql-common
  /usr/share/postgresql-common/pgdg/apt.postgresql.org.sh -y >/dev/null
  apt_install "postgresql-client-$PG_MAJOR"
}

rust() {
  asuser 'command -v rustup >/dev/null || curl -fsSL https://sh.rustup.rs | sh -s -- -y --profile minimal --default-toolchain none -q'
  asuser ". ~/.cargo/env && rustup toolchain install $RUST_VER --profile minimal >/dev/null 2>&1 && rustup default $RUST_VER >/dev/null"
  asuser '. ~/.cargo/env && rustc --version'
}

node() {
  if [ "$(/usr/local/bin/node -v 2>/dev/null)" != "v$NODE_VER" ]; then
    curl -fsSL "https://nodejs.org/dist/v$NODE_VER/node-v$NODE_VER-linux-x64.tar.xz" | tar -xJ -C /usr/local --strip-components=1
  fi
  echo "node $(/usr/local/bin/node -v)"
}

bun() {
  if [ "$("$UH/.bun/bin/bun" --version 2>/dev/null)" != "$BUN_VER" ]; then
    asuser "curl -fsSL https://bun.sh/install | bash -s bun-v$BUN_VER >/dev/null"
  fi
  ln -sf "$UH/.bun/bin/bun" /usr/local/bin/bun
  echo "bun $(/usr/local/bin/bun --version)"
}

java() { # JDK 25 (Temurin) + Maven
  if ! dpkg -s temurin-25-jdk 2>/dev/null | grep -qx "Version: $TEMURIN_VER"; then
    install -d /etc/apt/keyrings
    curl -fsSL https://packages.adoptium.net/artifactory/api/gpg/key/public | gpg --dearmor --yes -o /etc/apt/keyrings/adoptium.gpg
    echo "deb [signed-by=/etc/apt/keyrings/adoptium.gpg] https://packages.adoptium.net/artifactory/deb $(. /etc/os-release; echo "$VERSION_CODENAME") main" \
      >/etc/apt/sources.list.d/adoptium.list
    apt-get update -qq
    apt_install --allow-downgrades "temurin-25-jdk=$TEMURIN_VER"
  fi
  if [ "$(readlink -f /opt/maven)" != "/opt/apache-maven-$MAVEN_VER" ]; then
    curl -fsSL "https://archive.apache.org/dist/maven/maven-3/$MAVEN_VER/binaries/apache-maven-$MAVEN_VER-bin.tar.gz" | tar -xz -C /opt
    ln -sfn "/opt/apache-maven-$MAVEN_VER" /opt/maven
    ln -sf /opt/maven/bin/mvn /usr/local/bin/mvn
  fi
  command java -version 2>&1 | head -1
  /usr/local/bin/mvn -v 2>/dev/null | head -1
}

dotnet() {
  if ! /usr/local/bin/dotnet --list-sdks 2>/dev/null | grep -q "^$DOTNET_SDK_VER "; then
    curl -fsSL https://dot.net/v1/dotnet-install.sh -o /tmp/dotnet-install.sh
    bash /tmp/dotnet-install.sh --version "$DOTNET_SDK_VER" --install-dir /usr/local/dotnet >/dev/null
    ln -sf /usr/local/dotnet/dotnet /usr/local/bin/dotnet
  fi
  grep -q DOTNET_ROOT /etc/environment || echo 'DOTNET_ROOT=/usr/local/dotnet' >>/etc/environment
  grep -q DOTNET_CLI_TELEMETRY_OPTOUT /etc/environment || echo 'DOTNET_CLI_TELEMETRY_OPTOUT=1' >>/etc/environment
  echo "dotnet $(/usr/local/bin/dotnet --version)"
}

go() {
  if [ "$(/usr/local/go/bin/go version 2>/dev/null | awk '{print $3}')" != "$GO_VER" ]; then
    rm -rf /usr/local/go
    curl -fsSL "https://go.dev/dl/$GO_VER.linux-$(dpkg --print-architecture).tar.gz" | tar -xz -C /usr/local
  fi
  ln -sf /usr/local/go/bin/go /usr/local/bin/go
  /usr/local/bin/go version
}

python() { # uv + CPython
  if [ "$("$UH/.local/bin/uv" --version 2>/dev/null | awk '{print $2}')" != "$UV_VER" ]; then
    asuser "curl -fsSL https://astral.sh/uv/$UV_VER/install.sh | sh >/dev/null 2>&1"
  fi
  asuser "~/.local/bin/uv python install $PY_VER >/dev/null"
  ln -sf "$UH/.local/bin/uv" /usr/local/bin/uv
  asuser '"$(~/.local/bin/uv python find 3.13)" --version'
}

for p in $PARTS; do
  case "$p" in
    base | pgclient | rust | node | bun | java | dotnet | go | python) echo "== $p"; "$p" ;;
    *) echo "unknown part '$p' (base pgclient rust node bun java dotnet go python)" >&2; exit 2 ;;
  esac
done
