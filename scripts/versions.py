#!/usr/bin/env python3
"""Record what a run actually used (app host, called by fw.sh run): runtime + library versions
of the framework, CPU model, kernel, Postgres version and the app's non-secret env knobs.
Versions come from lock files / installed packages, not from the manifests' ranges.
  python3 scripts/versions.py <fw> [--app-env /data/run/bench7-app.env] [--pg "18.1 ..."] > versions.json
"""
import argparse
import glob
import json
import os
import platform
import re
import subprocess
import zipfile

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
ap = argparse.ArgumentParser()
ap.add_argument("fw")
ap.add_argument("--app-env", default="/data/run/bench7-app.env")
ap.add_argument("--pg", default=None, help="Postgres server version string")
a = ap.parse_args()


def sh(*cmd):
    try:
        r = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
        return (r.stdout or r.stderr).strip().splitlines()[0] if (r.stdout or r.stderr).strip() else None
    except (OSError, subprocess.TimeoutExpired):
        return None


def first(*paths):
    for p in paths:
        p = os.path.expanduser(p)
        if os.path.exists(p):
            return p
    return None


def cargo_lock(names):
    out, txt = {}, open(f"{ROOT}/apps/axum/Cargo.lock").read()
    for m in re.finditer(r'\[\[package\]\]\nname = "([^"]+)"\nversion = "([^"]+)"', txt):
        if m.group(1) in names:
            out[m.group(1)] = m.group(2)
    return out


def go_mod(names):
    txt = open(f"{ROOT}/apps/gin/go.mod").read()
    return {m.group(1): m.group(2) for m in re.finditer(r"^\s*(\S+)\s+(v\S+)", txt, re.M)
            if any(n in m.group(1) for n in names)}


def csproj():
    txt = "".join(open(p).read() for p in glob.glob(f"{ROOT}/apps/aspnet/*.csproj"))
    return dict(re.findall(r'PackageReference Include="([^"]+)" Version="([^"]+)"', txt))


def spring_jar(names):
    jar = f"{ROOT}/apps/spring/target/bench7-spring.jar"
    if not os.path.exists(jar):
        return {}
    out = {}
    for n in zipfile.ZipFile(jar).namelist():
        m = re.match(r"BOOT-INF/lib/(.+?)-(\d[\w.\-]*)\.jar$", n)
        if m and any(m.group(1).startswith(x) for x in names):
            out[m.group(1)] = m.group(2)
    return out


def node_modules(app):
    pkg = json.load(open(f"{ROOT}/apps/{app}/package.json"))
    out = {}
    for name in pkg.get("dependencies", {}):
        p = f"{ROOT}/apps/{app}/node_modules/{name}/package.json"
        out[name] = json.load(open(p)).get("version") if os.path.exists(p) else None
    return out


def py_venv():
    py = f"{ROOT}/apps/fastapi/.venv/bin/python"
    code = ("import importlib.metadata as m, json, sys; print(json.dumps({'python': sys.version.split()[0], **{d: m.version(d) "
            "for d in ('fastapi', 'starlette', 'uvicorn', 'uvloop', 'httptools', 'asyncpg', 'orjson', 'PyJWT', 'pydantic') "
            "if d.lower() in {x.metadata['Name'].lower() for x in m.distributions()}}}))")
    try:
        return json.loads(subprocess.run([py, "-c", code], capture_output=True, text=True, timeout=30).stdout)
    except (OSError, ValueError, subprocess.TimeoutExpired):
        return {}


fw = a.fw
info = {"framework": fw}
try:
    if fw == "axum":
        info["runtime"] = sh(first("~/.cargo/bin/rustc", "/usr/local/bin/rustc") or "rustc", "--version")
        info["libs"] = cargo_lock({"axum", "tokio", "hyper", "sqlx", "moka", "jsonwebtoken", "mimalloc", "serde_json",
                                   "sonic-rs", "simd-json", "uuid"})
    elif fw == "gin":
        info["runtime"] = sh(first("/usr/local/go/bin/go") or "go", "version")
        info["libs"] = go_mod(("gin-gonic/gin", "jackc/pgx", "ristretto", "golang-jwt", "go-json", "sonic", "uuid"))
    elif fw == "aspnet":
        info["runtime"] = "dotnet " + (sh(first("/usr/local/dotnet/dotnet") or "dotnet", "--version") or "?")
        info["libs"] = csproj()
    elif fw == "spring":
        info["runtime"] = sh("java", "-version")
        info["libs"] = spring_jar(("spring-boot", "spring-webmvc", "tomcat-embed-core", "HikariCP", "postgresql",
                                   "jackson-databind", "java-jwt", "jjwt", "caffeine"))
    elif fw in ("fastify", "fastify-bun"):
        info["runtime"] = ("node " + (sh("node", "--version") or "?")) if fw == "fastify" else ("bun " + (sh("bun", "--version") or "?"))
        info["libs"] = node_modules("fastify")
    elif fw == "bun":
        info["runtime"] = "bun " + (sh("bun", "--version") or "?")
        info["libs"] = node_modules("bun")
    elif fw == "fastapi":
        libs = py_venv()
        info["runtime"] = "python " + str(libs.pop("python", "?"))
        info["libs"] = libs
except (OSError, ValueError) as e:
    info["error"] = str(e)

cpu = next((l.split(":", 1)[1].strip() for l in open("/proc/cpuinfo") if l.startswith("model name")), None)
info.update(cpu_model=cpu, ncpu=os.cpu_count(), kernel=platform.release(), postgres=a.pg)
knobs = {}
if os.path.exists(a.app_env):
    for line in open(a.app_env):
        k, _, v = line.strip().partition("=")
        if k and not k.startswith("#") and not re.search(r"SECRET|KEY|PASSWORD|DATABASE_URL", k):
            knobs[k] = v
info["env"] = knobs
print(json.dumps(info, indent=1))
