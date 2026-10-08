#!/usr/bin/env python3
"""Write the load-generator bundle (app host, called by run_<fw>.sh).

The bundle holds what k6 needs and nothing secret beyond bench JWTs:
  users, public_cursors, like_posts  (seed ids from results/seed-export.json)
  tokens [{uid, tok}]                 (minted here via POST /auth/token; the issuer key never leaves this host)
The sampler serves it at GET http://<app-host>:41901/bundle.
"""
import argparse
import json
import os
import re
import time
import urllib.request
from concurrent.futures import ThreadPoolExecutor

ap = argparse.ArgumentParser()
ap.add_argument("--env", required=True, help="bench7.env (TOKEN_ISSUER_KEY)")
ap.add_argument("--seed", required=True, help="results/seed-export.json")
ap.add_argument("--base", default="http://127.0.0.1:8080")
ap.add_argument("--tokens", type=int, default=512)
ap.add_argument("--framework", required=True)
ap.add_argument("--out", required=True)
ap.add_argument("--info", help="versions.json (scripts/versions.py) copied into the bundle as 'info'")
a = ap.parse_args()

key = re.search(r"^TOKEN_ISSUER_KEY=(\S+)", open(a.env).read(), re.M).group(1)
seed = json.load(open(a.seed))
# readers: users with >= 5 private posts and >= 5 received messages, so direct reads return full pages
readers = seed.get("readers") or seed["users"]
ids = readers[:: max(1, len(readers) // a.tokens)][: a.tokens]


def tok(u):
    r = urllib.request.Request(a.base + "/auth/token", data=json.dumps({"user_id": u}).encode(), method="POST",
                               headers={"content-type": "application/json", "x-issuer-key": key})
    with urllib.request.urlopen(r, timeout=10) as resp:
        return {"uid": u, "tok": "Bearer " + json.loads(resp.read())["token"]}


with ThreadPoolExecutor(16) as ex:
    tokens = list(ex.map(tok, ids))

bundle = {"framework": a.framework, "created": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
          "users": seed["users"], "public_cursors": seed["public_cursors"], "like_posts": seed["like_posts"],
          "tokens": tokens}
if a.info and os.path.exists(a.info):
    bundle["info"] = json.load(open(a.info))
tmp = a.out + ".tmp"
fd = os.open(tmp, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
with os.fdopen(fd, "w") as f:
    json.dump(bundle, f, separators=(",", ":"))
os.replace(tmp, a.out)
print(f"bundle: {len(tokens)} tokens, {len(seed['users'])} users, {len(seed['public_cursors'])} cursors -> {a.out}")
