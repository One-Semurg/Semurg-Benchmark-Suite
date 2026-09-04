#!/usr/bin/env bash
# FAISS VECTOR lane (MIT, Meta). FAISS is a LIBRARY (no server image), shipped as the pip wheel
# faiss-cpu. This lane stands up the official python:3.12-slim image, installs the PINNED faiss-cpu +
# numpy inside it, loads the SAME deterministic base set, builds an EXACT brute-force index
# (IndexFlatL2), runs the SAME top-K query set, and emits the neighbour ids as a canonical answer hash
# + timing. EXACT (IndexFlatL2), not an approximate HNSW/IVF index, because only an exact answer can be
# gated bit-exact against the independent reference.
#
# OFFLINE-SAFE (this is the whole point of the two install paths below):
#   * PRE-SEEDED WHEELHOUSE (fully offline): set FAISS_WHEELHOUSE=<dir> to a directory holding the
#     pre-downloaded wheels; the lane pip-installs with --no-index --find-links=<dir>, no internet at
#     all. Build the wheelhouse ONCE on a connected box:
#         mkdir wh
#         docker run --rm -v "$PWD/wh:/wh" python:3.12-slim \
#           pip download -d /wh numpy==2.0.2 faiss-cpu==1.9.0.post1
#         # then on the offline box: FAISS_WHEELHOUSE=$PWD/wh
#   * ONLINE (default): pip fetches the pinned wheels from PyPI on first run.
# If NEITHER is possible (no internet AND no wheelhouse), the lane emits a clean STATUS=skip with the
# exact fix -- it NEVER hard-errors and NEVER blocks the rest of the suite, and it NEVER fakes a number.
# Emits: LANE=faiss STATUS=ok LOAD_MS=.. QUERY_MS=.. ANSWER_HASH=..
set -uo pipefail; here="$(cd "$(dirname "$0")"&&pwd)"; . "$here/_common.sh"

# Shared VECTOR contract (identical vars every vector lane reads, set by the vector orchestrator).
BASE_CSV="${VEC_BASE_CSV:?}"; QUERY_CSV="${VEC_QUERY_CSV:?}"; META="${VEC_META:?}"
K="${VEC_K:-10}"
C="arena_faiss_$$"
IMG="${FAISS_IMAGE:-python:3.12-slim}"
# faiss-cpu 1.9.0.post1 is the first release that resolves cleanly with numpy 2.x (1.8.x pins numpy<2,
# so faiss-cpu==1.8.0.post1 + numpy==2.0.2 is an unsatisfiable pair). Both wheels are cp312 manylinux.
FAISS_PKG="${FAISS_PKG:-faiss-cpu==1.9.0.post1}"
NUMPY_PKG="${NUMPY_PKG:-numpy==2.0.2}"
WHEELS="${FAISS_WHEELHOUSE:-}"; WHEELS="${WHEELS%/}"

st="$(arena_docker_status)"; [ "$st" = ok ] || { echo "LANE=faiss STATUS=skip REASON=docker-$st($(arena_docker_fix "$st"))"; exit 0; }
docker rm -f "$C" >/dev/null 2>&1
trap 'docker rm -f "$C" >/dev/null 2>&1 || true' EXIT INT TERM
if ! docker run -d --name "$C" --label "$ARENA_LABEL=1" "$IMG" tail -f /dev/null >/tmp/faiss_$$.err 2>&1; then
  echo "LANE=faiss STATUS=skip REASON=image-start-failed([$(tr -d '\n' </tmp/faiss_$$.err|tail -c 80)])"; rm -f /tmp/faiss_$$.err; exit 0
fi
rm -f /tmp/faiss_$$.err
alive(){ docker ps -q --no-trunc | grep -q "$(docker inspect -f '{{.Id}}' "$C" 2>/dev/null)"; }
alive || { echo "LANE=faiss STATUS=skip REASON=container-did-not-stay-up"; exit 0; }

# --- install the pinned wheels, OFFLINE-SAFE. Wheelhouse first (no internet), else online, else a
#     clean SKIP with the exact remedy. A failure to fetch a wheel is ALWAYS a skip, never a hard error.
BUILD_HINT="build a wheelhouse once online: docker run --rm -v \$PWD/wh:/wh $IMG pip download -d /wh $NUMPY_PKG $FAISS_PKG; then set FAISS_WHEELHOUSE=\$PWD/wh"
install_ok=0; install_err=""
if [ -n "$WHEELS" ] && [ -d "$WHEELS" ] && ls "$WHEELS"/*.whl >/dev/null 2>&1; then
  # OFFLINE: install strictly from the pre-seeded wheelhouse, no network touched.
  if docker cp "$WHEELS" "$C":/wheelhouse >/dev/null 2>&1 && \
     docker exec "$C" pip install --quiet --no-input --no-index --find-links=/wheelhouse "$NUMPY_PKG" "$FAISS_PKG" >/tmp/faiss_pip_$$.err 2>&1; then
    install_ok=1
  else
    install_err="$(tr -d '\n' </tmp/faiss_pip_$$.err|tail -c 90)"
  fi
  rm -f /tmp/faiss_pip_$$.err
  if [ "$install_ok" != 1 ]; then
    echo "LANE=faiss STATUS=skip REASON=wheelhouse-install-failed(FAISS_WHEELHOUSE=$WHEELS missing $NUMPY_PKG/$FAISS_PKG for $IMG; $BUILD_HINT)([$install_err])"; exit 0
  fi
else
  if [ -n "$WHEELS" ]; then
    echo "LANE=faiss STATUS=skip REASON=wheelhouse-empty(FAISS_WHEELHOUSE=$WHEELS has no *.whl; $BUILD_HINT)"; exit 0
  fi
  # No wheelhouse -> needs internet. Detect offline up front (unless a custom pip mirror is configured)
  # so the skip reason is crisp and instant instead of a long pip timeout.
  if [ -z "${PIP_INDEX_URL:-}" ] && [ -z "${PIP_EXTRA_INDEX_URL:-}" ] && \
     ! docker exec "$C" python3 -c "import urllib.request; urllib.request.urlopen('https://pypi.org/simple/', timeout=6)" >/dev/null 2>&1; then
    echo "LANE=faiss STATUS=skip REASON=offline-no-wheelhouse(no internet for $FAISS_PKG and FAISS_WHEELHOUSE unset; to run offline, $BUILD_HINT)"; exit 0
  fi
  # ONLINE: fetch the pinned wheels from PyPI (or the configured mirror).
  if docker exec "$C" pip install --quiet --no-input "$NUMPY_PKG" "$FAISS_PKG" >/tmp/faiss_pip_$$.err 2>&1; then
    install_ok=1
  else
    install_err="$(tr -d '\n' </tmp/faiss_pip_$$.err|tail -c 90)"
  fi
  rm -f /tmp/faiss_pip_$$.err
  if [ "$install_ok" != 1 ]; then
    echo "LANE=faiss STATUS=skip REASON=pip-install-failed(could not fetch $FAISS_PKG; to run offline, $BUILD_HINT)([$install_err])"; exit 0
  fi
fi

# stage the SAME deterministic data the reference (and every other lane) uses.
docker cp "$BASE_CSV"  "$C":/base.csv  >/dev/null 2>&1
docker cp "$QUERY_CSV" "$C":/query.csv >/dev/null 2>&1
docker cp "$META"      "$C":/meta      >/dev/null 2>&1

# EXACT top-K driver. IndexFlatL2 == brute-force squared-L2; base added in id order so faiss idx == id.
# Timing: LOAD_MS = build+add; QUERY_MS = the batched top-K search. Canonical answer: for each query in
# qid order, K ids sorted (dist ASC, id ASC), joined ",", queries joined ";". Printed for the shell to
# hash with the SAME hash_answer() the reference used -> a bit-exact equal-answer gate.
OUT="$(docker exec -e VEC_K="$K" -i "$C" python3 - <<'PY' 2>&1
import time, numpy as np, faiss
meta = {}
for tok in open("/meta").read().split():
    if "=" in tok:
        k, v = tok.split("=", 1); meta[k] = v
D = int(meta["D"]); K = int(meta.get("K", 10))
def load(path):
    ids = []; rows = []
    with open(path) as f:
        for ln in f:
            ln = ln.strip()
            if not ln: continue
            p = ln.split(",")
            ids.append(int(p[0])); rows.append([float(x) for x in p[1:1+D]])
    return np.array(ids, dtype=np.int64), np.ascontiguousarray(np.array(rows, dtype=np.float32))
bid, base = load("/base.csv")            # base ids are 0..N-1 in file order
qid, query = load("/query.csv")
t0 = time.perf_counter()
index = faiss.IndexFlatL2(D)
index.add(base)
load_ms = int((time.perf_counter() - t0) * 1000)
t0 = time.perf_counter()
dist, idx = index.search(query, K)       # ascending distance
query_ms = int((time.perf_counter() - t0) * 1000)
# reorder queries by qid asc (they are already 0..Q-1 in file order) and apply (dist ASC, id ASC).
order = np.argsort(qid, kind="stable")
lines = []
for qi in order:
    pairs = sorted(zip(dist[qi].tolist(), [int(bid[j]) for j in idx[qi]]), key=lambda t: (t[0], t[1]))
    lines.append(",".join(str(i) for _, i in pairs[:K]))
print(f"FAISS_LOAD_MS={load_ms}")
print(f"FAISS_QUERY_MS={query_ms}")
print("FAISS_ANSWER=" + ";".join(lines))
PY
)"
alive || { echo "LANE=faiss STATUS=dnf REASON=container-died-during-query"; exit 0; }
lm="$(sed -n 's/^FAISS_LOAD_MS=\([0-9]*\).*/\1/p'  <<<"$OUT" | tail -1)"
qm="$(sed -n 's/^FAISS_QUERY_MS=\([0-9]*\).*/\1/p' <<<"$OUT" | tail -1)"
ans="$(sed -n 's/^FAISS_ANSWER=\(.*\)$/\1/p'       <<<"$OUT" | tail -1)"
if [ -z "$ans" ]; then
  echo "LANE=faiss STATUS=dnf REASON=no-result([$(printf '%s' "$OUT"|tr '\n' ' '|tail -c 120)])"; exit 0
fi
echo "LANE=faiss STATUS=ok LOAD_MS=${lm:-0} QUERY_MS=${qm:-0} ANSWER_HASH=$(hash_answer "$ans")"
