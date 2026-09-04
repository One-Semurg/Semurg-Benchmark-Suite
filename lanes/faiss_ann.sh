#!/usr/bin/env bash
# FAISS ANN lane (MIT, Meta) -- the APPROXIMATE index board. Where lanes/faiss.sh builds an EXACT
# IndexFlatL2 (gate-able bit-exact but a brute-force scan), THIS lane builds FAISS's HNSW graph index
# (IndexHNSWFlat) -- the approximate nearest-neighbour index the vector world actually competes on -- and
# is scored by RECALL@K against the independent EXACT L2 reference, plus its query latency. Same base +
# query vectors as every other vector lane, so the ANN board is apples-to-apples (all approximate, all
# recall-scored on the same task).
#
# OFFLINE-SAFE exactly like lanes/faiss.sh: a pre-seeded FAISS_WHEELHOUSE runs fully offline; otherwise
# the pinned wheels come from PyPI; if neither is possible it emits a clean STATUS=skip with the exact
# fix. It never hard-errors, never blocks the suite, and never fabricates a recall or a latency.
# Emits: LANE=faiss_ann STATUS=ok-recall RECALL_AT_K=0.NN QUERY_MS=.. LOAD_MS=.. K=.. MODE=hnsw
set -uo pipefail; here="$(cd "$(dirname "$0")"&&pwd)"; . "$here/_common.sh"

BASE_CSV="${VEC_BASE_CSV:?}"; QUERY_CSV="${VEC_QUERY_CSV:?}"; META="${VEC_META:?}"
K="${VEC_K:-10}"
REF_ANSWER="${VEC_REF_ANSWER:-${BASE_CSV%.base.csv}.answer.txt}"
# HNSW knobs (standard ann-benchmarks defaults; a higher ef trades latency for recall).
HNSW_M="${FAISS_HNSW_M:-32}"; HNSW_EFC="${FAISS_HNSW_EFC:-200}"; HNSW_EFS="${FAISS_HNSW_EFS:-128}"
C="arena_faiss_ann_$$"
IMG="${FAISS_IMAGE:-python:3.12-slim}"
FAISS_PKG="${FAISS_PKG:-faiss-cpu==1.9.0.post1}"
NUMPY_PKG="${NUMPY_PKG:-numpy==2.0.2}"
WHEELS="${FAISS_WHEELHOUSE:-}"; WHEELS="${WHEELS%/}"

[ -f "$REF_ANSWER" ] || { echo "LANE=faiss_ann STATUS=skip REASON=exact-reference-missing($REF_ANSWER; run gen_vectors.sh first)"; exit 0; }
st="$(arena_docker_status)"; [ "$st" = ok ] || { echo "LANE=faiss_ann STATUS=skip REASON=docker-$st($(arena_docker_fix "$st"))"; exit 0; }
docker rm -f "$C" >/dev/null 2>&1
trap 'docker rm -f "$C" >/dev/null 2>&1 || true' EXIT INT TERM
if ! docker run -d --name "$C" --label "$ARENA_LABEL=1" "$IMG" tail -f /dev/null >/tmp/faiss_ann_$$.err 2>&1; then
  echo "LANE=faiss_ann STATUS=skip REASON=image-start-failed([$(tr -d '\n' </tmp/faiss_ann_$$.err|tail -c 80)])"; rm -f /tmp/faiss_ann_$$.err; exit 0
fi
rm -f /tmp/faiss_ann_$$.err
alive(){ docker ps -q --no-trunc | grep -q "$(docker inspect -f '{{.Id}}' "$C" 2>/dev/null)"; }
alive || { echo "LANE=faiss_ann STATUS=skip REASON=container-did-not-stay-up"; exit 0; }

# --- install the pinned wheels, OFFLINE-SAFE (identical logic to lanes/faiss.sh). ---
BUILD_HINT="build a wheelhouse once online: docker run --rm -v \$PWD/wh:/wh $IMG pip download -d /wh $NUMPY_PKG $FAISS_PKG; then set FAISS_WHEELHOUSE=\$PWD/wh"
install_ok=0; install_err=""
if [ -n "$WHEELS" ] && [ -d "$WHEELS" ] && ls "$WHEELS"/*.whl >/dev/null 2>&1; then
  if docker cp "$WHEELS" "$C":/wheelhouse >/dev/null 2>&1 && \
     docker exec "$C" pip install --quiet --no-input --no-index --find-links=/wheelhouse "$NUMPY_PKG" "$FAISS_PKG" >/tmp/faiss_ann_pip_$$.err 2>&1; then
    install_ok=1
  else
    install_err="$(tr -d '\n' </tmp/faiss_ann_pip_$$.err|tail -c 90)"
  fi
  rm -f /tmp/faiss_ann_pip_$$.err
  if [ "$install_ok" != 1 ]; then
    echo "LANE=faiss_ann STATUS=skip REASON=wheelhouse-install-failed(FAISS_WHEELHOUSE=$WHEELS missing $NUMPY_PKG/$FAISS_PKG for $IMG; $BUILD_HINT)([$install_err])"; exit 0
  fi
else
  if [ -n "$WHEELS" ]; then
    echo "LANE=faiss_ann STATUS=skip REASON=wheelhouse-empty(FAISS_WHEELHOUSE=$WHEELS has no *.whl; $BUILD_HINT)"; exit 0
  fi
  if [ -z "${PIP_INDEX_URL:-}" ] && [ -z "${PIP_EXTRA_INDEX_URL:-}" ] && \
     ! docker exec "$C" python3 -c "import urllib.request; urllib.request.urlopen('https://pypi.org/simple/', timeout=6)" >/dev/null 2>&1; then
    echo "LANE=faiss_ann STATUS=skip REASON=offline-no-wheelhouse(no internet for $FAISS_PKG and FAISS_WHEELHOUSE unset; to run offline, $BUILD_HINT)"; exit 0
  fi
  if docker exec "$C" pip install --quiet --no-input "$NUMPY_PKG" "$FAISS_PKG" >/tmp/faiss_ann_pip_$$.err 2>&1; then
    install_ok=1
  else
    install_err="$(tr -d '\n' </tmp/faiss_ann_pip_$$.err|tail -c 90)"
  fi
  rm -f /tmp/faiss_ann_pip_$$.err
  if [ "$install_ok" != 1 ]; then
    echo "LANE=faiss_ann STATUS=skip REASON=pip-install-failed(could not fetch $FAISS_PKG; to run offline, $BUILD_HINT)([$install_err])"; exit 0
  fi
fi

# stage the SAME deterministic data + the independent exact reference every other lane uses.
docker cp "$BASE_CSV"   "$C":/base.csv   >/dev/null 2>&1
docker cp "$QUERY_CSV"  "$C":/query.csv  >/dev/null 2>&1
docker cp "$META"       "$C":/meta       >/dev/null 2>&1
docker cp "$REF_ANSWER" "$C":/ref.txt    >/dev/null 2>&1

# APPROXIMATE HNSW driver. LOAD_MS = build+add; QUERY_MS = the batched top-K search. RECALL_AT_K = mean
# set overlap of the returned ids vs the exact L2 reference (per query, averaged). Over-fetch is NOT used
# here on purpose -- this is the honest approximate answer HNSW returns at the configured efSearch.
OUT="$(docker exec -e VEC_K="$K" -e HNSW_M="$HNSW_M" -e HNSW_EFC="$HNSW_EFC" -e HNSW_EFS="$HNSW_EFS" -i "$C" python3 - <<'PY' 2>&1
import os, time, numpy as np, faiss
meta = {}
for tok in open("/meta").read().split():
    if "=" in tok:
        k, v = tok.split("=", 1); meta[k] = v
D = int(meta["D"]); K = int(meta.get("K", 10))
M = int(os.environ.get("HNSW_M", "32")); EFC = int(os.environ.get("HNSW_EFC", "200")); EFS = int(os.environ.get("HNSW_EFS", "128"))
def load(path):
    ids = []; rows = []
    with open(path) as f:
        for ln in f:
            ln = ln.strip()
            if not ln: continue
            p = ln.split(",")
            ids.append(int(p[0])); rows.append([float(x) for x in p[1:1+D]])
    return np.array(ids, dtype=np.int64), np.ascontiguousarray(np.array(rows, dtype=np.float32))
bid, base = load("/base.csv")            # base ids are 0..N-1 in file order => faiss idx == id
qid, query = load("/query.csv")
t0 = time.perf_counter()
index = faiss.IndexHNSWFlat(D, M)        # APPROXIMATE graph index (L2)
index.hnsw.efConstruction = EFC
index.add(base)
load_ms = int((time.perf_counter() - t0) * 1000)
index.hnsw.efSearch = EFS
t0 = time.perf_counter()
dist, idx = index.search(query, K)       # approximate top-K
query_ms = int((time.perf_counter() - t0) * 1000)
# recall@K vs the exact L2 reference (set overlap). ids come ONLY from faiss's returned hits.
ref = [ln.split(",") for ln in open("/ref.txt").read().strip().split(";")]
order = np.argsort(qid, kind="stable")
hit = tot = 0
for pos, qi in enumerate(order):
    got = set(int(bid[j]) for j in idx[qi] if j >= 0)
    rset = set(int(x) for x in ref[pos])
    tot += len(rset); hit += len(got & rset)
recall = (hit / tot) if tot > 0 else 0.0
print(f"FAISS_ANN_LOAD_MS={load_ms}")
print(f"FAISS_ANN_QUERY_MS={query_ms}")
print(f"FAISS_ANN_RECALL={recall:.4f}")
PY
)"
alive || { echo "LANE=faiss_ann STATUS=dnf REASON=container-died-during-query"; exit 0; }
lm="$(sed -n 's/^FAISS_ANN_LOAD_MS=\([0-9]*\).*/\1/p'  <<<"$OUT" | tail -1)"
qm="$(sed -n 's/^FAISS_ANN_QUERY_MS=\([0-9]*\).*/\1/p' <<<"$OUT" | tail -1)"
rc="$(sed -n 's/^FAISS_ANN_RECALL=\([0-9.]*\).*/\1/p'  <<<"$OUT" | tail -1)"
if [ -z "$rc" ]; then
  echo "LANE=faiss_ann STATUS=dnf REASON=no-result([$(printf '%s' "$OUT"|tr '\n' ' '|tail -c 120)])"; exit 0
fi
echo "LANE=faiss_ann STATUS=ok-recall RECALL_AT_K=$rc QUERY_MS=${qm:-0} LOAD_MS=${lm:-0} K=$K MODE=hnsw(M=$HNSW_M,efc=$HNSW_EFC,efs=$HNSW_EFS)"
