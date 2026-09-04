#!/usr/bin/env bash
# Qdrant ANN lane (Apache-2.0) -- the APPROXIMATE index board. Where lanes/qdrant.sh runs EXACT/FLAT
# (params.exact=true, a full brute-force scan, gate-able bit-exact), THIS lane runs Qdrant's NATIVE HNSW
# index (params.exact=false + hnsw_ef) -- the approximate nearest-neighbour path Qdrant is actually built
# for -- and is scored by RECALL@K against the independent EXACT L2 reference, plus its query latency. The
# collection is forced to build the HNSW index (indexing_threshold lowered) and the lane WAITS for
# indexing to finish before timing, so the query is a genuine indexed HNSW search, not an accidental exact
# scan. Client is python3 STDLIB only (urllib+json, no pip).
# Emits: LANE=qdrant_ann STATUS=ok-recall RECALL_AT_K=0.NN QUERY_MS=.. LOAD_MS=.. K=.. MODE=hnsw
set -uo pipefail; here="$(cd "$(dirname "$0")"&&pwd)"; . "$here/_common.sh"

BASE_CSV="${VEC_BASE_CSV:?}"; QUERY_CSV="${VEC_QUERY_CSV:?}"; META="${VEC_META:?}"
K="${VEC_K:-10}"
REF_ANSWER="${VEC_REF_ANSWER:-${BASE_CSV%.base.csv}.answer.txt}"
HNSW_M="${QDRANT_HNSW_M:-16}"; HNSW_EFC="${QDRANT_HNSW_EFC:-100}"; HNSW_EFS="${QDRANT_HNSW_EFS:-128}"
C="arena_qdrant_ann_$$"
IMG="${QDRANT_IMAGE:-qdrant/qdrant:v1.11.0}"
PORT="${QDRANT_ANN_PORT:-$(( 6800 + ($$ % 400) ))}"
COLL="${QDRANT_COLL:-arena_vec_ann}"

[ -f "$REF_ANSWER" ] || { echo "LANE=qdrant_ann STATUS=skip REASON=exact-reference-missing($REF_ANSWER; run gen_vectors.sh first)"; exit 0; }
command -v python3 >/dev/null 2>&1 || { echo "LANE=qdrant_ann STATUS=skip REASON=python3-stdlib-client-required(install python3)"; exit 0; }
st="$(arena_docker_status)"; [ "$st" = ok ] || { echo "LANE=qdrant_ann STATUS=skip REASON=docker-$st($(arena_docker_fix "$st"))"; exit 0; }
docker rm -f "$C" >/dev/null 2>&1
trap 'docker rm -f "$C" >/dev/null 2>&1 || true' EXIT INT TERM
if ! docker run -d --name "$C" --label "$ARENA_LABEL=1" -p "127.0.0.1:$PORT:6333" "$IMG" >/tmp/qdrant_ann_$$.err 2>&1; then
  echo "LANE=qdrant_ann STATUS=skip REASON=image-start-failed([$(tr -d '\n' </tmp/qdrant_ann_$$.err|tail -c 80)])"; rm -f /tmp/qdrant_ann_$$.err; exit 0
fi
rm -f /tmp/qdrant_ann_$$.err
alive(){ docker ps -q --no-trunc | grep -q "$(docker inspect -f '{{.Id}}' "$C" 2>/dev/null)"; }
URL="http://127.0.0.1:$PORT"
ready=0
for i in $(seq 1 60); do
  if curl -fsS "$URL/healthz" >/dev/null 2>&1 || curl -fsS "$URL/" >/dev/null 2>&1; then ready=1; break; fi
  alive || break; sleep 1
done
[ "$ready" = 1 ] || { echo "LANE=qdrant_ann STATUS=dnf REASON=engine-not-ready(rest-port-$PORT-never-answered)"; exit 0; }

OUT="$(QDRANT_URL="$URL" QDRANT_COLL="$COLL" VEC_K="$K" REF_ANSWER="$REF_ANSWER" \
       HNSW_M="$HNSW_M" HNSW_EFC="$HNSW_EFC" HNSW_EFS="$HNSW_EFS" \
       python3 - "$BASE_CSV" "$QUERY_CSV" "$META" <<'PY' 2>&1
import os, sys, time, json, urllib.request
URL = os.environ["QDRANT_URL"]; COLL = os.environ["QDRANT_COLL"]
ref_p = os.environ["REF_ANSWER"]
M = int(os.environ.get("HNSW_M", "16")); EFC = int(os.environ.get("HNSW_EFC", "100")); EFS = int(os.environ.get("HNSW_EFS", "128"))
base_p, query_p, meta_p = sys.argv[1], sys.argv[2], sys.argv[3]
meta = {}
for tok in open(meta_p).read().split():
    if "=" in tok:
        k, v = tok.split("=", 1); meta[k] = v
D = int(meta["D"]); K = int(meta.get("K", 10))
def req(method, path, body=None, timeout=300):
    data = json.dumps(body).encode() if body is not None else None
    r = urllib.request.Request(URL + path, data=data, method=method,
                               headers={"Content-Type": "application/json"})
    with urllib.request.urlopen(r, timeout=timeout) as resp:
        return json.loads(resp.read().decode())
def load(path):
    ids, vecs = [], []
    with open(path) as f:
        for ln in f:
            ln = ln.strip()
            if not ln: continue
            p = ln.split(",")
            ids.append(int(p[0])); vecs.append([float(x) for x in p[1:1+D]])
    return ids, vecs
bid, base = load(base_p)
qid, query = load(query_p)
N = len(bid)
# fresh collection with an explicit HNSW config, and indexing_threshold lowered so the HNSW index is
# actually built for this modest N (Euclid == L2). delete-then-create is idempotent.
try: req("DELETE", f"/collections/{COLL}")
except Exception: pass
req("PUT", f"/collections/{COLL}", {
    "vectors": {"size": D, "distance": "Euclid"},
    "hnsw_config": {"m": M, "ef_construct": EFC},
    "optimizers_config": {"indexing_threshold": 100},
})
t0 = time.perf_counter()
B = 2000
for i in range(0, len(bid), B):
    pts = [{"id": bid[j], "vector": base[j]} for j in range(i, min(i + B, len(bid)))]
    req("PUT", f"/collections/{COLL}/points?wait=true", {"points": pts})
# WAIT for the HNSW index to finish building so the query is a genuine indexed search (not an exact scan
# that happens because the index was not ready). Poll until green + all vectors indexed, up to ~180s.
indexed = 0
for _ in range(180):
    info = req("GET", f"/collections/{COLL}")["result"]
    indexed = info.get("indexed_vectors_count", 0) or 0
    if info.get("status") == "green" and indexed >= N:
        break
    time.sleep(1)
load_ms = int((time.perf_counter() - t0) * 1000)
# APPROXIMATE HNSW batched top-K (params.exact=false + hnsw_ef). No over-fetch: this is the honest
# approximate answer Qdrant returns at the configured ef.
searches = [{"vector": query[i], "limit": K, "params": {"exact": False, "hnsw_ef": EFS}, "with_payload": False}
            for i in range(len(qid))]
t0 = time.perf_counter()
res = req("POST", f"/collections/{COLL}/points/search/batch", {"searches": searches})["result"]
query_ms = int((time.perf_counter() - t0) * 1000)
top = {}
for slot, qi in enumerate(qid):
    top[qi] = [int(h["id"]) for h in res[slot]]
# recall@K vs the independent exact L2 reference (set overlap). ids come ONLY from Qdrant's returned hits.
ref = [ln.split(",") for ln in open(ref_p).read().strip().split(";")]
order = sorted(range(len(qid)), key=lambda i: qid[i])
hit = tot = 0
for pos, i in enumerate(order):
    got = set(top[qid[i]])
    rset = set(int(x) for x in ref[pos])
    tot += len(rset); hit += len(got & rset)
recall = (hit / tot) if tot > 0 else 0.0
print(f"QDRANT_ANN_LOAD_MS={load_ms}")
print(f"QDRANT_ANN_QUERY_MS={query_ms}")
print(f"QDRANT_ANN_INDEXED={indexed}")
print(f"QDRANT_ANN_RECALL={recall:.4f}")
PY
)"
alive || { echo "LANE=qdrant_ann STATUS=dnf REASON=container-died-during-query"; exit 0; }
lm="$(sed -n 's/^QDRANT_ANN_LOAD_MS=\([0-9]*\).*/\1/p'  <<<"$OUT" | tail -1)"
qm="$(sed -n 's/^QDRANT_ANN_QUERY_MS=\([0-9]*\).*/\1/p' <<<"$OUT" | tail -1)"
rc="$(sed -n 's/^QDRANT_ANN_RECALL=\([0-9.]*\).*/\1/p'  <<<"$OUT" | tail -1)"
ix="$(sed -n 's/^QDRANT_ANN_INDEXED=\([0-9]*\).*/\1/p'  <<<"$OUT" | tail -1)"
if [ -z "$rc" ]; then
  echo "LANE=qdrant_ann STATUS=dnf REASON=no-result([$(printf '%s' "$OUT"|tr '\n' ' '|tail -c 120)])"; exit 0
fi
echo "LANE=qdrant_ann STATUS=ok-recall RECALL_AT_K=$rc QUERY_MS=${qm:-0} LOAD_MS=${lm:-0} K=$K MODE=hnsw(M=$HNSW_M,efc=$HNSW_EFC,efs=$HNSW_EFS,indexed=${ix:-0})"
