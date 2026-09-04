#!/usr/bin/env bash
# Qdrant VECTOR lane (Apache-2.0). Stand up the official qdrant/qdrant image, create a collection sized
# to the dataset (Euclid / L2), upsert the SAME deterministic base set, then run the SAME top-K query
# set in EXACT / FLAT mode (params.exact=true forces a full brute-force scan, bypassing the approximate
# HNSW index) so the neighbour ids are a genuine exact nearest-neighbour answer, gate-able against the
# independent reference. The client is python3 STDLIB only (urllib+json, no pip) talking to the
# container's published REST port.
#
# WHY THE EARLIER MISMATCH, AND WHY THIS IS NOT "APPROXIMATE" (investigated 2026-09-04, ref 2d1d6513):
# The dataset has MANY groups of exactly-equidistant base points (the components live on a fixed grid,
# so squared-L2 ties are common, not probability-zero). The canonical answer breaks such ties with a
# defensive (dist ASC, id ASC) rule -- the SAME rule the independent reference and the FAISS lane use.
# The previous driver asked Qdrant for exactly K hits, which TRUNCATES a group of tied points at the
# K-th boundary before the id-ASC tiebreak is even defined: Qdrant returned an equidistant alternate
# (a higher id at the identical distance), so the id list -- though a correct set of exact nearest
# neighbours -- hashed differently (recall was ~0.94, and every "miss" was an exact-distance tie, not a
# wrong neighbour). The FIX is NOT to relabel a wrong answer as a win, and NOT to switch to approximate
# HNSW: it is to OVER-FETCH past K in exact mode and apply the canonical (dist ASC, id ASC) tiebreak
# across the FULL boundary tie group. That yields Qdrant's genuine exact top-K -- an exact answer, so it
# gates equal-answer against the reference honestly. The lane ALSO reports the measured recall@K vs the
# reference (RECALL_AT_K) as the honest vector-search-quality metric; ids come ONLY from Qdrant's
# returned hits (never fabricated), so if Qdrant's exact answer ever genuinely disagreed the hash would
# simply not match -- a mismatch is never papered over.
# Emits: LANE=qdrant STATUS=ok LOAD_MS=.. QUERY_MS=.. ANSWER_HASH=.. RECALL_AT_K=.. MODE=exact-flat
set -uo pipefail; here="$(cd "$(dirname "$0")"&&pwd)"; . "$here/_common.sh"

BASE_CSV="${VEC_BASE_CSV:?}"; QUERY_CSV="${VEC_QUERY_CSV:?}"; META="${VEC_META:?}"
K="${VEC_K:-10}"
C="arena_qdrant_$$"
IMG="${QDRANT_IMAGE:-qdrant/qdrant:v1.11.0}"
PORT="${QDRANT_PORT:-$(( 6400 + ($$ % 400) ))}"     # published on 127.0.0.1:$PORT -> container 6333
COLL="${QDRANT_COLL:-arena_vec}"
# the independent exact-KNN reference sits beside the base csv ("<base>.answer.txt"); used ONLY to
# report recall@K (a diagnostic). The gate itself is the answer hash Qdrant's exact search produces.
REF_ANSWER="${VEC_REF_ANSWER:-${BASE_CSV%.base.csv}.answer.txt}"

command -v python3 >/dev/null 2>&1 || { echo "LANE=qdrant STATUS=skip REASON=python3-stdlib-client-required(install python3)"; exit 0; }
st="$(arena_docker_status)"; [ "$st" = ok ] || { echo "LANE=qdrant STATUS=skip REASON=docker-$st($(arena_docker_fix "$st"))"; exit 0; }
docker rm -f "$C" >/dev/null 2>&1
trap 'docker rm -f "$C" >/dev/null 2>&1 || true' EXIT INT TERM
if ! docker run -d --name "$C" --label "$ARENA_LABEL=1" -p "127.0.0.1:$PORT:6333" "$IMG" >/tmp/qdrant_$$.err 2>&1; then
  echo "LANE=qdrant STATUS=skip REASON=image-start-failed([$(tr -d '\n' </tmp/qdrant_$$.err|tail -c 80)])"; rm -f /tmp/qdrant_$$.err; exit 0
fi
rm -f /tmp/qdrant_$$.err
alive(){ docker ps -q --no-trunc | grep -q "$(docker inspect -f '{{.Id}}' "$C" 2>/dev/null)"; }
URL="http://127.0.0.1:$PORT"
ready=0
for i in $(seq 1 60); do
  if curl -fsS "$URL/healthz" >/dev/null 2>&1 || curl -fsS "$URL/" >/dev/null 2>&1; then ready=1; break; fi
  alive || break; sleep 1
done
[ "$ready" = 1 ] || { echo "LANE=qdrant STATUS=dnf REASON=engine-not-ready(rest-port-$PORT-never-answered)"; exit 0; }

# python3 STDLIB driver: create collection, batch-upsert base, EXACT top-K search with a boundary-safe
# over-fetch + canonical (dist ASC, id ASC) tiebreak, emit the canonical answer + measured recall@K.
OUT="$(QDRANT_URL="$URL" QDRANT_COLL="$COLL" VEC_K="$K" REF_ANSWER="$REF_ANSWER" \
       python3 - "$BASE_CSV" "$QUERY_CSV" "$META" <<'PY' 2>&1
import os, sys, time, json, urllib.request
URL = os.environ["QDRANT_URL"]; COLL = os.environ["QDRANT_COLL"]
ref_p = os.environ.get("REF_ANSWER", "")
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
# fresh collection (Euclid == L2; sqrt is monotonic so the ranking AND the exact ties are identical to
# squared-L2). delete-then-create is idempotent.
try: req("DELETE", f"/collections/{COLL}")
except Exception: pass
req("PUT", f"/collections/{COLL}", {"vectors": {"size": D, "distance": "Euclid"}})
t0 = time.perf_counter()
B = 2000
for i in range(0, len(bid), B):
    pts = [{"id": bid[j], "vector": base[j]} for j in range(i, min(i + B, len(bid)))]
    req("PUT", f"/collections/{COLL}/points?wait=true", {"points": pts})
load_ms = int((time.perf_counter() - t0) * 1000)
# EXACT/FLAT batched top-K (params.exact=true => full scan, no HNSW approximation), boundary-safe.
# Per query we over-fetch and expand the window until the K-th distance is STRICTLY bracketed (a
# returned hit is farther than the K-th), which guarantees every point at the boundary distance was
# returned, so the (dist ASC, id ASC) tiebreak is well defined. fetch grows to N in the worst case
# (full scan) => always terminates with the genuine exact top-K.
top = [None] * len(qid)
pending = list(range(len(qid)))
fetch = min(N, max(4 * K, K + 32))
query_ms = 0
while pending:
    searches = [{"vector": query[i], "limit": fetch, "params": {"exact": True}, "with_payload": False}
                for i in pending]
    t0 = time.perf_counter()
    res = req("POST", f"/collections/{COLL}/points/search/batch", {"searches": searches})["result"]
    query_ms += int((time.perf_counter() - t0) * 1000)
    still = []
    for slot, qi in enumerate(pending):
        hits = [(h["score"], int(h["id"])) for h in res[slot]]
        hits.sort(key=lambda t: (t[0], t[1]))           # (dist ASC, id ASC) -- the canonical tiebreak
        if len(hits) < K:
            top[qi] = [h[1] for h in hits[:K]]           # fewer than K points exist in the collection
        elif len(hits) < fetch or fetch >= N or hits[-1][0] > hits[K-1][0]:
            top[qi] = [h[1] for h in hits[:K]]           # boundary strictly bracketed -> exact top-K
        else:
            still.append(qi)                             # tie may extend past the window; fetch wider
    pending = still
    if pending:
        fetch = min(N, fetch * 4)
order = sorted(range(len(qid)), key=lambda i: qid[i])    # qid asc (already 0..Q-1)
lines = [",".join(str(x) for x in top[i]) for i in order]
print(f"QDRANT_LOAD_MS={load_ms}")
print(f"QDRANT_QUERY_MS={query_ms}")
print("QDRANT_ANSWER=" + ";".join(lines))
# recall@K vs the independent exact reference (diagnostic; honest vector-search quality). Compared per
# query on the neighbour-id SET; the answer above stays the gate value regardless.
if ref_p and os.path.exists(ref_p):
    ref = [ln.split(",") for ln in open(ref_p).read().strip().split(";")]
    if len(ref) == len(qid):
        ov = tot = 0
        for i in range(len(qid)):
            rset = set(ref[i]); tot += len(rset)
            ov += len(rset & set(str(x) for x in top[i]))
        if tot > 0:
            print("QDRANT_RECALL=%.4f" % (ov / tot))
PY
)"
alive || { echo "LANE=qdrant STATUS=dnf REASON=container-died-during-query"; exit 0; }
lm="$(sed -n 's/^QDRANT_LOAD_MS=\([0-9]*\).*/\1/p'  <<<"$OUT" | tail -1)"
qm="$(sed -n 's/^QDRANT_QUERY_MS=\([0-9]*\).*/\1/p' <<<"$OUT" | tail -1)"
ans="$(sed -n 's/^QDRANT_ANSWER=\(.*\)$/\1/p'       <<<"$OUT" | tail -1)"
rc="$(sed -n 's/^QDRANT_RECALL=\([0-9.]*\).*/\1/p'  <<<"$OUT" | tail -1)"
if [ -z "$ans" ]; then
  echo "LANE=qdrant STATUS=dnf REASON=no-result([$(printf '%s' "$OUT"|tr '\n' ' '|tail -c 120)])"; exit 0
fi
echo "LANE=qdrant STATUS=ok LOAD_MS=${lm:-0} QUERY_MS=${qm:-0} ANSWER_HASH=$(hash_answer "$ans") RECALL_AT_K=${rc:-na} MODE=exact-flat"
