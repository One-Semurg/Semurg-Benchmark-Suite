#!/usr/bin/env bash
# OpenSearch SEARCH lane (board domain #10, Apache-2.0 -- public-OK, NOT a DeWitt engine). Stands up the
# official opensearchproject/opensearch image, builds the SAME deterministic full-text document corpus the
# semurg_search reference builds (identical LCG + knobs => byte-identical corpus on any box), indexes it
# across number_of_shards = all cores (max concurrency on the incumbent's node) striped over BOTH NVMe data
# disks, then runs the domain's four canonical SEARCH operations three cycles (cold -> warm -> warm) through
# the OpenSearch REST API and GATES the engine's answer against an INDEPENDENT awk reference before it
# reports a single number.
#
# The four canonical operations (identical to every lane in this domain -- see lanes/semurg_search.sh):
#   S1  full-text term match  -> COUNT of docs whose topic field matches the term (a `match` full-text query
#                                on the analyzed topic field). The vocabulary is chosen so token-match ==
#                                byte-substring-match, so this COUNT identifies the SAME doc-id set on every
#                                engine over this deterministic corpus.
#   S2  exact term filter     -> COUNT of docs with category == CAT   (a `term` query on the integer field).
#   S3  terms aggregation     -> per-category doc COUNTS [c0,c1,c2]   (a `terms` aggregation, key asc).
#   S4  numeric range filter  -> COUNT of docs with priority >= PRI   (a `range` query on the integer field).
# BM25 relevance RANKING is the incumbent's home turf and is a SEPARATE, non-gated metric, never claimed here.
#
# EQUAL-ANSWER (the gate). The canonical answer string "S1=..;S2=..;S3=c0,c1,c2;S4=.." is hashed with the
# domain rule (hash_answer = first 32 hex of sha256, no trailing newline). This lane computes that reference
# INDEPENDENTLY with an engine-agnostic awk pass over the corpus, then requires OpenSearch to reproduce it
# EXACTLY -- a disagreement emits STATUS=dnf, never a green number. Because the corpus + ops are the same as
# the semurg_search reference, the hash is the SAME 32-hex value (default knobs => 30bac289...), so under
# run_all_domains.sh (which gates search first-ok-lane) this lane renders ok-matched against semurg_search.
#
# Emits ONE line run_all_domains.sh parses (LOAD_MS + QUERY_MS + ANSWER_HASH are the parsed fields; the rest
# is honest evidence a third party can read):
#   LANE=opensearch STATUS=ok LOAD_MS=<int> QUERY_MS=<int> COLD_MS=<f> WARM_MS=<f> WARM2_MS=<f> \
#     S1=.. S2=.. S3=c0,c1,c2 S4=.. DOCS=<int> SHARDS=<int> DISKS=<n> ANSWER_HASH=<32hex>
# QUERY_MS is the WARM2 (steady-state) query-set wall; COLD/WARM/WARM2 show the cache warming. If docker or
# python3 is unavailable, or the image will not start, the lane emits a clean SKIP with the exact fix
# (never crash, never a fake number).
set -uo pipefail
here="$(cd "$(dirname "$0")" && pwd)"; . "$here/_common.sh"

# ---- scratch (run_all_domains.sh passes ARENA_DATA; else self-contained temp) ----
SCRATCH="${ARENA_DATA:-${SEARCH_SCRATCH:-${TMPDIR:-/tmp}/opensearch_search_$$}}"
mkdir -p "$SCRATCH" 2>/dev/null || { echo "LANE=opensearch STATUS=skip REASON=scratch-unwritable($SCRATCH)"; exit 0; }

# ---- deterministic corpus knobs (identical knobs => byte-identical corpus vs semurg_search) ----
DOCS="${SEARCH_DOCS:-200000}"       # corpus size (docs)
SEED="${SEARCH_SEED:-1234567}"      # LCG seed (determinism)
NCAT="${SEARCH_NCAT:-3}"            # category cardinality 0..NCAT-1
S1_TERM="${SEARCH_S1_TERM:-foxtrot}"
S2_CAT="${SEARCH_S2_CAT:-1}"
S4_PRI="${SEARCH_S4_PRI:-4}"        # priority range: >= this (priority is 1..5)
# 12 distinct words, NONE a substring of another, so a token match and a raw byte-substring match count the
# SAME docs -> the boolean full-text answer is bit-exact across engines.
VOCAB="alpha bravo charlie delta echo foxtrot golf hotel india juliet kilo lima"

# ---- preflight: python3 stdlib client + docker daemon (clean SKIP with the exact fix otherwise) ----
command -v python3 >/dev/null 2>&1 || { echo "LANE=opensearch STATUS=skip REASON=python3-stdlib-client-required(install python3)"; exit 0; }
st="$(arena_docker_status)"; [ "$st" = ok ] || { echo "LANE=opensearch STATUS=skip REASON=docker-$st($(arena_docker_fix "$st"))"; exit 0; }

corpus="$SCRATCH/search.corpus.tsv"

# ---- 1. corpus: one LCG stream; each draw uses HIGH bits (drops the 17 poor low bits of a power-of-2 LCG)
#         so topic/body/category/priority are uniform + well-spread. body's primary token is `topic`. ----
awk -v n="$DOCS" -v seed="$SEED" -v ncat="$NCAT" -v vocab="$VOCAB" 'BEGIN{
  m=split(vocab, W, " "); s=seed; OFS="\t";
  for(i=1;i<=n;i++){
    s=(s*1103515245+12345)%2147483648; ti=1+int(s/131072)%m;
    s=(s*1103515245+12345)%2147483648; a=1+int(s/131072)%m;
    s=(s*1103515245+12345)%2147483648; b=1+int(s/131072)%m;
    s=(s*1103515245+12345)%2147483648; cat=int(s/131072)%ncat;
    s=(s*1103515245+12345)%2147483648; pri=1+int(s/131072)%5;
    body="the " W[ti] " subsystem streams through the " W[a] " " W[b] " tier";
    print i, W[ti], body, cat, pri;
  }
}' > "$corpus" 2>/dev/null || { echo "LANE=opensearch STATUS=skip REASON=corpus-gen-failed"; [ -z "${ARENA_DATA:-}" ] && rm -rf "$SCRATCH" 2>/dev/null; exit 0; }

# ---- 2. INDEPENDENT reference answers -- a single O(N) awk pass over the corpus text (no engine). ----
read -r R_S1 R_S2 R_S3 R_S4 < <(awk -v term="$S1_TERM" -v c="$S2_CAT" -v ncat="$NCAT" -v p="$S4_PRI" '
  BEGIN{ FS="\t"; s1=0; s4=0; for(k=0;k<ncat;k++) g[k]=0 }
  { topic=$2; cat=$4+0; pri=$5+0;
    if(topic==term) s1++;
    g[cat]++;
    if(pri>=p) s4++;
  }
  END{
    s2=g[c];
    grp=""; for(k=0;k<ncat;k++){ grp=(k==0?g[k]:grp","g[k]) }
    print s1, s2, grp, s4;
  }' "$corpus")
REF_ANS="S1=${R_S1};S2=${R_S2};S3=${R_S3};S4=${R_S4}"
REF_HASH="$(hash_answer "$REF_ANS")"

# ---- 3. bring up the OpenSearch container (security disabled = plain http, single-node, no bootstrap
#         checks). number_of_shards = all cores (query + index fan across every core = max concurrency on
#         the incumbent's node). path.data striped across BOTH NVMe data disks where writable (fair use of
#         both pipes), falling back to scratch then to the container's own volume. ----
NPROC="$(nproc 2>/dev/null || echo 4)"
IMG="${OPENSEARCH_IMAGE:-opensearchproject/opensearch:2.17.0}"
HEAP="${OPENSEARCH_HEAP:-2g}"
PORT="${OPENSEARCH_PORT:-$(( 9600 + ($$ % 300) ))}"   # published on 127.0.0.1:$PORT -> container 9200
IDX="${OPENSEARCH_INDEX:-arena_search}"
C="arena_opensearch_$$"

# best-effort two-disk striping for path.data; each host dir chmod 777 so the container uid (1000) can write
mounts=(); pdata=(); ddirs=(); ndisk=0
for cand in /data0 /data1; do
  hd="$cand/arena_opensearch_${$}_$ndisk"
  if mkdir -p "$hd" 2>/dev/null && chmod 777 "$hd" 2>/dev/null; then
    mounts+=(-v "$hd:/mnt/osdata$ndisk"); pdata+=("/mnt/osdata$ndisk"); ddirs+=("$hd"); ndisk=$((ndisk+1))
  fi
done
if [ "$ndisk" = 0 ]; then                              # no data disk writable -> stripe under scratch
  hd="$SCRATCH/osdata0"
  if mkdir -p "$hd" 2>/dev/null && chmod 777 "$hd" 2>/dev/null; then
    mounts+=(-v "$hd:/mnt/osdata0"); pdata+=("/mnt/osdata0"); ddirs+=("$hd"); ndisk=1
  fi
fi
pdcsv="$(IFS=,; echo "${pdata[*]:-}")"
pdenv=(); [ -n "$pdcsv" ] && pdenv=(-e "path.data=$pdcsv")

docker rm -f "$C" >/dev/null 2>&1
cleanup(){ docker rm -f "$C" >/dev/null 2>&1 || true; local d; for d in "${ddirs[@]:-}"; do [ -n "$d" ] && rm -rf "$d" 2>/dev/null; done; [ -z "${ARENA_DATA:-}" ] && rm -rf "$SCRATCH" 2>/dev/null; }
trap cleanup EXIT INT TERM

if ! docker run -d --name "$C" --label "$ARENA_LABEL=1" \
      -p "127.0.0.1:$PORT:9200" \
      -e "discovery.type=single-node" \
      -e "DISABLE_SECURITY_PLUGIN=true" \
      -e "DISABLE_INSTALL_DEMO_CONFIG=true" \
      -e "bootstrap.memory_lock=false" \
      -e "OPENSEARCH_JAVA_OPTS=-Xms$HEAP -Xmx$HEAP" \
      -e "cluster.routing.allocation.disk.threshold_enabled=false" \
      -e "logger.level=WARN" \
      --ulimit nofile=65536:65536 --ulimit memlock=-1:-1 \
      "${pdenv[@]}" "${mounts[@]}" \
      "$IMG" >/tmp/opensearch_$$.err 2>&1; then
  echo "LANE=opensearch STATUS=skip REASON=image-start-failed([$(tr -d '\n' </tmp/opensearch_$$.err | tail -c 90)])"
  rm -f /tmp/opensearch_$$.err; exit 0
fi
rm -f /tmp/opensearch_$$.err

alive(){ docker ps -q --no-trunc | grep -q "$(docker inspect -f '{{.Id}}' "$C" 2>/dev/null)"; }
URL="http://127.0.0.1:$PORT"
ready=0
for i in $(seq 1 150); do
  if curl -fsS "$URL/_cluster/health" 2>/dev/null | grep -qE '"status":"(green|yellow)"'; then ready=1; break; fi
  alive || break; sleep 1
done
if [ "$ready" != 1 ]; then
  if ! alive; then
    echo "LANE=opensearch STATUS=skip REASON=container-exited-on-start([$(docker logs "$C" 2>&1 | tr -d '\n' | tail -c 100)])"
  else
    echo "LANE=opensearch STATUS=dnf REASON=engine-not-ready(rest-port-$PORT-never-green-yellow)"
  fi
  exit 0
fi

# ---- 4. python3 STDLIB driver: create the index (shards = cores, replicas 0), CONCURRENT bulk index the
#         corpus (all cores busy), refresh + force-merge to a settled index, then run the 4 ops three
#         cycles concurrently (max search fan) and print the counts + timings. ----
OUT="$( OS_URL="$URL" OS_IDX="$IDX" OS_SHARDS="$NPROC" \
        OS_TERM="$S1_TERM" OS_CAT="$S2_CAT" OS_PRI="$S4_PRI" OS_NCAT="$NCAT" OS_DOCS="$DOCS" \
        python3 - "$corpus" <<'PY' 2>&1
import os, sys, time, json, urllib.request
from concurrent.futures import ThreadPoolExecutor

URL   = os.environ["OS_URL"]
IDX   = os.environ["OS_IDX"]
SH    = int(os.environ["OS_SHARDS"])
TERM  = os.environ["OS_TERM"]
CAT   = int(os.environ["OS_CAT"])
PRI   = int(os.environ["OS_PRI"])
NCAT  = int(os.environ["OS_NCAT"])
NDOCS = int(os.environ["OS_DOCS"])
corpus = sys.argv[1]

def req(method, path, body=None, ndjson=False, timeout=600):
    if ndjson:
        data = body.encode(); ctype = "application/x-ndjson"
    elif body is not None:
        data = json.dumps(body).encode(); ctype = "application/json"
    else:
        data = None; ctype = "application/json"
    r = urllib.request.Request(URL + path, data=data, method=method, headers={"Content-Type": ctype})
    with urllib.request.urlopen(r, timeout=timeout) as resp:
        return json.loads(resp.read().decode())

# fresh index: shards = cores (spread segments so index + search fan across every core), 0 replicas
# (single node), refresh disabled during load. topic is analyzed text (the full-text match field);
# category/priority are integers (exact term + range + terms-agg).
try: req("DELETE", "/" + IDX)
except Exception: pass
req("PUT", "/" + IDX, {
    "settings": {"index": {"number_of_shards": SH, "number_of_replicas": 0, "refresh_interval": "-1"}},
    "mappings": {"properties": {
        "topic":    {"type": "text"},
        "body":     {"type": "text"},
        "category": {"type": "integer"},
        "priority": {"type": "integer"},
    }},
})

# build bulk chunks (ndjson). _id = corpus id so a re-run is idempotent and the doc set is exact.
def chunks():
    buf = []; ck = []
    with open(corpus) as f:
        for ln in f:
            ln = ln.rstrip("\n")
            if not ln: continue
            did, topic, body, cat, pri = ln.split("\t")
            buf.append('{"index":{"_id":"%s"}}' % did)
            buf.append(json.dumps({"topic": topic, "body": body,
                                   "category": int(cat), "priority": int(pri)}))
            if len(buf) >= 10000:            # 5000 docs per bulk request
                ck.append("\n".join(buf) + "\n"); buf = []
    if buf: ck.append("\n".join(buf) + "\n")
    return ck

def send_bulk(payload):
    r = req("POST", "/%s/_bulk" % IDX, payload, ndjson=True)
    if r.get("errors"):
        # surface the first item error so a silent drop can never masquerade as a green number
        for it in r.get("items", []):
            act = next(iter(it.values()))
            if act.get("error"):
                raise RuntimeError("bulk-item-error:" + json.dumps(act["error"])[:120])
    return len(r.get("items", []))

ck = chunks()
t0 = time.perf_counter()
# CONCURRENT bulk from a worker pool = the server's indexing thread pool (all cores) stays saturated.
with ThreadPoolExecutor(max_workers=min(SH, 16)) as ex:
    list(ex.map(send_bulk, ck))
req("POST", "/%s/_refresh" % IDX)
load_ms = int((time.perf_counter() - t0) * 1000)

# verify the exact doc count landed (a drop would make the counts wrong -> caught, never faked)
cnt = req("GET", "/%s/_count" % IDX).get("count", -1)
if cnt != NDOCS:
    print("OS_ERROR=doc-count-mismatch(indexed=%s expected=%s)" % (cnt, NDOCS)); sys.exit(0)

# settle the index (force-merge to one segment per shard) so the warm cycles reflect steady state
try: req("POST", "/%s/_forcemerge?max_num_segments=1" % IDX, timeout=600)
except Exception: pass

# the four canonical ops as REST requests (each fans across all shards = all cores).
def op_s1():   # full-text term match on the analyzed topic field
    return "s1", req("POST", "/%s/_count" % IDX, {"query": {"match": {"topic": TERM}}})["count"]
def op_s2():   # exact term filter on category
    return "s2", req("POST", "/%s/_count" % IDX, {"query": {"term": {"category": CAT}}})["count"]
def op_s3():   # terms aggregation on category, key ascending -> [c0,c1,c2]
    r = req("POST", "/%s/_search" % IDX, {"size": 0, "aggs": {"by_cat": {"terms":
             {"field": "category", "size": NCAT, "order": {"_key": "asc"}}}}})
    m = {int(b["key"]): b["doc_count"] for b in r["aggregations"]["by_cat"]["buckets"]}
    return "s3", [m.get(k, 0) for k in range(NCAT)]
def op_s4():   # numeric range filter priority >= PRI
    return "s4", req("POST", "/%s/_count" % IDX, {"query": {"range": {"priority": {"gte": PRI}}}})["count"]

def cycle():
    res = {}
    t = time.perf_counter()
    with ThreadPoolExecutor(max_workers=4) as ex:       # 4 ops concurrent = max search fan
        for name, val in ex.map(lambda f: f(), [op_s1, op_s2, op_s3, op_s4]):
            res[name] = val
    return (time.perf_counter() - t) * 1000.0, res

c1_ms, r1 = cycle()
c2_ms, _  = cycle()
c3_ms, r3 = cycle()

s1 = r3["s1"]; s2 = r3["s2"]; s3 = r3["s3"]; s4 = r3["s4"]
answer = "S1=%d;S2=%d;S3=%s;S4=%d" % (s1, s2, ",".join(str(x) for x in s3), s4)
print("OS_LOAD_MS=%d" % load_ms)
print("OS_C1_MS=%.3f" % c1_ms)
print("OS_C2_MS=%.3f" % c2_ms)
print("OS_C3_MS=%.3f" % c3_ms)
print("OS_DOCS=%d" % cnt)
print("OS_SHARDS=%d" % SH)
print("OS_S1=%d OS_S2=%d OS_S3=%s OS_S4=%d" % (s1, s2, ",".join(str(x) for x in s3), s4))
print("OS_ANSWER=" + answer)
PY
)"

alive || { echo "LANE=opensearch STATUS=dnf REASON=container-died-during-run"; exit 0; }

oserr="$(sed -n 's/^OS_ERROR=\(.*\)$/\1/p' <<<"$OUT" | tail -1)"
if [ -n "$oserr" ]; then
  echo "LANE=opensearch STATUS=dnf REASON=$oserr"; exit 0
fi
lm="$(sed -n 's/^OS_LOAD_MS=\([0-9]*\).*/\1/p'  <<<"$OUT" | tail -1)"
c1="$(sed -n 's/^OS_C1_MS=\([0-9.]*\).*/\1/p'   <<<"$OUT" | tail -1)"
c2="$(sed -n 's/^OS_C2_MS=\([0-9.]*\).*/\1/p'   <<<"$OUT" | tail -1)"
c3="$(sed -n 's/^OS_C3_MS=\([0-9.]*\).*/\1/p'   <<<"$OUT" | tail -1)"
docs="$(sed -n 's/^OS_DOCS=\([0-9]*\).*/\1/p'   <<<"$OUT" | tail -1)"
shards="$(sed -n 's/^OS_SHARDS=\([0-9]*\).*/\1/p' <<<"$OUT" | tail -1)"
ans="$(sed -n 's/^OS_ANSWER=\(.*\)$/\1/p'       <<<"$OUT" | tail -1)"
s1v="$(sed -n 's/.*OS_S1=\([0-9]*\).*/\1/p'     <<<"$OUT" | tail -1)"
s2v="$(sed -n 's/.*OS_S2=\([0-9]*\).*/\1/p'     <<<"$OUT" | tail -1)"
s3v="$(sed -n 's/.*OS_S3=\([0-9,]*\).*/\1/p'    <<<"$OUT" | tail -1)"
s4v="$(sed -n 's/.*OS_S4=\([0-9]*\).*/\1/p'     <<<"$OUT" | tail -1)"

if [ -z "$ans" ]; then
  echo "LANE=opensearch STATUS=dnf REASON=no-result([$(printf '%s' "$OUT" | tr '\n' ' ' | tail -c 130)])"; exit 0
fi

# ---- 5. EQUAL-ANSWER GATE: OpenSearch must reproduce the INDEPENDENT awk reference exactly ----
ENG_HASH="$(hash_answer "$ans")"
if [ "$ENG_HASH" != "$REF_HASH" ]; then
  echo "LANE=opensearch STATUS=dnf REASON=equal-answer-mismatch(engine[$ans]!=ref[$REF_ANS])"; exit 0
fi

# QUERY_MS = the warm2 (steady-state) query-set wall, integer ms; COLD/WARM/WARM2 show the cache warming.
qms="$(awk -v v="${c3:-0}" 'BEGIN{printf "%.0f", v}')"
echo "LANE=opensearch STATUS=ok LOAD_MS=${lm:-0} QUERY_MS=${qms:-0} COLD_MS=${c1:-0} WARM_MS=${c2:-0} WARM2_MS=${c3:-0} S1=${s1v} S2=${s2v} S3=${s3v} S4=${s4v} DOCS=${docs:-0} SHARDS=${shards:-0} DISKS=${ndisk} ANSWER_HASH=${ENG_HASH}"
