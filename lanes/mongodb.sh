#!/usr/bin/env bash
# MongoDB DOCUMENT lane (domain 8; mongo:7, SSPL self-host-ok -- public suite, NOT a DeWitt engine):
# stand up mongo:7 in Docker, ingest a deterministic JSON document collection, run the three canonical
# JSON document operations under 16 CONCURRENT CLIENTS, and gate EQUAL-ANSWER against an INDEPENDENT
# awk reference over the same generated data (NOT any engine):
#   Q1  point-lookup by _id (projected to amount_cents)             -> findOne({_id}, {amount_cents:1})
#   Q2  group-by aggregation (count per category)                   -> $group by category_id
#   Q3  nested-field filter count (shipping.region_id == R)         -> countDocuments({"shipping.region_id":R})
# The document generator is the IDENTICAL deterministic LCG the semurg_doc lane uses (same gawk program,
# same defaults), so the per-record values are bit-identical and the answers match BY CONSTRUCTION. A
# number is emitted ONLY when Mongo's Q1|Q2|Q3 answer hash matches the independent awk reference; on any
# mismatch or engine error the lane reports STATUS=dnf (honest), and if Docker/the image is unavailable
# it reports a clean STATUS=skip. It NEVER fakes a number and NEVER crashes the board.
#
# FAIRNESS (drive the incumbent at max concurrency on its node): the collection is ingested with
# --numInsertionWorkers = client count (all cores), and every query phase is driven by DOC_MONGO_CLIENTS
# (default 16) concurrent mongosh clients hammering the single mongod -> all 16 cores engaged. The
# reported Q*_MS is the per-operation wall latency each client sees under that concurrent load (the
# slowest client's loop wall / iterations); throughput (ops/s) is printed to stderr for transparency.
#
# Emits ONE machine-readable line the board parses (same schema as semurg_doc / the SQL/TS lanes):
#   LANE=mongodb STATUS=ok LOAD_MS=.. Q1_MS=.. Q2_MS=.. Q3_MS=.. ANSWER_HASH=<32hex>
#
# NOTE: no `set -e` -- a lane must NEVER abort the whole board; it isolates its own failure into one line.
set -uo pipefail
here="$(cd "$(dirname "$0")" && pwd)"; . "$here/_common.sh"

# The board runs this lane inside `while read ... done < <(plan_lines)`, so our inherited fd0 is the
# plan stream. Reroute our own (and every child's) stdin to /dev/null so no docker exec / mongosh can
# consume a plan line and truncate the board's per-domain iteration (the same hazard semurg_doc guards).
exec </dev/null

# ---- knobs: deterministic dataset -- IDENTICAL defaults to semurg_doc so the answer matches ----------
ROWS="${DOC_ROWS:-2000000}"
CATS="${DOC_CATEGORIES:-32}"
REGS="${DOC_REGIONS:-16}"
STATS="${DOC_STATUSES:-8}"
TSTART="${DOC_TS_START:-1700000000}"
TSSPAN="${DOC_TS_SPAN:-31536000}"
AMTMOD="${DOC_AMOUNT_MOD:-1000000}"
CUSTMOD="${DOC_CUSTOMERS:-50000}"
NTAGS="${DOC_TAG_UNIVERSE:-48}"

# ---- knobs: MongoDB engine + concurrency ------------------------------------------------------------
IMG="${MONGO_IMAGE:-mongo:7}"
NCLIENTS="${DOC_MONGO_CLIENTS:-16}"      # 16 concurrent clients (fairness: all cores of its node)
CACHE_GB="${MONGO_CACHE_GB:-8}"          # WiredTiger cache cap (good citizen; CPU concurrency unaffected)
Q1_ITERS="${DOC_MONGO_Q1_ITERS:-500}"    # point-lookups per client
Q2_ITERS="${DOC_MONGO_Q2_ITERS:-3}"      # full group-by aggregations per client (heavy)
Q3_ITERS="${DOC_MONGO_Q3_ITERS:-20}"     # nested-field counts per client
C="arena_mongo_$$"

# ---- docker readiness (clean SKIP with an actionable fix; never a cryptic error) --------------------
st="$(arena_docker_status)"
[ "$st" = ok ] || { echo "LANE=mongodb STATUS=skip REASON=docker-$st($(arena_docker_fix "$st"))"; exit 0; }

# ---- scratch (throwaway; harness passes ARENA_DATA) ------------------------------------------------
BASE="${ARENA_DATA:-${TMPDIR:-/tmp}/arena_mongo_$$}"
WORK="$BASE/mongodb_work"
CSV="$WORK/docs.csv"; JSONL="$WORK/docs.jsonl"
mkdir -p "$WORK" 2>/dev/null || true
trap 'docker rm -f "$C" >/dev/null 2>&1 || true; rm -rf "$WORK" 2>/dev/null || true' EXIT INT TERM

# ---- (1) deterministic document collection (IDENTICAL LCG shape to semurg_doc's generator) ---------
# Cols: id,customer_id,category_id,region_id,status_id,amount_cents,ts,tags_bitmap  (all integers).
awk -v n="$ROWS" -v C="$CATS" -v R="$REGS" -v S="$STATS" -v tstart="$TSTART" -v tsspan="$TSSPAN" \
    -v amtmod="$AMTMOD" -v custmod="$CUSTMOD" -v ntags="$NTAGS" 'BEGIN{
  s=1234567;
  print "id,customer_id,category_id,region_id,status_id,amount_cents,ts,tags_bitmap";
  for(i=1;i<=n;i++){
    s=(s*1103515245+12345)%2147483648; cust = 1 + s%custmod;
    s=(s*1103515245+12345)%2147483648; cat  = int(s/256)%C;
    s=(s*1103515245+12345)%2147483648; reg  = int(s/256)%R;
    s=(s*1103515245+12345)%2147483648; st   = int(s/256)%S;
    s=(s*1103515245+12345)%2147483648; amt  = 1 + s%amtmod;
    s=(s*1103515245+12345)%2147483648; ts   = tstart + s%tsspan;
    s=(s*1103515245+12345)%2147483648; t1   = int(s/256)%ntags;
    s=(s*1103515245+12345)%2147483648; t2   = int(s/256)%ntags;
    s=(s*1103515245+12345)%2147483648; prio = int(s/256)%5;
    bm = lshift_or(t1, t2);
    printf "%d,%d,%d,%d,%d,%d,%d,%d\n", i,cust,cat,reg,st,amt,ts,bm;
  }
}
function pow2(b,  r){ r=1; while(b-->0) r*=2; return r; }
function lshift_or(a,b,  x,y){ x=pow2(a); y=pow2(b); if(a==b) return x; return x+y; }
' > "$CSV" 2>/dev/null
[ -s "$CSV" ] || { echo "LANE=mongodb STATUS=dnf REASON=dataset-generation-failed"; exit 0; }

# ---- (2) INDEPENDENT awk reference (ground truth; NOT the engine) -- identical derivation to semurg_doc
Q1ID="${DOC_Q1_ID:-$(( 1 + ROWS/3 ))}"; [ "$Q1ID" -gt "$ROWS" ] && Q1ID="$ROWS"
Q3REG="${DOC_Q3_REGION:-$(awk -F, 'NR>1{c[$4]++} END{best=-1;b=0; for(k in c){ if(c[k]>best||(c[k]==best&&k<b)){best=c[k];b=k} } print b}' "$CSV")}"
REF_Q1="$(awk -F, -v T="$Q1ID" 'NR>1 && $1==T{print $6; exit}' "$CSV")"
REF_Q2="$(awk -F, -v C="$CATS" 'NR>1{c[$3]++} END{for(i=0;i<C;i++) printf "%s%d",(i?",":""),c[i]+0}' "$CSV")"
REF_Q3="$(awk -F, -v R="$Q3REG" 'NR>1 && $4==R{n++} END{print n+0}' "$CSV")"
REF="$(hash_answer "$REF_Q1|$REF_Q2|$REF_Q3")"
echo "[mongodb] rows=$ROWS cats=$CATS regs=$REGS clients=$NCLIENTS (cores=$(nproc 2>/dev/null||echo ?))  independent awk reference:" >&2
echo "[mongodb]   Q1 _id=$Q1ID -> amount_cents=$REF_Q1 | Q2 per-category=[$REF_Q2] | Q3 shipping.region_id=$Q3REG -> $REF_Q3" >&2
echo "[mongodb]   reference ANSWER_HASH=$REF" >&2

# ---- (3) CSV -> JSONL with a NESTED shipping.region_id (a genuine document/nested-field query) ------
awk -F, 'NR>1{
  printf "{\"_id\":%s,\"customer_id\":%s,\"category_id\":%s,\"region_id\":%s,\"status_id\":%s,\"amount_cents\":%s,\"ts\":%s,\"tags_bitmap\":%s,\"shipping\":{\"region_id\":%s}}\n",
    $1,$2,$3,$4,$5,$6,$7,$8,$4
}' "$CSV" > "$JSONL" 2>/dev/null
[ -s "$JSONL" ] || { echo "LANE=mongodb STATUS=dnf REASON=jsonl-conversion-failed"; exit 0; }

# ---- (4) start mongod (its own container; other containers on the box are never touched) ------------
docker rm -f "$C" >/dev/null 2>&1
if ! docker run -d --rm --label "$ARENA_LABEL=1" --name "$C" "$IMG" \
      --wiredTigerCacheSizeGB "$CACHE_GB" >/tmp/arena_mongo_$$.err 2>&1; then
  echo "LANE=mongodb STATUS=skip REASON=image-pull-or-start-failed([$(tr -d '\n' </tmp/arena_mongo_$$.err|tail -c 90)])(fix:docker pull $IMG)"
  rm -f /tmp/arena_mongo_$$.err; exit 0
fi
rm -f /tmp/arena_mongo_$$.err
alive(){ docker ps -q --no-trunc | grep -q "$(docker inspect -f '{{.Id}}' "$C" 2>/dev/null)"; }
msh(){ docker exec "$C" mongosh --quiet arena --eval "$1" 2>&1; }
ready=0
for i in $(seq 1 90); do
  docker exec "$C" mongosh --quiet --eval "db.runCommand({ping:1}).ok" >/dev/null 2>&1 && { ready=1; break; }
  alive || break
  sleep 1
done
[ "$ready" = 1 ] || { echo "LANE=mongodb STATUS=dnf REASON=engine-not-ready-in-90s(container crashed or slow; logs: docker logs $C)"; exit 0; }

# ---- (5) INGEST (all cores: --numInsertionWorkers = client count) + INDEXES, timed as LOAD_MS -------
t0="$(now_ns)"
if ! docker exec -i "$C" mongoimport --quiet --db arena --collection docs --type json \
      --numInsertionWorkers "$NCLIENTS" < "$JSONL" >/tmp/arena_mongo_imp_$$.err 2>&1; then
  echo "LANE=mongodb STATUS=dnf REASON=mongoimport-failed([$(tr -d '\n' </tmp/arena_mongo_imp_$$.err|tail -c 90)])"
  rm -f /tmp/arena_mongo_imp_$$.err; exit 0
fi
rm -f /tmp/arena_mongo_imp_$$.err
msh 'db.docs.createIndex({"shipping.region_id":1}); db.docs.createIndex({category_id:1});' >/dev/null 2>&1
LOAD_MS="$(ms_since "$t0")"
alive || { echo "LANE=mongodb STATUS=dnf REASON=engine-died-during-load(logs: docker logs $C)"; exit 0; }
CNT="$(msh 'print(db.docs.countDocuments({}))' | grep -oE '[0-9]+' | tail -1)"
if [ -z "$CNT" ] || [ "$CNT" -ne "$ROWS" ]; then
  echo "LANE=mongodb STATUS=dnf REASON=load-incomplete(ingested=${CNT:-0}/$ROWS)"; exit 0
fi
echo "[mongodb] load: ingested $CNT docs in ${LOAD_MS}ms (numInsertionWorkers=$NCLIENTS)" >&2

# ---- (6) EQUAL-ANSWER canonical pass (single client; the correctness gate, not timed) ---------------
CANON="$(msh "
var CATS=$CATS, Q1ID=$Q1ID, Q3REG=$Q3REG;
var d=db.docs.find({_id:Q1ID},{amount_cents:1,_id:0}).toArray();
var q1=(d.length? d[0].amount_cents : -1);
var r=db.docs.aggregate([{\$group:{_id:'\$category_id',n:{\$sum:1}}}],{allowDiskUse:true}).toArray();
var m={}; r.forEach(function(x){m[x._id]=x.n});
var parts=[]; for(var i=0;i<CATS;i++){parts.push((m[i]||0).toString());}
var q3=db.docs.countDocuments({'shipping.region_id':Q3REG});
print('MONGO_Q1='+q1); print('MONGO_Q2='+parts.join(',')); print('MONGO_Q3='+q3);
")"
M_Q1="$(sed -n 's/^MONGO_Q1=\(.*\)/\1/p' <<<"$CANON" | tail -1)"
M_Q2="$(sed -n 's/^MONGO_Q2=\(.*\)/\1/p' <<<"$CANON" | tail -1)"
M_Q3="$(sed -n 's/^MONGO_Q3=\(.*\)/\1/p' <<<"$CANON" | tail -1)"
if [ -z "$M_Q1" ] || [ -z "$M_Q2" ] || [ -z "$M_Q3" ]; then
  echo "LANE=mongodb STATUS=dnf REASON=query-engine-error([$(printf '%s' "$CANON"|tr '\n' ' '|tail -c 110)])"; exit 0
fi
MONGO_HASH="$(hash_answer "$M_Q1|$M_Q2|$M_Q3")"
echo "[mongodb]   mongo answers: Q1=$M_Q1 Q3=$M_Q3 hash=$MONGO_HASH" >&2
if [ "$MONGO_HASH" != "$REF" ]; then
  echo "LANE=mongodb STATUS=dnf REASON=equal-answer-mismatch(mongo!=independent-awk-reference)"; exit 0
fi

# ---- (7) TIMED phase: NCLIENTS concurrent clients per query; wall = slowest client's loop -----------
# Each client is one mongosh process that loops ITERS times internally (startup amortised once), forces
# materialisation, and prints its loop wall. We take the MAX across clients as the concurrent batch wall
# (the point at which every client has drained), then per-op = ceil(wall/iters). ops/s -> stderr.
CDIR="$WORK/timings"; mkdir -p "$CDIR"
run_concurrent(){ # qjs iters tag  ; echoes: "<wall_ms> <ok_clients>"
  local qjs="$1" iters="$2" tag="$3" k pids=() v mx=0 okc=0
  rm -f "$CDIR/$tag."* 2>/dev/null
  for k in $(seq 1 "$NCLIENTS"); do
    ( docker exec "$C" mongosh --quiet arena --eval "
        var ROWS=$ROWS, ITERS=$iters, CATS=$CATS, Q1ID=$Q1ID, Q3REG=$Q3REG;
        var t0=Date.now();
        for(var i=0;i<ITERS;i++){ $qjs }
        print('CLIENT_ELAPSED_MS='+(Date.now()-t0));
      " 2>/dev/null | grep -m1 CLIENT_ELAPSED_MS > "$CDIR/$tag.$k" ) &
    pids+=($!)
  done
  for p in "${pids[@]}"; do wait "$p" 2>/dev/null; done
  for k in $(seq 1 "$NCLIENTS"); do
    v="$(sed -n 's/CLIENT_ELAPSED_MS=\([0-9]*\)/\1/p' "$CDIR/$tag.$k" 2>/dev/null)"
    [ -n "$v" ] || continue
    okc=$((okc+1)); [ "$v" -gt "$mx" ] && mx="$v"
  done
  echo "$mx $okc"
}
ceilms(){ local w="${1:-0}" it="${2:-1}"; [ "$it" -gt 0 ] || it=1; local m=$(( (w + it - 1) / it )); [ "$m" -lt 1 ] && m=1; echo "$m"; }
thr(){ local w="${1:-0}" it="${2:-1}"; [ "$w" -gt 0 ] || { echo 0; return; }; echo $(( NCLIENTS * it * 1000 / w )); }

read -r W1 OK1 < <(run_concurrent "var id=Math.floor(Math.random()*ROWS)+1; db.docs.findOne({_id:id},{amount_cents:1,_id:0});" "$Q1_ITERS" q1)
read -r W2 OK2 < <(run_concurrent "db.docs.aggregate([{\$group:{_id:'\$category_id',n:{\$sum:1}}}],{allowDiskUse:true}).toArray();" "$Q2_ITERS" q2)
read -r W3 OK3 < <(run_concurrent "db.docs.countDocuments({'shipping.region_id':Q3REG});" "$Q3_ITERS" q3)
alive || { echo "LANE=mongodb STATUS=dnf REASON=engine-died-during-query(logs: docker logs $C)"; exit 0; }
if [ "${OK1:-0}" -lt 1 ] || [ "${OK2:-0}" -lt 1 ] || [ "${OK3:-0}" -lt 1 ]; then
  echo "LANE=mongodb STATUS=dnf REASON=concurrent-timing-produced-no-result(q1_ok=$OK1 q2_ok=$OK2 q3_ok=$OK3)"; exit 0
fi
Q1_MS="$(ceilms "$W1" "$Q1_ITERS")"; Q2_MS="$(ceilms "$W2" "$Q2_ITERS")"; Q3_MS="$(ceilms "$W3" "$Q3_ITERS")"
echo "[mongodb] concurrency: $NCLIENTS clients (ok q1=$OK1 q2=$OK2 q3=$OK3) across $(nproc 2>/dev/null||echo ?) cores" >&2
echo "[mongodb]   Q1 point-lookup   per-op=${Q1_MS}ms  thr=$(thr "$W1" "$Q1_ITERS") ops/s (wall ${W1}ms x ${Q1_ITERS} iters/client)" >&2
echo "[mongodb]   Q2 group-by-agg   per-op=${Q2_MS}ms  thr=$(thr "$W2" "$Q2_ITERS") ops/s (wall ${W2}ms x ${Q2_ITERS} iters/client)" >&2
echo "[mongodb]   Q3 nested-count   per-op=${Q3_MS}ms  thr=$(thr "$W3" "$Q3_ITERS") ops/s (wall ${W3}ms x ${Q3_ITERS} iters/client)" >&2

# ---- (8) verified -> emit the board line -----------------------------------------------------------
echo "[mongodb] EQUAL-ANSWER OK vs independent awk reference; emitting board line." >&2
echo "LANE=mongodb STATUS=ok LOAD_MS=$LOAD_MS Q1_MS=$Q1_MS Q2_MS=$Q2_MS Q3_MS=$Q3_MS ANSWER_HASH=$REF"
exit 0
