#!/usr/bin/env bash
# SurrealDB UNIVERSAL lane (domain 11) -- the headline single-source-of-truth head-to-head. SurrealDB is
# the direct multi-model competitor (one store; document + graph + relational at once), so it is THE
# incumbent for the UNIVERSAL / multi-modal domain. This lane stands up surrealdb/surrealdb in Docker,
# loads ONE deterministic dataset under TWO models over the same records, and runs the three canonical
# cross-model queries -- proving the single-store multi-model path end to end:
#   Q1  DOCUMENT point-lookup            SELECT amount FROM person:<id>
#   Q2  DOCUMENT group-by aggregation    SELECT category, count() FROM person GROUP BY category
#   Q3  GRAPH + DOCUMENT join (headline) SELECT count() FROM friend WHERE out.region = <R>
#       -- walk the `friend` graph relation AND read the linked document's region field, in one query.
#
# EQUAL-ANSWER GATE: SurrealDB's Q1|Q2|Q3 hash is compared to an INDEPENDENT awk reference over the same
# CSVs (NOT the engine, see _universal_gen.sh). A number is emitted ONLY when the engine matches the
# reference on all 3 cycles; otherwise an honest STATUS=dnf. The Semurg lane (semurg_universal.sh)
# generates the byte-identical dataset and computes the same reference, so the board renders Semurg vs
# SurrealDB on one equal-answer row.
#
# FAIRNESS: SurrealDB runs at its fastest, non-crippled path -- the in-memory storage engine (its own
# maximum) by default, its tokio server using all cores on the node; override with SURREAL_STORAGE
# (e.g. rocksdb:/data/db) for a disk-backed run. SurrealDB is a single-store engine by design, so it
# uses one store path -- that is its architecture, not a throttle.
#
# Emits ONE machine-readable line run_all_domains.sh parses (same schema as the SQL/doc lanes):
#   LANE=surrealdb STATUS=ok LOAD_MS=.. Q1_MS=.. Q2_MS=.. Q3_MS=.. ANSWER_HASH=<32hex>
# ROBUST: docker/curl/image unavailable or engine-not-ready -> a clean STATUS=skip (never a crash,
# never a fake number).
set -uo pipefail
here="$(cd "$(dirname "$0")" && pwd)"; . "$here/_common.sh"; . "$here/_universal_gen.sh"

IMG="${SURREAL_IMAGE:-surrealdb/surrealdb:latest}"
PORT="${SURREAL_PORT:-18011}"
STORAGE="${SURREAL_STORAGE:-memory}"
CHUNK="${SURREAL_INSERT_CHUNK:-5000}"
C="arena_surreal_$$"

# ---- preflight: docker + curl (jq optional; a pure-awk JSON fallback covers a jq-less box) ----------
st="$(arena_docker_status)"
[ "$st" = ok ] || { echo "LANE=surrealdb STATUS=skip REASON=docker-$st($(arena_docker_fix "$st"))"; exit 0; }
command -v curl >/dev/null 2>&1 || { echo "LANE=surrealdb STATUS=skip REASON=curl-not-available(needed for the SurrealDB HTTP /sql endpoint)"; exit 0; }
HAVE_JQ=0; command -v jq >/dev/null 2>&1 && HAVE_JQ=1

# ---- scratch (throwaway) ---------------------------------------------------------------------------
BASE="${ARENA_DATA:-${TMPDIR:-/tmp}/arena_universal_$$}"
WORK="$BASE/surreal_work"; mkdir -p "$WORK" 2>/dev/null || true
trap 'docker rm -f "$C" >/dev/null 2>&1 || true; rm -rf "$WORK" 2>/dev/null || true' EXIT INT TERM

# ---- (1) start SurrealDB ---------------------------------------------------------------------------
docker rm -f "$C" >/dev/null 2>&1 || true
if ! docker run -d --rm --label "$ARENA_LABEL=1" --name "$C" -p "$PORT:8000" "$IMG" \
     start --user root --pass root --bind 0.0.0.0:8000 "$STORAGE" >/tmp/arena_surreal_$$.err 2>&1; then
  echo "LANE=surrealdb STATUS=skip REASON=image-pull-or-start-failed detail=[$(tr -d '\n' </tmp/arena_surreal_$$.err | tail -c 90)] fix=[check network/registry or: docker pull $IMG]"
  rm -f /tmp/arena_surreal_$$.err; exit 0
fi
rm -f /tmp/arena_surreal_$$.err

# ---- (2) readiness (never wedge: bail the moment the container dies) --------------------------------
ready=0
for i in $(seq 1 60); do
  curl -s -m 3 "http://localhost:$PORT/health" >/dev/null 2>&1 && { ready=1; break; }
  docker ps -q --no-trunc | grep -q "$(docker inspect -f '{{.Id}}' "$C" 2>/dev/null)" || break
  sleep 1
done
[ "$ready" = 1 ] || { echo "LANE=surrealdb STATUS=skip REASON=engine-not-ready-in-60s(container crashed or slow; logs: docker logs $C)"; exit 0; }

# HTTP /sql helper: root:root, ns/db=test, JSON out. --data-binary keeps the SQL verbatim.
SQLURL="http://localhost:$PORT/sql"
q(){ curl -s -m 300 -X POST "$SQLURL" -u root:root -H "surreal-ns: test" -H "surreal-db: test" -H "Accept: application/json" --data-binary "$1" 2>/dev/null; }
qf(){ curl -s -m 600 -X POST "$SQLURL" -u root:root -H "surreal-ns: test" -H "surreal-db: test" -H "Accept: application/json" --data-binary @"$1" 2>/dev/null; }

# ---- (3) deterministic dataset + independent reference (shared generator) ---------------------------
univ_gen_dataset "$WORK" || { echo "LANE=surrealdb STATUS=dnf REASON=dataset-generation-failed"; exit 0; }
univ_compute_reference "$WORK" || { echo "LANE=surrealdb STATUS=dnf REASON=reference-computation-failed"; exit 0; }
ROWS="$(( $(wc -l < "$WORK/persons.csv") - 1 ))"
echo "[surrealdb] rows=$ROWS storage=$STORAGE  independent awk reference:" >&2
echo "[surrealdb]   Q1 person:$UNIV_Q1ID amount=$REF_Q1 | Q2 per-category=[$REF_Q2] | Q3 friend->region=$UNIV_Q3REG -> $REF_Q3" >&2
echo "[surrealdb]   reference ANSWER_HASH=$REF_HASH" >&2

# getnum: extract a numeric field value from a SurrealDB result json ($2=field); jq if present else awk.
getnum(){
  local json="$1" field="$2"
  if [ "$HAVE_JQ" = 1 ]; then
    printf '%s' "$json" | jq -r ".[0].result[0].$field // empty" 2>/dev/null | head -1
  else
    printf '%s' "$json" | grep -o "\"$field\":[0-9]*" | head -1 | grep -o '[0-9]*'
  fi
}

# The SurrealDB HTTP /sql endpoint rejects any request body over 1 MiB (1,048,576 bytes) with the
# plain-text reply "length limit exceeded" (NOT a JSON {"status":"ERR"}), so a single-shot POST of the
# whole ~2.3 MB dataset silently loads NOTHING. q_load_file POSTs a statement-per-line .sql file in
# <=SURREAL_MAX_BODY byte batches (whole statements only, never split), via @file so a 218 KB INSERT
# statement never trips the shell's arg-length limit, and gates EVERY batch response (both a JSON ERR
# and the plain-text rejection are caught). LOAD_ERR carries the first failure detail.
MAXB="${SURREAL_MAX_BODY:-800000}"     # safely under SurrealDB's 1 MiB /sql body limit
LOAD_ERR=""
q_load_file(){ # $1 = .sql file with ONE INSERT statement per line
  local f="$1" batch sz=0 line ls r
  batch="$(mktemp "$WORK/batch.XXXXXX")"; : > "$batch"
  post_batch(){
    r="$(qf "$batch")"
    if printf '%s' "$r" | grep -q '"status":"ERR"'; then LOAD_ERR="$(printf '%s' "$r" | tr '\n' ' ' | grep -o '"result":"[^"]*"' | head -1 | tail -c 90)"; return 1; fi
    if ! printf '%s' "$r" | grep -q '"status":"OK"'; then LOAD_ERR="$(printf '%s' "$r" | tr '\n' ' ' | tail -c 90)"; return 1; fi
    return 0
  }
  while IFS= read -r line; do
    [ -z "$line" ] && continue
    ls=${#line}
    if [ "$sz" -gt 0 ] && [ $(( sz + ls + 1 )) -gt "$MAXB" ]; then
      post_batch || { rm -f "$batch"; return 1; }
      : > "$batch"; sz=0
    fi
    printf '%s\n' "$line" >> "$batch"; sz=$(( sz + ls + 1 ))
  done < "$f"
  if [ "$sz" -gt 0 ]; then post_batch || { rm -f "$batch"; return 1; }; fi
  rm -f "$batch"; return 0
}

# ---- (4) build the SurrealQL load: define ns/db, chunked INSERT INTO person + INSERT RELATION friend
q "DEFINE NAMESPACE IF NOT EXISTS test; DEFINE DATABASE IF NOT EXISTS test;" >/dev/null
# document model: person records with explicit numeric ids (=> person:<id>)
awk -F, -v CH="$CHUNK" 'NR>1{ idx=(NR-2)%CH;
  if(idx==0) printf "INSERT INTO person [";
  printf "%s{id:%d,category:%d,region:%d,amount:%d}", (idx?",":""), $1,$2,$3,$4;
  if(idx==CH-1) printf "];\n";
} END{ if((NR-1)%CH!=0) printf "];\n" }' "$WORK/persons.csv" > "$WORK/person.sql"
# graph model: the friend relation, one edge record per person (in -> out)
awk -F, -v CH="$CHUNK" 'NR>1{ idx=(NR-2)%CH;
  if(idx==0) printf "INSERT RELATION INTO friend [";
  printf "%s{in:person:%d,out:person:%d}", (idx?",":""), $1,$2;
  if(idx==CH-1) printf "];\n";
} END{ if((NR-1)%CH!=0) printf "];\n" }' "$WORK/edges.csv" > "$WORK/friend.sql"

# a load failure is a clean SKIP with the real reason -- never a DNF with 0 rows.
t=$(now_ns)
q_load_file "$WORK/person.sql" || { echo "LANE=surrealdb STATUS=skip REASON=person-load-failed([$LOAD_ERR])"; exit 0; }
q_load_file "$WORK/friend.sql" || { echo "LANE=surrealdb STATUS=skip REASON=friend-load-failed([$LOAD_ERR])"; exit 0; }
LOAD_MS=$(ms_since $t)

# ---- (4b) WAIT for the load to fully materialise: poll the row counts until person==N AND friend==N
# (or a generous timeout). Only then time the queries -- never query a half-loaded store.
POLL_S="${SURREAL_LOAD_POLL_S:-180}"
PCNT=0; FCNT=0; waited=0
while :; do
  PCNT="$(getnum "$(q "SELECT count() AS c FROM person GROUP ALL;")" c)"; PCNT="${PCNT:-0}"
  FCNT="$(getnum "$(q "SELECT count() AS c FROM friend GROUP ALL;")" c)"; FCNT="${FCNT:-0}"
  [ "$PCNT" = "$ROWS" ] && [ "$FCNT" = "$ROWS" ] && break
  [ "$waited" -ge "$POLL_S" ] && break
  sleep 2; waited=$(( waited + 2 ))
done
if [ "$PCNT" != "$ROWS" ] || [ "$FCNT" != "$ROWS" ]; then
  echo "LANE=surrealdb STATUS=skip REASON=load-did-not-reach-full-rows-in-${POLL_S}s(person=$PCNT/$ROWS friend=$FCNT/$ROWS)"
  exit 0
fi
echo "[surrealdb] load: persons=$PCNT friends=$FCNT load_ms=$LOAD_MS (waited ${waited}s for full materialisation)" >&2

# ---- (5) 3-cycle query phase (cold -> warm -> warm); equal-answer gate every cycle ------------------
q1min=""; q2min=""; q3min=""; all_match=1; ncyc=0
for cyc in 1 2 3; do
  ncyc=$((ncyc+1))
  t=$(now_ns); r1="$(q "SELECT amount FROM person:$UNIV_Q1ID;")"; u1=$(ms_since $t)
  t=$(now_ns); r2="$(q "SELECT category, count() AS c FROM person GROUP BY category;")"; u2=$(ms_since $t)
  t=$(now_ns); r3="$(q "SELECT count() AS c FROM friend WHERE out.region = $UNIV_Q3REG GROUP ALL;")"; u3=$(ms_since $t)

  a1="$(getnum "$r1" amount)"
  # Q2 -> comma-joined per-category count over 0..C-1 (fill missing categories with 0)
  if [ "$HAVE_JQ" = 1 ]; then
    a2="$(printf '%s' "$r2" | jq -r '.[0].result[]? | "\(.category) \(.c)"' 2>/dev/null | awk -v C="${UNIV_CATEGORIES:-32}" '{cnt[$1]=$2} END{for(i=0;i<C;i++)printf "%s%d",(i?",":""),cnt[i]+0}')"
  else
    a2="$(printf '%s' "$r2" | grep -o '"c":[0-9]*,"category":[0-9]*' | sed 's/"c"://;s/,"category":/ /' | awk -v C="${UNIV_CATEGORIES:-32}" '{cnt[$2]=$1} END{for(i=0;i<C;i++)printf "%s%d",(i?",":""),cnt[i]+0}')"
  fi
  a3="$(getnum "$r3" c)"; [ -n "$a3" ] || a3=0

  cyc_hash="$(hash_answer "${a1}|${a2}|${a3}")"
  [ "$cyc_hash" = "$REF_HASH" ] || all_match=0
  [ -n "$u1" ] && { [ -z "$q1min" ] || [ "$u1" -lt "$q1min" ]; } && q1min="$u1"
  [ -n "$u2" ] && { [ -z "$q2min" ] || [ "$u2" -lt "$q2min" ]; } && q2min="$u2"
  [ -n "$u3" ] && { [ -z "$q3min" ] || [ "$u3" -lt "$q3min" ]; } && q3min="$u3"
  echo "[surrealdb] cycle=$cyc q1_ms=$u1 q2_ms=$u2 q3_ms=$u3 q1=$a1 q3=$a3 match=$([ "$cyc_hash" = "$REF_HASH" ] && echo yes || echo NO)" >&2
done

# ---- (6) gate + emit -------------------------------------------------------------------------------
if [ "$ncyc" -lt 3 ]; then
  echo "LANE=surrealdb STATUS=dnf REASON=query-phase-incomplete(engine error mid-cycle; logs: docker logs $C)"; exit 0
fi
if [ "$all_match" != 1 ]; then
  echo "LANE=surrealdb STATUS=dnf REASON=equal-answer-mismatch(surrealdb!=independent-awk-reference)"; exit 0
fi
echo "[surrealdb] EQUAL-ANSWER OK on all 3 cycles; emitting board line." >&2
echo "LANE=surrealdb STATUS=ok LOAD_MS=$LOAD_MS Q1_MS=${q1min:-0} Q2_MS=${q2min:-0} Q3_MS=${q3min:-0} ANSWER_HASH=$REF_HASH"
exit 0
