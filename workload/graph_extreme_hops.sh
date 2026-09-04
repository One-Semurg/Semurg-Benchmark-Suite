#!/usr/bin/env bash
# graph_extreme_hops.sh -- the THIRD graph benchmark: bigger-than-RAM traversal at EXTREME hop depths.
# ONE deterministic bigger-than-budget graph (workload/gen_biggraph.sh). Every incumbent LOADS the graph
# ONCE, then sweeps the hop-depth ladder 2,4,8,16,...,131072, running a HOT k-hop reachability count at
# each depth under a PER-DEPTH WALL-CLOCK CAP. On a cap breach / OOM / crash the incumbent's line
# TRUNCATES at that depth (deeper depths are not attempted) -- the survive-vs-DNF crown as depth grows.
#
# Non-DeWitt public board ONLY: Semurg vs Neo4j (GPLv3 CE) vs Kuzu (MIT). Memgraph / TigerGraph are
# licence-restricted and NEVER appear here.
#
# Emits a TSV (default $HERE/../.arena_data/extreme_hops.tsv, override with OUT_TSV) with the schema the
# table + line chart consume:
#     incumbent <TAB> depth <TAB> cumulative_ms <TAB> status
#   * cumulative_ms = running sum of the per-depth QUERY wall time (load time is reported separately as a
#     `# LOAD ...` comment). This is the Y axis of the line chart; X = depth (CATEGORICAL, not to scale).
#   * status in {ok, cap, oom, dnf, mismatch, skip}. A line is drawn across its `ok` rows and STOPS at
#     the first non-ok row (truncate) -- that stop point is the crown.
#   * equal-answer: when gen_biggraph emitted a reference curve, each engine's visited-count is checked
#     against the reference at that depth; a disagreement is status=mismatch (and truncates, since a
#     wrong count invalidates deeper counts too).
#
# USAGE:
#   # self-test (small graph, small depths, short cap -- proves it runs + truncates):
#   GRAPH_NODES=20000 DEPTHS=2,4,8 PER_DEPTH_CAP_S=20 GRAPH_MEM_CAP=0 bash workload/graph_extreme_hops.sh
#
#   # small out-of-core (exceeds a 256M budget):
#   GRAPH_NODES=3000000 GRAPH_MEM_CAP=256M PER_DEPTH_CAP_S=60 bash workload/graph_extreme_hops.sh
#
#   # real bigger-than-RAM sweep (exceeds a 2G budget; full extreme ladder; long caps):
#   GRAPH_NODES=50000000 GRAPH_MEM_CAP=2G PER_DEPTH_CAP_S=120 \
#     DEPTHS=2,4,8,16,32,64,128,512,1024,2048,4096,8192,16384,32768,65536,131072 \
#     bash workload/graph_extreme_hops.sh
#
# ENV:
#   GRAPH_NODES (3000000)  GRAPH_DEG (10)  GRAPH_SEEDS (64)  GRAPH_A (1000003)
#   DEPTHS (the full extreme ladder)   PER_DEPTH_CAP_S (60)   GRAPH_MEM_CAP (2G; 0 = no cap)
#   GEN_REF (1)   OUT_TSV   LANES ("semurg kuzu neo4j")   SEMURG_REL_BIN (/opt/semurg/bin/r11)
set -uo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"
REPO="$(cd "$HERE/.." && pwd)"
. "$REPO/lanes/_common.sh"

DEPTHS="${DEPTHS:-2,4,8,16,32,64,128,512,1024,2048,4096,8192,16384,32768,65536,131072}"
PER_DEPTH_CAP_S="${PER_DEPTH_CAP_S:-60}"
CAP="${GRAPH_MEM_CAP:-2G}"
N="${GRAPH_NODES:-3000000}"
DEG="${GRAPH_DEG:-10}"
NSEEDS="${GRAPH_SEEDS:-64}"
A="${GRAPH_A:-1000003}"
GEN_REF="${GEN_REF:-1}"
LANES="${LANES:-semurg kuzu neo4j}"
OUT_TSV="${OUT_TSV:-$REPO/.arena_data/extreme_hops.tsv}"
SCRATCH_ROOT="${GRAPH_SCRATCH_ROOT:-${TMPDIR:-/tmp}/arena_xhops}"
SWEEP_EXS="${SWEEP_EXS:-$HERE/semurg_hops_sweep.exs}"
REL="${SEMURG_REL_BIN:-/opt/semurg/bin/r11}"; [ -x "$REL" ] || REL="$(command -v r11 2>/dev/null || true)"

cap_mb(){ case "$1" in 0|"") echo 0;; *[Gg]) echo $(( ${1%[Gg]} * 1024 ));; *[Mm]) echo ${1%[Mm]};; *) echo $(( $1 / 1048576 ));; esac; }
CAP_MB="$(cap_mb "$CAP")"
PER_DEPTH_CAP_MS=$(( PER_DEPTH_CAP_S * 1000 ))

mkdir -p "$SCRATCH_ROOT" "$(dirname "$OUT_TSV")"
BASE="$SCRATCH_ROOT/g"

echo "############################################################################################"
echo "# GRAPH lane 3: EXTREME HOPS on a bigger-than-budget graph. Semurg vs Neo4j (GPLv3) vs Kuzu #"
echo "# (MIT). Per-depth wall cap=${PER_DEPTH_CAP_S}s; mem budget=${CAP}; depths=$DEPTHS"
echo "############################################################################################"

# ---- 1. generate the ONE graph (deterministic). ----
echo "# generating graph N=$N DEG=$DEG seeds=$NSEEDS gen_ref=$GEN_REF ..."
GRAPH_NODES="$N" GRAPH_DEG="$DEG" GRAPH_SEEDS="$NSEEDS" GRAPH_A="$A" GEN_REF="$GEN_REF" \
  bash "$HERE/gen_biggraph.sh" "$BASE"
EDGES_TSV="$BASE.edges.tsv"; SEEDS_TXT="$BASE.seeds.txt"; CURVE="$BASE.answercurve.tsv"; META="$BASE.meta.tsv"
SAT_HOP="$(sed -n 's/^saturation_hop\t//p' "$META")"
SAT_CNT="$(sed -n 's/^saturation_visited\t//p' "$META")"
SEEDS_CSV="$(paste -sd, "$SEEDS_TXT")"
# derived inputs the incumbents want
awk '{print $1","$2}' "$EDGES_TSV" > "$BASE.edges.csv"
seq 1 "$N" > "$BASE.nodes.csv"

# reference at a depth: value from the curve at min(depth, saturation_hop). Echoes "" if no curve.
ref_at(){ local d="$1"; [ -s "$CURVE" ] || { echo ""; return; }
  awk -v d="$d" 'BEGIN{best=""} {if($1<=d){best=$2}} END{print best}' "$CURVE"; }

# ---- 2. TSV header ----
{
  printf '# extreme-hops graph benchmark  generated_at=%s  host=%s\n' "$(date -u +%FT%TZ)" "$(hostname)"
  printf '# N=%s DEG=%s seeds=%s A=%s mem_cap=%s per_depth_cap_s=%s saturation_hop=%s saturation_visited=%s\n' \
         "$N" "$DEG" "$NSEEDS" "$A" "$CAP" "$PER_DEPTH_CAP_S" "${SAT_HOP:-NA}" "${SAT_CNT:-NA}"
  printf 'incumbent\tdepth\tcumulative_ms\tstatus\n'
} > "$OUT_TSV"

emit(){ printf '%s\t%s\t%s\t%s\n' "$1" "$2" "$3" "$4" | tee -a "$OUT_TSV"; }

IFS=',' read -r -a DEPTH_ARR <<< "$DEPTHS"

# ---- 3a. Semurg lane: ONE eval loads once + sweeps all depths internally (truncates in-exs on cap). ----
run_semurg(){
  if [ -z "$REL" ] || [ ! -x "$REL" ]; then
    echo "# LOAD incumbent=semurg SKIP reason=release-not-installed(SEMURG_REL_BIN=$REL)"
    for d in "${DEPTH_ARR[@]}"; do :; done
    emit semurg "${DEPTH_ARR[0]}" 0 skip; return
  fi
  [ -f "$SWEEP_EXS" ] || { echo "# LOAD incumbent=semurg SKIP reason=sweep-exs-missing"; emit semurg "${DEPTH_ARR[0]}" 0 skip; return; }
  local sc="$SCRATCH_ROOT/semurg"; rm -rf "$sc"; mkdir -p "$sc"
  local D="$sc/data"; mkdir -p "$D" "$(dirname "$(dirname "$REL")")/tmp" 2>/dev/null || true
  [ -f /etc/semurg/semurg.env ] && SKB="$(sed -n 's/^SECRET_KEY_BASE=//p' /etc/semurg/semurg.env | head -1)"
  local ENVS=(
    --setenv=RELEASE_TMP="$(dirname "$(dirname "$REL")")/tmp"
    --setenv=SEMURG_DATA_DIR="$D" --setenv=SEMURG_STRIPE_ROOTS="$D/s0"
    --setenv=SECRET_KEY_BASE="${SKB:-arena_scratch_secret_key_base_0000000000000000}"
    --setenv=PORT=4993 --setenv=SEMURG_BIND=127.0.0.1
    --setenv=SEMURG_COLOC_RAM_BUDGET=1073741824 --setenv=SEMURG_RESIDENT_BUDGET_BYTES=1073741824
    --setenv=GRAPH_EDGES="$EDGES_TSV" --setenv=GRAPH_SEEDS_FILE="$SEEDS_TXT"
    --setenv=GRAPH_DEPTHS="$DEPTHS" --setenv=GRAPH_PER_DEPTH_CAP_MS="$PER_DEPTH_CAP_MS"
    --setenv=GRAPH_STORE="$D/graph.bin"
  )
  local CMD=(bash -c "$REL eval \"\$(cat '$SWEEP_EXS')\"")
  local OUT
  if [ "$CAP" != 0 ] && [ -n "$CAP" ] && command -v systemd-run >/dev/null 2>&1; then
    OUT="$(systemd-run --scope -p MemoryMax="$CAP" -p MemorySwapMax=0 --quiet "${ENVS[@]}" "${CMD[@]}" 2>&1)"
  else
    eval "$(for e in "${ENVS[@]}"; do echo "export ${e#--setenv=}"; done)"
    OUT="$("${CMD[@]}" 2>&1)"
  fi
  local lm; lm="$(printf '%s\n' "$OUT" | sed -n 's/.*SEMURG_LOAD load_ms=\([0-9]*\).*/\1/p' | head -1)"
  echo "# LOAD incumbent=semurg load_ms=${lm:-NA} mem_cap=$CAP"
  # parse each SEMURG_HOP line -> tsv; mismatch-check against reference; the exs already truncated on cap.
  local any=0
  while IFS= read -r ln; do
    case "$ln" in SEMURG_HOP*) ;; *) continue;; esac
    any=1
    local d st cum v
    d=$(sed -n 's/.*depth=\([0-9]*\).*/\1/p' <<<"$ln")
    st=$(sed -n 's/.*status=\([a-z]*\).*/\1/p' <<<"$ln")
    cum=$(sed -n 's/.*cumulative_ms=\([0-9]*\).*/\1/p' <<<"$ln")
    v=$(sed -n 's/.*visited=\(-\{0,1\}[0-9]*\).*/\1/p' <<<"$ln")
    if [ "$st" = ok ]; then
      local ref; ref="$(ref_at "$d")"
      if [ -n "$ref" ] && [ "$v" != "$ref" ]; then emit semurg "$d" "$cum" mismatch; break; fi
    fi
    emit semurg "$d" "$cum" "$st"
    [ "$st" = ok ] || break
  done <<< "$OUT"
  if [ "$any" = 0 ]; then
    if printf '%s' "$OUT" | grep -qiE 'killed|out of memory|oom'; then emit semurg "${DEPTH_ARR[0]}" "$PER_DEPTH_CAP_MS" oom
    else emit semurg "${DEPTH_ARR[0]}" 0 dnf; fi
  fi
}

# ---- 3b. Kuzu lane: load db ONCE, then per-depth capped query (path-enum *0..D explodes at depth). ----
run_kuzu(){
  local KZ="${KUZU_BIN:-}"; [ -z "$KZ" ] && [ -x "$REPO/bin/kuzu" ] && KZ="$REPO/bin/kuzu"
  [ -z "$KZ" ] && KZ="$(command -v kuzu 2>/dev/null || true)"
  if [ -z "$KZ" ] || [ ! -x "$KZ" ]; then echo "# LOAD incumbent=kuzu SKIP reason=kuzu-cli-not-found(set KUZU_BIN or place bin/kuzu)"; emit kuzu "${DEPTH_ARR[0]}" 0 skip; return; fi
  local sc="$SCRATCH_ROOT/kuzu"; rm -rf "$sc"; mkdir -p "$sc"
  local DB="$sc/kuzu.db"; local BP=""; [ "$CAP_MB" != 0 ] && BP="-d $(( CAP_MB * 3 / 4 ))"
  local cap_wrap=(); if [ "$CAP" != 0 ] && [ -n "$CAP" ] && command -v systemd-run >/dev/null 2>&1; then
    cap_wrap=(systemd-run --scope -p MemoryMax="$CAP" -p MemorySwapMax=0 --quiet); fi
  local load_cql="$sc/load.cql"
  cat > "$load_cql" <<CQL
CREATE NODE TABLE N(id INT64 PRIMARY KEY);
CREATE REL TABLE E(FROM N TO N);
COPY N FROM "$BASE.nodes.csv";
COPY E FROM "$BASE.edges.csv";
CQL
  local t0 lm LOUT lrc
  t0=$(now_ns); LOUT="$("${cap_wrap[@]}" "$KZ" "$DB" $BP -b -s < "$load_cql" 2>&1)"; lrc=$?; lm=$(ms_since $t0)
  if [ $lrc -ne 0 ] || printf '%s' "$LOUT" | grep -qiE 'killed|out of memory|bad_alloc|cannot alloc'; then
    echo "# LOAD incumbent=kuzu OOM/ERROR at cap=$CAP"; emit kuzu "${DEPTH_ARR[0]}" 0 oom; rm -rf "$DB"; return
  fi
  echo "# LOAD incumbent=kuzu load_ms=$lm mem_cap=$CAP"
  local cum=0
  for d in "${DEPTH_ARR[@]}"; do
    local q_cql="$sc/q.cql"
    echo "MATCH (s:N)-[:E*0..$d]->(m:N) WHERE s.id IN [$SEEDS_CSV] RETURN count(DISTINCT m.id) AS visited;" > "$q_cql"
    local t qms QOUT qrc
    t=$(now_ns)
    QOUT="$(timeout -k 5 "${PER_DEPTH_CAP_S}s" "${cap_wrap[@]}" "$KZ" "$DB" $BP -b -s -r < "$q_cql" 2>&1)"; qrc=$?
    qms=$(ms_since $t); cum=$(( cum + qms ))
    if [ $qrc -eq 124 ]; then emit kuzu "$d" "$cum" cap; break; fi
    if [ $qrc -ne 0 ] || printf '%s' "$QOUT" | grep -qiE 'killed|out of memory|bad_alloc'; then emit kuzu "$d" "$cum" oom; break; fi
    local v; v="$(printf '%s' "$QOUT" | grep -oE '[0-9]+' | tail -1)"
    [ -n "$v" ] || { emit kuzu "$d" "$cum" dnf; break; }
    local ref; ref="$(ref_at "$d")"
    if [ -n "$ref" ] && [ "$v" != "$ref" ]; then emit kuzu "$d" "$cum" mismatch; break; fi
    emit kuzu "$d" "$cum" ok
  done
  rm -rf "$DB"
}

# ---- 3c. Neo4j lane: start container, load ONCE, then per-depth capped apoc.path.subgraphNodes. ----
run_neo4j(){
  local st; st="$(arena_docker_status)"
  [ "$st" = ok ] || { echo "# LOAD incumbent=neo4j SKIP reason=docker-$st($(arena_docker_fix "$st"))"; emit neo4j "${DEPTH_ARR[0]}" 0 skip; return; }
  local C="arena_xhops_neo4j_$$"; local IMG="${NEO4J_IMAGE:-neo4j:5.26-community}"
  local MEMOPT=""; [ "$CAP" != 0 ] && [ -n "$CAP" ] && MEMOPT="--memory=${CAP,,} --memory-swap=${CAP,,}"
  docker rm -f "$C" >/dev/null 2>&1
  trap 'docker rm -f "$C" >/dev/null 2>&1 || true' RETURN
  if ! docker run -d --name "$C" $MEMOPT --label "$ARENA_LABEL=1" \
       -e NEO4J_AUTH=none -e 'NEO4J_PLUGINS=["apoc"]' -e NEO4J_dbms_security_procedures_unrestricted='apoc.*' \
       "$IMG" >/tmp/xhn_$$.err 2>&1; then
    echo "# LOAD incumbent=neo4j SKIP reason=image-start-failed([$(tr -d '\n' </tmp/xhn_$$.err|tail -c 80)])"; rm -f /tmp/xhn_$$.err; emit neo4j "${DEPTH_ARR[0]}" 0 skip; return
  fi
  rm -f /tmp/xhn_$$.err
  local alive cyq
  alive(){ docker ps -q --no-trunc | grep -q "$(docker inspect -f '{{.Id}}' "$C" 2>/dev/null)"; }
  cyq(){ docker exec -i "$C" cypher-shell --format plain "$1" 2>&1; }
  local ready=0 i
  for i in $(seq 1 90); do cyq "RETURN 1;" >/dev/null 2>&1 && { ready=1; break; }; alive || break; sleep 2; done
  [ "$ready" = 1 ] || { echo "# LOAD incumbent=neo4j OOM/not-ready at cap=$CAP"; emit neo4j "${DEPTH_ARR[0]}" 0 oom; return; }
  docker cp "$BASE.edges.csv" "$C":/var/lib/neo4j/import/edges.csv >/dev/null 2>&1
  cyq "CREATE CONSTRAINT nid IF NOT EXISTS FOR (n:N) REQUIRE n.id IS UNIQUE;" >/dev/null 2>&1
  local t0 lm
  t0=$(now_ns)
  cyq "UNWIND range(1,$N) AS i CALL (i) { CREATE (:N {id:i}) } IN TRANSACTIONS OF 50000 ROWS;" >/dev/null 2>&1
  cyq "LOAD CSV FROM 'file:///edges.csv' AS row CALL (row) { MATCH (a:N {id: toInteger(row[0])}), (b:N {id: toInteger(row[1])}) CREATE (a)-[:E]->(b) } IN TRANSACTIONS OF 20000 ROWS;" >/dev/null 2>&1
  lm=$(ms_since $t0)
  if ! alive; then echo "# LOAD incumbent=neo4j OOM-during-load at cap=$CAP"; emit neo4j "${DEPTH_ARR[0]}" 0 oom; return; fi
  local EDG EXP; EDG="$(cyq "MATCH ()-[e:E]->() RETURN count(e) AS c;" | grep -oE '[0-9]+' | tail -1)"; EXP=$(wc -l < "$BASE.edges.csv")
  if [ -z "$EDG" ] || [ "$EDG" -lt "$EXP" ]; then echo "# LOAD incumbent=neo4j graph-not-built-under-cap edges=${EDG:-0}/$EXP"; emit neo4j "${DEPTH_ARR[0]}" 0 dnf; return; fi
  echo "# LOAD incumbent=neo4j load_ms=$lm mem_cap=$CAP edges=$EDG"
  local cum=0 d
  for d in "${DEPTH_ARR[@]}"; do
    local t qms V VN qrc
    t=$(now_ns)
    V="$(timeout -k 5 "${PER_DEPTH_CAP_S}s" docker exec -i "$C" cypher-shell --format plain \
        "MATCH (s:N) WHERE s.id IN [$SEEDS_CSV] WITH collect(s) AS ss CALL apoc.path.subgraphNodes(ss, {maxLevel:$d, relationshipFilter:'E>'}) YIELD node RETURN count(node) AS visited;" 2>&1)"; qrc=$?
    qms=$(ms_since $t); cum=$(( cum + qms ))
    if [ $qrc -eq 124 ]; then emit neo4j "$d" "$cum" cap; break; fi
    if ! alive; then emit neo4j "$d" "$cum" oom; break; fi
    VN="$(printf '%s' "$V" | grep -oE '[0-9]+' | tail -1)"
    [ -n "$VN" ] || { emit neo4j "$d" "$cum" dnf; break; }
    local ref; ref="$(ref_at "$d")"
    if [ -n "$ref" ] && [ "$VN" != "$ref" ]; then emit neo4j "$d" "$cum" mismatch; break; fi
    emit neo4j "$d" "$cum" ok
  done
}

# ---- 4. run the requested lanes ----
for lane in $LANES; do
  echo; echo "== lane: $lane =="
  case "$lane" in
    semurg) run_semurg;;
    kuzu)   run_kuzu;;
    neo4j)  run_neo4j;;
    *) echo "unknown lane: $lane";;
  esac
done

echo
echo "TSV written: $OUT_TSV"
echo "Read it: one row per (incumbent,depth); the line is drawn across ok rows and STOPS at the first"
echo "non-ok status (cap/oom/dnf/mismatch) -- that truncation point is the survive-vs-DNF crown."
rm -rf "$SCRATCH_ROOT" 2>/dev/null || true
