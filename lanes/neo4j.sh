#!/usr/bin/env bash
# Neo4j GRAPH lane (Community, GPLv3): stand up neo4j:5.26-community in Docker, batch-ingest the shared
# graph (LOAD), then a HOT k-hop reachability count via APOC subgraphNodes (proper BFS incl. start nodes
# -> matches the reference; the pure-Cypher variable-length form over-counts and is NOT used). Optional
# hard memory cap via `docker --memory` (out-of-core regime). Equal-answer: `visited` must match the
# reference; a lane that cannot build/query the graph under the budget reports a clean STATUS=dnf/skip
# with a REAL reason (honest), it never fakes a number and never leaves the board with no line.
#
# ROBUSTNESS (so a full-board run under contention never yields a bare "no-LANE-line" DNF):
#   * ONE machine line is ALWAYS emitted -- an EXIT trap emits a clean SKIP if the lane is terminated
#     (e.g. killed by the outer per-lane timeout) before it produced a result.
#   * an INTERNAL time budget (NEO4J_LANE_BUDGET, default 840s, kept under the runners' 900s outer
#     timeout) bounds boot/load/query so the lane emits its OWN clean line BEFORE the outer kill.
#   * Neo4j is given explicit heap + pagecache so it is not starved (and, under a --memory cap, is sized
#     to FIT the cap instead of being OOM-killed while over-grabbing).
#   * readiness waits for Bolt to actually accept a query (cheap log check for "Bolt enabled"/"Started."
#     then one confirming probe), with a generous bounded window for slow boot/APOC under contention.
# Emits: LANE=neo4j STATUS=ok LOAD_MS=.. QUERY_MS=.. VISITED=..
set -uo pipefail; here="$(cd "$(dirname "$0")"&&pwd)"; . "$here/_common.sh"
HOPS="${GRAPH_HOPS:-3}"; SEEDS="${GRAPH_SEEDS_CSV:?}"; EDGES_CSV="${GRAPH_EDGES_CSV:?}"
# node count: honor GRAPH_NODES when the caller sets it (graph_run.sh); otherwise derive the max node id
# from the edges this lane already requires. run_all_domains passes only GRAPH_NODES_CSV (the same
# contract kuzu/semurg use), so deriving here makes the lane self-sufficient under BOTH runners -- one
# data contract, never a bare no-LANE-line DNF from an unset var (Law 0).
N="${GRAPH_NODES:-$(awk -F, '{if($1+0>m)m=$1+0; if($2+0>m)m=$2+0} END{print m+0}' "$EDGES_CSV")}"
CAP="${GRAPH_MEM_CAP:-0}"; C="arena_neo4j_$$"; IMG="${NEO4J_IMAGE:-neo4j:5.26-community}"
BUDGET="${NEO4J_LANE_BUDGET:-840}"          # seconds; MUST stay under the runners' outer timeout (900)
READY_S="${NEO4J_READY_S:-240}"             # boot + APOC load can be slow under full-board contention
START_TS="$(now_ns)"
remaining(){ echo $(( BUDGET - ( ( $(now_ns) - START_TS ) / 1000000000 ) )); }

# ---- ALWAYS emit exactly one LANE line; the EXIT trap covers an outer-timeout kill ------------------
EMITTED=0
emit(){ [ "$EMITTED" = 1 ] && return 0; EMITTED=1; printf '%s\n' "$1"; }
cleanup(){ docker rm -f "$C" >/dev/null 2>&1 || true
  [ "$EMITTED" = 1 ] || printf 'LANE=neo4j STATUS=skip REASON=lane-terminated-before-result(exceeded outer per-lane timeout under full-board contention at cap-%s; run the standalone graph_run.sh for its number)\n' "$CAP"; }
trap cleanup EXIT
trap 'exit 143' INT TERM

st="$(arena_docker_status)"; [ "$st" = ok ] || { emit "LANE=neo4j STATUS=skip REASON=docker-$st($(arena_docker_fix "$st"))"; exit 0; }

# ---- memory: give Neo4j its best shot; under a --memory cap, size the JVM to FIT the cap -------------
MEMOPT=""; NEOMEM=()
if [ "$CAP" != 0 ] && [ -n "$CAP" ]; then
  MEMOPT="--memory=${CAP,,} --memory-swap=${CAP,,}"
  # OOC: small heap + modest pagecache so the JVM does not over-grab and get cgroup-OOM-killed; Neo4j is
  # disk-based, so it builds via pagecache -- but a graph bigger than the WHOLE cap still cannot hold.
  NEOMEM=(-e NEO4J_server_memory_heap_initial__size=512m -e NEO4J_server_memory_heap_max__size=768m -e NEO4J_server_memory_pagecache_size=768m)
else
  # in-core: generous heap + pagecache so load/query are not starved.
  NEOMEM=(-e NEO4J_server_memory_heap_initial__size=2g -e NEO4J_server_memory_heap_max__size=4g -e NEO4J_server_memory_pagecache_size=2g)
fi

docker rm -f "$C" >/dev/null 2>&1
if ! docker run -d --name "$C" $MEMOPT "${NEOMEM[@]}" --label "$ARENA_LABEL=1" \
     -e NEO4J_AUTH=none -e 'NEO4J_PLUGINS=["apoc"]' -e NEO4J_dbms_security_procedures_unrestricted='apoc.*' \
     "$IMG" >/tmp/neo4j_$$.err 2>&1; then
  emit "LANE=neo4j STATUS=skip REASON=image-start-failed([$(tr -d '\n' </tmp/neo4j_$$.err|tail -c 80)])"; rm -f /tmp/neo4j_$$.err; exit 0
fi
rm -f /tmp/neo4j_$$.err

alive(){ docker ps -q --no-trunc | grep -q "$(docker inspect -f '{{.Id}}' "$C" 2>/dev/null)"; }
# timed cypher: bound each call so a thrashing/hung query cannot consume the whole budget silently.
cyqt(){ local to="$1"; shift; timeout -k 5 "${to}s" docker exec -i "$C" cypher-shell --format plain "$1" 2>&1; }
cyq(){ cyqt 60 "$1"; }

# ---- readiness: wait for Bolt to actually accept a query (cheap log check + one probe) --------------
ready=0; waited=0
while [ "$waited" -lt "$READY_S" ]; do
  if ! alive; then
    lg="$(docker logs --tail 4 "$C" 2>&1 | tr '\n' ' ' | tail -c 90)"
    if printf '%s' "$lg" | grep -qiE 'plugin|download|apoc|unable to|network'; then
      emit "LANE=neo4j STATUS=skip REASON=container-exited-on-start(plugin/download; needs internet for NEO4J_PLUGINS=apoc? logs:[$lg])"
    else
      emit "LANE=neo4j STATUS=dnf REASON=container-exited-before-ready-at-cap-$CAP(OOM/crash on start; logs:[$lg])"
    fi
    exit 0
  fi
  if docker logs "$C" 2>&1 | grep -qE 'Bolt enabled|Started\.'; then
    cyq "RETURN 1;" >/dev/null 2>&1 && { ready=1; break; }
  fi
  sleep 3; waited=$((waited+3))
done
[ "$ready" = 1 ] || { emit "LANE=neo4j STATUS=skip REASON=engine-not-ready-in-${READY_S}s-at-cap-$CAP(slow boot/APOC under contention; logs:[$(docker logs --tail 3 "$C" 2>&1|tr '\n' ' '|tail -c 80)])"; exit 0; }

docker cp "$EDGES_CSV" "$C":/var/lib/neo4j/import/edges.csv >/dev/null 2>&1
cyq "CREATE CONSTRAINT nid IF NOT EXISTS FOR (n:N) REQUIRE n.id IS UNIQUE;" >/dev/null 2>&1

# ---- LOAD (bounded by the remaining budget so it emits a clean line before the outer kill) ----------
t0=$(now_ns)
rem=$(remaining); [ "$rem" -lt 15 ] && { emit "LANE=neo4j STATUS=skip REASON=no-budget-left-before-load-at-cap-$CAP(boot too slow under contention)"; exit 0; }
# BATCHED node + rel creation (each its own transaction) so Neo4j gets its best shot under the budget.
cyqt "$rem" "UNWIND range(1,$N) AS i CALL (i) { CREATE (:N {id:i}) } IN TRANSACTIONS OF 50000 ROWS;" >/dev/null 2>&1
rem=$(remaining); [ "$rem" -lt 10 ] && { emit "LANE=neo4j STATUS=dnf REASON=node-create-exhausted-budget-at-cap-$CAP(graph not built in ${BUDGET}s)"; exit 0; }
LOAD="$(cyqt "$rem" "LOAD CSV FROM 'file:///edges.csv' AS row CALL (row) { MATCH (a:N {id: toInteger(row[0])}), (b:N {id: toInteger(row[1])}) CREATE (a)-[:E]->(b) } IN TRANSACTIONS OF 20000 ROWS;")"; lc=$?
lm=$(ms_since $t0)
alive || { emit "LANE=neo4j STATUS=dnf REASON=oom-killed-during-load-at-cap-$CAP"; exit 0; }
EDG="$(cyq "MATCH ()-[e:E]->() RETURN count(e) AS c;" | grep -oE '[0-9]+' | tail -1)"
EXP=$(wc -l < "$EDGES_CSV")
# if the graph did not fully materialise under the budget, that is a DNF, not a wrong answer.
if [ -z "$EDG" ] || [ "$EDG" -lt "$EXP" ]; then
  if [ "$lc" = 124 ] || [ "$lc" = 137 ]; then
    emit "LANE=neo4j STATUS=dnf REASON=graph-not-built-at-cap-$CAP(edges=${EDG:-0}/$EXP after ${lm}ms; load hit its ${BUDGET}s time budget under contention)"
  else
    emit "LANE=neo4j STATUS=dnf REASON=graph-not-built-under-cap-$CAP(edges=${EDG:-0}/$EXP after ${lm}ms; batched-txn could not complete)"
  fi
  exit 0
fi

# ---- QUERY (bounded) -------------------------------------------------------------------------------
t0=$(now_ns)
rem=$(remaining); [ "$rem" -lt 5 ] && { emit "LANE=neo4j STATUS=dnf REASON=no-budget-left-for-query-at-cap-$CAP"; exit 0; }
V="$(cyqt "$rem" "MATCH (s:N) WHERE s.id IN [$SEEDS] WITH collect(s) AS ss CALL apoc.path.subgraphNodes(ss, {maxLevel:$HOPS, relationshipFilter:'E>'}) YIELD node RETURN count(node) AS visited;")"
qm=$(ms_since $t0)
alive || { emit "LANE=neo4j STATUS=dnf REASON=oom-killed-during-query-at-cap-$CAP"; exit 0; }
if printf '%s' "$V" | grep -qiE 'apoc.*not|unknown function|no procedure'; then
  emit "LANE=neo4j STATUS=skip REASON=apoc-plugin-unavailable(needs internet at container start for NEO4J_PLUGINS=apoc)"; exit 0
fi
VN="$(printf '%s' "$V" | grep -oE '[0-9]+' | tail -1)"
[ -n "$VN" ] || { emit "LANE=neo4j STATUS=dnf REASON=no-result([$(printf '%s' "$V"|tr '\n' ' '|tail -c 80)])"; exit 0; }
emit "LANE=neo4j STATUS=ok LOAD_MS=$lm QUERY_MS=$qm VISITED=$VN"
exit 0
