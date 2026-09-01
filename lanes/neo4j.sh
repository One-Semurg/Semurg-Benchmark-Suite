#!/usr/bin/env bash
# Neo4j GRAPH lane (Community, GPLv3): stand up neo4j:5.26-community in Docker, batch-ingest the shared
# graph (LOAD), then a HOT k-hop reachability count via APOC subgraphNodes (proper BFS incl. start nodes
# -> matches the reference; the pure-Cypher variable-length form over-counts and is NOT used). Optional
# hard memory cap via `docker --memory` (out-of-core regime). Equal-answer: `visited` must match the
# reference; a lane that cannot build/query the graph under the budget reports STATUS=dnf (honest), it
# never fakes a number. Emits: LANE=neo4j STATUS=ok LOAD_MS=.. QUERY_MS=.. VISITED=..
set -uo pipefail; here="$(cd "$(dirname "$0")"&&pwd)"; . "$here/_common.sh"
N="${GRAPH_NODES:?}"; HOPS="${GRAPH_HOPS:-3}"; SEEDS="${GRAPH_SEEDS_CSV:?}"; EDGES_CSV="${GRAPH_EDGES_CSV:?}"
CAP="${GRAPH_MEM_CAP:-0}"; C="arena_neo4j_$$"; IMG="${NEO4J_IMAGE:-neo4j:5.26-community}"
st="$(arena_docker_status)"; [ "$st" = ok ] || { echo "LANE=neo4j STATUS=skip REASON=docker-$st($(arena_docker_fix "$st"))"; exit 0; }
MEMOPT=""; [ "$CAP" != 0 ] && [ -n "$CAP" ] && MEMOPT="--memory=${CAP,,} --memory-swap=${CAP,,}"
docker rm -f "$C" >/dev/null 2>&1
trap 'docker rm -f "$C" >/dev/null 2>&1 || true' EXIT INT TERM
if ! docker run -d --name "$C" $MEMOPT --label "$ARENA_LABEL=1" \
     -e NEO4J_AUTH=none -e 'NEO4J_PLUGINS=["apoc"]' -e NEO4J_dbms_security_procedures_unrestricted='apoc.*' \
     "$IMG" >/tmp/neo4j_$$.err 2>&1; then
  echo "LANE=neo4j STATUS=skip REASON=image-start-failed([$(tr -d '\n' </tmp/neo4j_$$.err|tail -c 80)])"; rm -f /tmp/neo4j_$$.err; exit 0
fi
rm -f /tmp/neo4j_$$.err
alive(){ docker ps -q --no-trunc | grep -q "$(docker inspect -f '{{.Id}}' "$C" 2>/dev/null)"; }
cyq(){ docker exec -i "$C" cypher-shell --format plain "$1" 2>&1; }
ready=0; for i in $(seq 1 90); do cyq "RETURN 1;" >/dev/null 2>&1 && { ready=1; break; }; alive || break; sleep 2; done
[ "$ready" = 1 ] || { echo "LANE=neo4j STATUS=dnf REASON=engine-not-ready-under-cap-$CAP(container OOM/crash on start)"; exit 0; }

docker cp "$EDGES_CSV" "$C":/var/lib/neo4j/import/edges.csv >/dev/null 2>&1
cyq "CREATE CONSTRAINT nid IF NOT EXISTS FOR (n:N) REQUIRE n.id IS UNIQUE;" >/dev/null 2>&1
t0=$(now_ns)
# BATCHED node + rel creation (each its own transaction) so Neo4j gets its best shot under the budget.
cyq "UNWIND range(1,$N) AS i CALL (i) { CREATE (:N {id:i}) } IN TRANSACTIONS OF 50000 ROWS;" >/dev/null 2>&1
LOAD="$(cyq "LOAD CSV FROM 'file:///edges.csv' AS row CALL (row) { MATCH (a:N {id: toInteger(row[0])}), (b:N {id: toInteger(row[1])}) CREATE (a)-[:E]->(b) } IN TRANSACTIONS OF 20000 ROWS;")"
lm=$(ms_since $t0)
alive || { echo "LANE=neo4j STATUS=dnf REASON=oom-killed-during-load-at-cap-$CAP"; exit 0; }
EDG="$(cyq "MATCH ()-[e:E]->() RETURN count(e) AS c;" | grep -oE '[0-9]+' | tail -1)"
EXP=$(wc -l < "$EDGES_CSV")
# if the graph did not fully materialise under the budget, that is a DNF, not a wrong answer.
if [ -z "$EDG" ] || [ "$EDG" -lt "$EXP" ]; then
  echo "LANE=neo4j STATUS=dnf REASON=graph-not-built-under-cap-$CAP(edges=${EDG:-0}/$EXP; batched-txn could not complete)"; exit 0
fi
t0=$(now_ns)
V="$(cyq "MATCH (s:N) WHERE s.id IN [$SEEDS] WITH collect(s) AS ss CALL apoc.path.subgraphNodes(ss, {maxLevel:$HOPS, relationshipFilter:'E>'}) YIELD node RETURN count(node) AS visited;")"
qm=$(ms_since $t0)
alive || { echo "LANE=neo4j STATUS=dnf REASON=oom-killed-during-query-at-cap-$CAP"; exit 0; }
VN="$(printf '%s' "$V" | grep -oE '[0-9]+' | tail -1)"
if printf '%s' "$V" | grep -qiE 'apoc.*not|unknown function|no procedure'; then
  echo "LANE=neo4j STATUS=skip REASON=apoc-plugin-unavailable(needs internet at container start for NEO4J_PLUGINS=apoc)"; exit 0
fi
[ -n "$VN" ] || { echo "LANE=neo4j STATUS=dnf REASON=no-result([$(printf '%s' "$V"|tr '\n' ' '|tail -c 80)])"; exit 0; }
echo "LANE=neo4j STATUS=ok LOAD_MS=$lm QUERY_MS=$qm VISITED=$VN"
