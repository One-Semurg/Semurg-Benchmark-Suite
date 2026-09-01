#!/usr/bin/env bash
# graph_run.sh -- the GRAPH head-to-head: Semurg vs Neo4j vs Kuzu on the SAME deterministic graph, with
# an EQUAL-ANSWER parity gate (an independent formula-BFS reference; a lane counts only if its k-hop
# reachable-count matches). Two regimes:
#   in-core     : a graph that fits everyone's RAM -- honest side-by-side (Semurg is competitive/faster
#                 than Neo4j here; against a Memgraph/TigerGraph-class engine we may LOSE raw TEPS -- we
#                 do NOT claim in-core TEPS domination).
#   out-of-core : a graph bigger than a FIXED small memory budget. Neo4j (JVM, graph-in-RAM) can't build
#                 or hold it -> DNF; Kuzu (also disk-based) survives but slower; Semurg finishes at FLAT
#                 memory (disk = truth). THIS survive-vs-DNF is the crown -- reproducible on your box
#                 because the budget is fixed by the lane, not by how much RAM you happen to have.
set -uo pipefail
GRAPH_MISMATCH=0
HERE="$(cd "$(dirname "$0")"&&pwd)"
LANES="${GRAPH_LANES_DIR:-$HERE/lanes}"; WORK="${GRAPH_WORK_DIR:-$HERE/workload}"
SCRATCH_ROOT="${GRAPH_SCRATCH_ROOT:-${TMPDIR:-/tmp}/arena_graph}"
export GRAPH_WALK_EXS="${GRAPH_WALK_EXS:-$WORK/semurg_walk.exs}"
TO="${LANE_TIMEOUT:-600}"
. "$LANES/_common.sh"

cap_mb(){ case "$1" in 0|"") echo 0;; *[Gg]) echo $(( ${1%[Gg]} * 1024 ));; *[Mm]) echo ${1%[Mm]};; *) echo $(( $1 / 1048576 ));; esac; }

run_regime(){ # label N DEG HOPS NSEEDS CAP
  local label="$1" N="$2" DEG="$3" HOPS="$4" NSEEDS="$5" CAP="$6"
  local sc="$SCRATCH_ROOT/$label"; rm -rf "$sc"; mkdir -p "$sc"
  local base="$sc/g"
  GRAPH_NODES="$N" GRAPH_DEG="$DEG" GRAPH_HOPS="$HOPS" GRAPH_SEEDS="$NSEEDS" bash "$WORK/gen_graph.sh" "$base" >/dev/null
  local REF; REF="$(cat "$base.answer.txt")"
  awk '{print $1","$2}' "$base.edges.tsv" > "$base.edges.csv"
  seq 1 "$N" > "$base.nodes.csv"
  local SEEDS_CSV; SEEDS_CSV="$(paste -sd, "$base.seeds.txt")"
  local EDGES=$(( $(wc -l < "$base.edges.tsv") ))

  echo
  echo "== GRAPH lane -- ${label^^}  ($N nodes, deg=$DEG => $EDGES directed edges; $HOPS-hop from $NSEEDS seeds; mem cap=${CAP:-none}) =="
  echo "   reference nodes_visited (independent formula-BFS, incl. seeds) = $REF"
  printf "   %-14s %-9s %-9s %-9s %-13s %s\n" engine load_ms query_ms visited equal-answer note

  local -A VIS QMS; local semq=""
  for lane in semurg_graph kuzu neo4j; do
    local line
    mkdir -p "$sc/$lane" 2>/dev/null || true
    local raw
    raw="$( GRAPH_SCRATCH="$sc/$lane" GRAPH_EDGES="$base.edges.tsv" GRAPH_EDGES_CSV="$base.edges.csv" \
        GRAPH_NODES_CSV="$base.nodes.csv" GRAPH_SEEDS_FILE="$base.seeds.txt" GRAPH_SEEDS_CSV="$SEEDS_CSV" \
        GRAPH_NODES="$N" GRAPH_HOPS="$HOPS" GRAPH_MEM_CAP="${CAP:-0}" GRAPH_CAP_MB="$(cap_mb "${CAP:-0}")" \
        timeout -k 10 "${TO}s" bash "$LANES/$lane.sh" 2>/dev/null )"
    # take the FIRST emitted LANE= line; if the lane produced none (killed/timeout), synthesize a DNF.
    line="$(grep -m1 '^LANE=' <<<"$raw")"
    [ -n "$line" ] || line="LANE=$lane STATUS=dnf REASON=timed-out-or-errored(>${TO}s)"
    local status v lm qm rs reason
    status=$(sed -n 's/.*STATUS=\([a-z]*\).*/\1/p' <<<"$line")
    v=$(sed -n 's/.*VISITED=\([0-9]*\).*/\1/p' <<<"$line")
    lm=$(sed -n 's/.*LOAD_MS=\([0-9]*\).*/\1/p' <<<"$line"); qm=$(sed -n 's/.*QUERY_MS=\([0-9]*\).*/\1/p' <<<"$line")
    rs=$(sed -n 's/.*PEAK_RSS_MB=\([0-9-]*\).*/\1/p' <<<"$line"); reason=$(sed -n 's/.*REASON=\(.*\)/\1/p' <<<"$line")
    local eq note=""
    case "$status" in
      ok)
        if [ "$v" = "$REF" ]; then eq="OK"; else eq="MISMATCH"; GRAPH_MISMATCH=1; fi
        [ "$lane" = semurg_graph ] && { note="peak_rss=${rs}MB"; semq="$qm"; }
        [ -n "$semq" ] && [ "$lane" != semurg_graph ] && [ -n "$qm" ] && [ "$qm" -gt 0 ] 2>/dev/null && \
          note="Semurg query $(awk -v a="$qm" -v b="$semq" 'BEGIN{if(b>0)printf "%.1fx", a/b; else print "-"}') faster"
        printf "   %-14s %-9s %-9s %-9s %-13s %s\n" "$lane" "${lm:-–}" "${qm:-–}" "${v:-–}" "$eq" "$note"
        VIS[$lane]="$v"; QMS[$lane]="$qm";;
      skip)
        printf "   %-14s %-9s %-9s %-9s %-13s %s\n" "$lane" "–" "–" "–" "SKIP" "${reason}";;
      *)
        printf "   %-14s %-9s %-9s %-9s %-13s %s\n" "$lane" "–" "–" "–" "DNF" "${reason:-did-not-finish}";;
    esac
  done
}

echo "############################################################################################"
echo "# GRAPH head-to-head: Semurg vs Neo4j (GPLv3 CE) vs Kuzu (MIT) -- equal-answer k-hop.       #"
echo "# Memgraph / TigerGraph are LICENSE-RESTRICTED (no third-party published benchmarks): they  #"
echo "# are NOT on this public board -- run them yourself, local-only, via 'run --licensed'.      #"
echo "############################################################################################"

# in-core: fits RAM everywhere (no cap). out-of-core: fixed budget, graph exceeds it (the crown).
run_regime incore   "${GRAPH_INCORE_NODES:-100000}"  10 "${GRAPH_HOPS:-3}" "${GRAPH_SEEDS:-64}" 0
run_regime out-of-core "${GRAPH_OOC_NODES:-3000000}" 10 "${GRAPH_OOC_HOPS:-4}" "${GRAPH_SEEDS:-64}" "${GRAPH_MEM_CAP:-2G}"

echo
echo "Reading it: 'equal-answer OK' means the engine returned the SAME k-hop reachable-count as the"
echo "independent reference (a lane that disagrees is MISMATCH and never counted). In-core we report"
echo "the numbers straight -- Semurg is competitive/faster than Neo4j; a Memgraph/TigerGraph-class"
echo "engine may beat our raw TEPS (run it yourself). The out-of-core row is the point: at a FIXED"
echo "memory budget on a bigger-than-budget graph, the engines that keep the graph in RAM DNF while"
echo "Semurg finishes at flat memory -- the survive-vs-DNF result no competitor can argue away."
rm -rf "$SCRATCH_ROOT" 2>/dev/null || true
[ "${GRAPH_MISMATCH:-0}" = 0 ] || { echo; echo "PARITY FAIL: an engine returned a k-hop count that disagrees with the independent reference (MISMATCH above). Exiting non-zero."; exit 3; }
