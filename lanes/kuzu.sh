#!/usr/bin/env bash
# Kuzu GRAPH lane (embedded, MIT): COPY the shared graph into an on-disk Kuzu db (LOAD), then a HOT
# k-hop reachability count via variable-length `*0..HOPS` (QUERY). Optional hard memory cap via
# systemd-run + a matching buffer-pool size (out-of-core regime). Equal-answer: `visited` must match
# the reference. Emits: LANE=kuzu STATUS=ok LOAD_MS=.. QUERY_MS=.. VISITED=..
set -uo pipefail; here="$(cd "$(dirname "$0")"&&pwd)"
SCRATCH="${GRAPH_SCRATCH:?}"; CAP="${GRAPH_MEM_CAP:-0}"; CAPMB="${GRAPH_CAP_MB:-0}"
KUZU_VER="${KUZU_VER:-0.11.3}"
KZ="${KUZU_BIN:-}"; [ -z "$KZ" ] && [ -x "$here/../bin/kuzu" ] && KZ="$here/../bin/kuzu"
[ -z "$KZ" ] && KZ="$(command -v kuzu 2>/dev/null || true)"
# Kuzu is MIT and embedded; we do not redistribute their binary in the kit -- fetch the PINNED release
# once into kit/bin (offline after first fetch). Honest-skip if it cannot be fetched.
if [ -z "$KZ" ] || [ ! -x "$KZ" ]; then
  dest="$here/../bin/kuzu"; mkdir -p "$here/../bin"
  url="https://github.com/kuzudb/kuzu/releases/download/v${KUZU_VER}/kuzu_cli-linux-x86_64.tar.gz"
  if command -v curl >/dev/null 2>&1 && curl -fsSL "$url" -o /tmp/kuzu_$$.tgz 2>/dev/null \
     && tar xzf /tmp/kuzu_$$.tgz -C "$here/../bin" kuzu 2>/dev/null && [ -x "$dest" ]; then
    KZ="$dest"; rm -f /tmp/kuzu_$$.tgz
  else
    rm -f /tmp/kuzu_$$.tgz
    echo "LANE=kuzu STATUS=skip REASON=kuzu-cli-not-found-and-fetch-failed(set KUZU_BIN, or download v${KUZU_VER} from github.com/kuzudb/kuzu/releases into kit/bin/kuzu)"; exit 0
  fi
fi

NODES_CSV="${GRAPH_NODES_CSV:?}"; EDGES_CSV="${GRAPH_EDGES_CSV:?}"; SEEDS="${GRAPH_SEEDS_CSV:?}"; HOPS="${GRAPH_HOPS:-3}"
mkdir -p "$SCRATCH"
DB="$SCRATCH/kuzu.db"; rm -rf "$DB"
BP=""; [ "$CAPMB" != 0 ] && BP="-d $(( CAPMB * 3 / 4 ))"
cap_wrap(){ if [ "$CAP" != 0 ] && [ -n "$CAP" ] && command -v systemd-run >/dev/null 2>&1; then
    systemd-run --scope -p MemoryMax="$CAP" -p MemorySwapMax=0 --quiet "$@"; else "$@"; fi; }

load_cql="$SCRATCH/kz_load.cql"; q_cql="$SCRATCH/kz_q.cql"
cat > "$load_cql" <<CQL
CREATE NODE TABLE N(id INT64 PRIMARY KEY);
CREATE REL TABLE E(FROM N TO N);
COPY N FROM "$NODES_CSV";
COPY E FROM "$EDGES_CSV";
CQL
echo "MATCH (s:N)-[:E*0..$HOPS]->(m:N) WHERE s.id IN [$SEEDS] RETURN count(DISTINCT m.id) AS visited;" > "$q_cql"

t0=$(now_ns)
LOUT="$(cap_wrap "$KZ" "$DB" $BP -b -s < "$load_cql" 2>&1)"; lrc=$?
lm=$(ms_since $t0)
if [ $lrc -ne 0 ] || printf '%s' "$LOUT" | grep -qiE 'killed|out of memory|cannot alloc|bad_alloc'; then
  rm -rf "$DB"; echo "LANE=kuzu STATUS=dnf REASON=oom-or-error-during-load-at-cap-$CAP"; exit 0
fi
t0=$(now_ns)
QOUT="$(cap_wrap "$KZ" "$DB" $BP -b -s -r < "$q_cql" 2>&1)"; qrc=$?
qm=$(ms_since $t0)
rm -rf "$DB"
if [ $qrc -ne 0 ] || printf '%s' "$QOUT" | grep -qiE 'killed|out of memory|bad_alloc'; then
  echo "LANE=kuzu STATUS=dnf REASON=oom-or-error-during-query-at-cap-$CAP"; exit 0
fi
V="$(printf '%s' "$QOUT" | grep -oE '[0-9]+' | tail -1)"
[ -n "$V" ] || { echo "LANE=kuzu STATUS=dnf REASON=no-result([$(printf '%s' "$QOUT"|tr '\n' ' '|tail -c 80)])"; exit 0; }
echo "LANE=kuzu STATUS=ok LOAD_MS=$lm QUERY_MS=$qm VISITED=$V"
