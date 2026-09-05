#!/usr/bin/env bash
# Semurg GRAPH lane: bulk-ingest the shared edge list into a THROWAWAY store via the installed release's
# `eval` (never touches the live node's store), build the co-located traversal image (LOAD), then run a
# HOT whole-graph k-hop (QUERY). Optional hard memory cap via systemd-run (out-of-core regime), so the
# survive-at-flat-memory claim is measured under the SAME budget as the incumbents. Emits one line:
#   LANE=semurg_graph STATUS=ok LOAD_MS=.. QUERY_MS=.. VISITED=.. TEPS=.. PEAK_RSS_MB=..
set -uo pipefail; here="$(cd "$(dirname "$0")"&&pwd)"
WALK="${GRAPH_WALK_EXS:?}"; SCRATCH="${GRAPH_SCRATCH:?}"; CAP="${GRAPH_MEM_CAP:-0}"
REL="${SEMURG_REL_BIN:-/opt/semurg/bin/r11}"
[ -x "$REL" ] || REL="$(command -v r11 2>/dev/null || true)"
[ -n "$REL" ] && [ -x "$REL" ] || { echo "LANE=semurg_graph STATUS=skip REASON=release-not-installed(run:semurg-arena install)"; exit 0; }
[ -f "$WALK" ] || { echo "LANE=semurg_graph STATUS=skip REASON=walk-script-missing"; exit 0; }

D="$SCRATCH/semurg_data"; mkdir -p "$D" "$(dirname "$(dirname "$REL")")/tmp" 2>/dev/null || true
# minimal env the release eval needs (runtime.exs). Prefer the installed env's SECRET_KEY_BASE if present.
[ -f /etc/semurg/semurg.env ] && SKB="$(sed -n 's/^SECRET_KEY_BASE=//p' /etc/semurg/semurg.env | head -1)"
ENVS=(
  --setenv=RELEASE_TMP="$(dirname "$(dirname "$REL")")/tmp"
  --setenv=SEMURG_DATA_DIR="$D" --setenv=SEMURG_STRIPE_ROOTS="$D/s0"
  --setenv=SECRET_KEY_BASE="${SKB:-arena_scratch_secret_key_base_0000000000000000}"
  --setenv=PORT=4991 --setenv=SEMURG_BIND=127.0.0.1
  --setenv=SEMURG_COLOC_RAM_BUDGET=1073741824 --setenv=SEMURG_RESIDENT_BUDGET_BYTES=1073741824
  --setenv=GRAPH_EDGES="${GRAPH_EDGES:?}" --setenv=GRAPH_SEEDS_FILE="${GRAPH_SEEDS_FILE:?}"
  --setenv=GRAPH_HOPS="${GRAPH_HOPS:-3}" --setenv=GRAPH_STORE="$D/graph.bin"
)
CMD=(bash -c "$REL eval \"\$(cat '$WALK')\"")

if [ "$CAP" != 0 ] && [ -n "$CAP" ] && command -v systemd-run >/dev/null 2>&1; then
  OUT="$(systemd-run --scope -p MemoryMax="$CAP" -p MemorySwapMax=0 --quiet "${ENVS[@]}" "${CMD[@]}" 2>&1)"; rc=$?
else
  # no cap (in-core) or no systemd: export env directly and run
  eval "$(for e in "${ENVS[@]}"; do echo "export ${e#--setenv=}"; done)"
  OUT="$("${CMD[@]}" 2>&1)"; rc=$?
fi

LINE="$(printf '%s\n' "$OUT" | grep -m1 '^SEMURG_GRAPH ')"
for e in "" .arith .sb .coloc .genoff.idx .genoff.idx.sparse .deadset.idx; do rm -f "$D/graph.bin$e"; done
if [ -z "$LINE" ]; then
  # A cgroup SIGKILL (137) leaves $OUT empty (the OOM notice goes to the journal, not the pipe), so the
  # exit code -- not a text grep -- is the reliable signal. NEVER emit an empty engine-error([]) (BUG-1 FIX B).
  rc="${rc:-0}"
  if [ "$rc" -ge 128 ] 2>/dev/null || printf '%s' "$OUT" | grep -qiE 'killed|out of memory|oom'; then
    echo "LANE=semurg_graph STATUS=dnf REASON=oom-killed-at-cap-$CAP(exit=$rc; raise the cap or lower SEMURG_COLOC_RAM_BUDGET)"
  else
    tail100="$(printf '%s' "$OUT" | tr '\n' ' ' | tail -c 100)"
    echo "LANE=semurg_graph STATUS=dnf REASON=engine-error(exit=$rc: ${tail100:-<no output; process died at cap without printing -- likely SIGKILL>})"
  fi
  exit 0
fi
# translate SEMURG_GRAPH k=v line -> LANE= line
lm=$(sed -n 's/.*load_ms=\([0-9]*\).*/\1/p' <<<"$LINE")
qm=$(sed -n 's/.*query_ms=\([0-9]*\).*/\1/p' <<<"$LINE")
vs=$(sed -n 's/.*visited=\([0-9]*\).*/\1/p' <<<"$LINE")
tp=$(sed -n 's/.*teps=\([0-9]*\).*/\1/p' <<<"$LINE")
rs=$(sed -n 's/.*peak_rss_mb=\([0-9-]*\).*/\1/p' <<<"$LINE")
echo "LANE=semurg_graph STATUS=ok LOAD_MS=$lm QUERY_MS=$qm VISITED=$vs TEPS=$tp PEAK_RSS_MB=$rs"
