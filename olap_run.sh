#!/usr/bin/env bash
# olap_run.sh -- the COLUMNAR head-to-head: Semurg vs the two columnar incumbents (DuckDB, ClickHouse)
# on the SAME narrow COUNT(*) GROUP BY over the SAME N rows / G groups, EQUAL-ANSWER gated (every engine
# must return the identical per-bucket counts -- the same sha256 hash). Reports Semurg's TWO numbers
# honestly with the correct direction:
#   * RAW SCAN  -- Semurg's O(N) SIMD sweep. A columnar warehouse (DuckDB, ClickHouse) is built for
#                 exactly this, so Semurg's raw scan may LOSE here. We print the loss straight; no spin.
#   * FOLD      -- fold-at-ingest: the same histogram served O(1) off a running monoid (a point-get, not
#                 a scan). This is the architectural win (delete the scan, don't out-scan the scanner).
# DuckDB = single-purpose columnar (embedded, MIT). ClickHouse = single-purpose columnar (docker,
# Apache-2.0). Both driven at MAX concurrency on their node (all cores); losses reported alongside wins.
set -uo pipefail
HERE="$(cd "$(dirname "$0")"&&pwd)"
LANES="${OLAP_LANES_DIR:-$HERE/lanes}"
export OLAP_EXS="${OLAP_EXS:-$HERE/workload/semurg_olap.exs}"
export OLAP_SCRATCH="${OLAP_SCRATCH:-${TMPDIR:-/tmp}/arena_olap}"
ROWS="${OLAP_ROWS:-20000000}"; NG="${OLAP_GROUPS:-64}"
TO="${LANE_TIMEOUT:-600}"
. "$LANES/_common.sh"
mkdir -p "$OLAP_SCRATCH"

echo "############################################################################################"
echo "# COLUMNAR head-to-head: Semurg vs DuckDB (MIT) vs ClickHouse (Apache-2.0) -- equal-answer.  #"
echo "############################################################################################"
echo "== workload: COUNT(*) GROUP BY (row % $NG) over $ROWS rows (in-core, equal-answer) =="

# Capture the ONE machine line each lane emits: a LANE= result line, or a SKIP line (docker lanes emit
# `SKIP <lane> reason=...` when the env is unavailable -- keep it so the reason renders, never a crash).
run(){ timeout -k 10 "${TO}s" bash "$LANES/$1.sh" 2>/dev/null | grep -m1 -E '^LANE=|^SKIP ' || echo "LANE=$1 STATUS=dnf REASON=timed-out-or-errored"; }
reason_of(){ printf '%s' "$1" | sed -n 's/^SKIP [^ ]* *reason=\(.*\)/\1/p;s/.*REASON=\(.*\)/\1/p;s/.*reason=\(.*\)/\1/p' | head -1; }

sline="$(OLAP_ROWS="$ROWS" OLAP_GROUPS="$NG" run semurg_olap)"
dline="$(OLAP_ROWS="$ROWS" OLAP_GROUPS="$NG" run duckdb_olap)"
chline="$(OLAP_ROWS="$ROWS" OLAP_GROUPS="$NG" run clickhouse)"

sscan=$(sed -n 's/.*SCAN_RPS=\([0-9]*\).*/\1/p' <<<"$sline"); sfold=$(sed -n 's/.*FOLD_RPS=\([0-9]*\).*/\1/p' <<<"$sline")
sans=$(sed -n 's/.*ANSWER=\([0-9a-f]*\).*/\1/p' <<<"$sline"); sstat=$(sed -n 's/.*STATUS=\([a-z]*\).*/\1/p' <<<"$sline")
drps=$(sed -n 's/.*ROWS_PER_S=\([0-9]*\).*/\1/p' <<<"$dline"); dans=$(sed -n 's/.*ANSWER=\([0-9a-f]*\).*/\1/p' <<<"$dline")
dstat=$(sed -n 's/.*STATUS=\([a-z]*\).*/\1/p' <<<"$dline")
chrps=$(sed -n 's/.*ROWS_PER_S=\([0-9]*\).*/\1/p' <<<"$chline"); chans=$(sed -n 's/.*ANSWER=\([0-9a-f]*\).*/\1/p' <<<"$chline")
chstat=$(sed -n 's/.*STATUS=\([a-z]*\).*/\1/p' <<<"$chline")
grep -q '^SKIP ' <<<"$chline" && chstat=skip

# the equal-answer reference: Semurg's answer where it ran, else the first incumbent's (DuckDB).
ref="${sans:-$dans}"

echo
OLAP_MISMATCH=0
printf "   %-16s %-16s %-13s %s\n" engine rows_per_s equal-answer note
if [ "$sstat" = ok ]; then
  eq="(reference)"
  printf "   %-16s %-16s %-13s %s\n" "semurg (scan)" "${sscan:-–}" "$eq" "O(N) SIMD sweep"
  printf "   %-16s %-16s %-13s %s\n" "semurg (fold)" "${sfold:-–}" "$eq" "O(1) fold-at-ingest point-get"
else
  printf "   %-16s %-16s %-13s %s\n" "semurg" "–" "DNF" "$(reason_of "$sline")"
fi
if [ "$dstat" = ok ]; then
  if [ -n "$ref" ] && [ "$dans" != "$ref" ]; then deq="MISMATCH"; OLAP_MISMATCH=1; else deq="OK"; fi
  printf "   %-16s %-16s %-13s %s\n" "duckdb" "${drps:-–}" "$deq" "columnar in-core aggregate (MIT)"
else
  printf "   %-16s %-16s %-13s %s\n" "duckdb" "–" "${dstat:-DNF}" "$(reason_of "$dline")"
fi
if [ "$chstat" = ok ]; then
  if [ -n "$ref" ] && [ "$chans" != "$ref" ]; then cheq="MISMATCH"; OLAP_MISMATCH=1; else cheq="OK"; fi
  printf "   %-16s %-16s %-13s %s\n" "clickhouse" "${chrps:-–}" "$cheq" "columnar MergeTree aggregate (Apache-2.0)"
else
  # SKIP (docker/image/engine unavailable) or DNF -- print the reason straight, never a fake number.
  printf "   %-16s %-16s %-13s %s\n" "clickhouse" "–" "${chstat:-DNF}" "$(reason_of "$chline")"
fi

echo
if [ "$sstat" = ok ] && [ "$dstat" = ok ] && [ "$dans" = "$ref" ] && [ -n "$drps" ] && [ "$drps" -gt 0 ] 2>/dev/null; then
  awk -v ds="$drps" -v ss="$sscan" -v sf="$sfold" 'BEGIN{
    if (ss>0){ if (ds>=ss) printf "   raw scan:  DuckDB %.1fx faster than Semurg'"'"'s raw scan (honest loss -- a columnar warehouse out-scans a row sweep)\n", ds/ss;
               else        printf "   raw scan:  Semurg %.1fx faster than DuckDB on the raw scan\n", ss/ds }
    if (sf>0)   printf "   fold:      Semurg fold serves the SAME answer %.0fx faster than DuckDB (O(1) point-get vs an O(N) aggregate -- the architectural win)\n", sf/ds
  }'
fi
if [ "$sstat" = ok ] && [ "$chstat" = ok ] && [ "$chans" = "$ref" ] && [ -n "$chrps" ] && [ "$chrps" -gt 0 ] 2>/dev/null; then
  awk -v cs="$chrps" -v ss="$sscan" -v sf="$sfold" 'BEGIN{
    if (ss>0){ if (cs>=ss) printf "   raw scan:  ClickHouse %.1fx faster than Semurg'"'"'s raw scan (honest loss -- a columnar warehouse out-scans a row sweep)\n", cs/ss;
               else        printf "   raw scan:  Semurg %.1fx faster than ClickHouse on the raw scan\n", ss/cs }
    if (sf>0)   printf "   fold:      Semurg fold serves the SAME answer %.0fx faster than ClickHouse (O(1) point-get vs an O(N) aggregate -- the architectural win)\n", sf/cs
  }'
fi
if [ "$sstat" != ok ] || { [ "$dstat" != ok ] && [ "$chstat" != ok ]; }; then
  echo "   (comparison shown only when Semurg and an incumbent both return the equal answer)"
fi
echo
echo "Reading it: Semurg's RAW SCAN can lose to a columnar warehouse -- we print that straight. The win"
echo "is the FOLD: the identical histogram served O(1) off a running monoid (delete the scan, don't try"
echo "to out-scan the scanner). Both DuckDB and ClickHouse return the SAME bit-exact per-bucket counts"
echo "Semurg does (equal-answer gated); a ClickHouse SKIP just means docker/the image was unavailable."
[ "${OLAP_MISMATCH:-0}" = 0 ] || { echo; echo "PARITY FAIL: an engine disagreed with the equal-answer reference (MISMATCH above). Exiting non-zero so a corrupted answer never passes green."; exit 3; }
rm -rf "$OLAP_SCRATCH" 2>/dev/null || true
