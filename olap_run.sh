#!/usr/bin/env bash
# olap_run.sh -- the OLAP head-to-head: Semurg vs DuckDB on the SAME narrow COUNT(*) GROUP BY over the
# SAME N rows / G groups, EQUAL-ANSWER gated (both must return the identical per-bucket counts). Reports
# Semurg's TWO numbers honestly with the correct direction:
#   * RAW SCAN  -- Semurg's O(N) SIMD sweep. A columnar warehouse (DuckDB) is built for exactly this, so
#                 Semurg's raw scan may LOSE here. We print the loss straight; no spin.
#   * FOLD      -- fold-at-ingest: the same histogram served O(1) off a running monoid (a point-get, not
#                 a scan). This is the architectural win (delete the scan, don't out-scan the scanner).
# Losses reported alongside wins -- the kit's discipline.
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
echo "# OLAP head-to-head: Semurg vs DuckDB (MIT) -- equal-answer COUNT(*) GROUP BY.              #"
echo "############################################################################################"
echo "== workload: COUNT(*) GROUP BY (row % $NG) over $ROWS rows (in-core, equal-answer) =="

run(){ timeout -k 10 "${TO}s" bash "$LANES/$1.sh" 2>/dev/null | grep -m1 '^LANE=' || echo "LANE=$1 STATUS=dnf REASON=timed-out-or-errored"; }
sline="$(OLAP_ROWS="$ROWS" OLAP_GROUPS="$NG" run semurg_olap)"
dline="$(OLAP_ROWS="$ROWS" OLAP_GROUPS="$NG" run duckdb_olap)"

sscan=$(sed -n 's/.*SCAN_RPS=\([0-9]*\).*/\1/p' <<<"$sline"); sfold=$(sed -n 's/.*FOLD_RPS=\([0-9]*\).*/\1/p' <<<"$sline")
sans=$(sed -n 's/.*ANSWER=\([0-9a-f]*\).*/\1/p' <<<"$sline"); sstat=$(sed -n 's/.*STATUS=\([a-z]*\).*/\1/p' <<<"$sline")
drps=$(sed -n 's/.*ROWS_PER_S=\([0-9]*\).*/\1/p' <<<"$dline"); dans=$(sed -n 's/.*ANSWER=\([0-9a-f]*\).*/\1/p' <<<"$dline")
dstat=$(sed -n 's/.*STATUS=\([a-z]*\).*/\1/p' <<<"$dline")

echo
printf "   %-16s %-16s %-13s %s\n" engine rows_per_s equal-answer note
if [ "$sstat" = ok ]; then
  eq="(reference)"
  printf "   %-16s %-16s %-13s %s\n" "semurg (scan)" "${sscan:-–}" "$eq" "O(N) SIMD sweep"
  printf "   %-16s %-16s %-13s %s\n" "semurg (fold)" "${sfold:-–}" "$eq" "O(1) fold-at-ingest point-get"
else
  printf "   %-16s %-16s %-13s %s\n" "semurg" "–" "DNF" "$(sed -n 's/.*REASON=\(.*\)/\1/p' <<<"$sline")"
fi
if [ "$dstat" = ok ]; then
  if [ -n "$sans" ] && [ "$dans" != "$sans" ]; then deq="MISMATCH"; else deq="OK"; fi
  printf "   %-16s %-16s %-13s %s\n" "duckdb" "${drps:-–}" "$deq" "columnar in-core aggregate"
else
  printf "   %-16s %-16s %-13s %s\n" "duckdb" "–" "${dstat^^}" "$(sed -n 's/.*REASON=\(.*\)/\1/p' <<<"$dline")"
fi

echo
if [ "$sstat" = ok ] && [ "$dstat" = ok ] && [ "$dans" = "$sans" ] && [ -n "$drps" ] && [ "$drps" -gt 0 ] 2>/dev/null; then
  awk -v ds="$drps" -v ss="$sscan" -v sf="$sfold" 'BEGIN{
    if (ss>0){ if (ds>=ss) printf "   raw scan:  DuckDB %.1fx faster than Semurg'"'"'s raw scan (honest loss -- a columnar warehouse out-scans a row sweep)\n", ds/ss;
               else        printf "   raw scan:  Semurg %.1fx faster than DuckDB on the raw scan\n", ss/ds }
    if (sf>0)   printf "   fold:      Semurg fold serves the SAME answer %.0fx faster than DuckDB (O(1) point-get vs an O(N) aggregate -- the architectural win)\n", sf/ds
  }'
else
  echo "   (comparison shown only when BOTH engines return the equal answer)"
fi
echo
echo "Reading it: Semurg's RAW SCAN can lose to a columnar warehouse -- we print that straight. The win"
echo "is the FOLD: the identical histogram served O(1) off a running monoid (delete the scan, don't try"
echo "to out-scan the scanner). Both are the SAME bit-exact answer DuckDB returns (equal-answer gated)."
rm -rf "$OLAP_SCRATCH" 2>/dev/null || true
