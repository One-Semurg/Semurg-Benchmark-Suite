#!/usr/bin/env bash
# DuckDB side of the OLAP head-to-head, EQUAL-ANSWER to the Semurg side: the SAME narrow COUNT(*) GROUP
# BY (r % G) over the SAME rows, in-core (table pre-materialised), at thread parity. Emits:
#   LANE=duckdb_olap STATUS=ok ROWS_PER_S=.. ANSWER=<hash>
# The ANSWER is sha256 of the per-bucket counts in bucket order -- the SAME hash the Semurg side emits.
set -uo pipefail; here="$(cd "$(dirname "$0")"&&pwd)"; . "$here/_common.sh"
ROWS="${OLAP_ROWS:-20000000}"; NG="${OLAP_GROUPS:-64}"; THREADS="${OLAP_THREADS:-$(nproc)}"
DUCK="${DUCKDB_BIN:-duckdb}"
command -v "$DUCK" >/dev/null 2>&1 || { echo "LANE=duckdb_olap STATUS=skip REASON=duckdb-binary-not-found(set DUCKDB_BIN or install from duckdb.org)"; exit 0; }
HI=$((ROWS + 1))

# 1) timed aggregation (best real of 3), table materialised in RAM first.
SQL_T="PRAGMA threads=${THREADS};
CREATE TABLE t AS SELECT (i % ${NG}) AS k FROM range(1, ${HI}) AS r(i);
SELECT k, COUNT(*) AS c FROM t GROUP BY k ORDER BY k LIMIT 1;
.timer on
SELECT k, COUNT(*) AS c FROM t GROUP BY k ORDER BY k LIMIT 1;
SELECT k, COUNT(*) AS c FROM t GROUP BY k ORDER BY k LIMIT 1;
SELECT k, COUNT(*) AS c FROM t GROUP BY k ORDER BY k LIMIT 1;"
BEST="$(printf '%s\n' "$SQL_T" | "$DUCK" :memory: 2>/dev/null | grep -oE 'real [0-9.]+' | awk '{print $2}' | sort -g | head -1)"
[ -n "$BEST" ] || { echo "LANE=duckdb_olap STATUS=dnf REASON=no-timing(duckdb ran but produced no timer output)"; exit 0; }
RPS="$(awk -v r="$ROWS" -v t="$BEST" 'BEGIN{ if (t+0>0) printf "%d", r/t }')"

# 2) the answer: per-bucket counts in bucket order -> the SAME hash the Semurg side emits
#    (one count per line, joined by commas == Semurg's Enum.map_join(counts, ",")).
COUNTS="$(printf 'SELECT COUNT(*) AS c FROM range(1,%s) AS r(i) GROUP BY (i %% %s) ORDER BY (i %% %s);\n' "$HI" "$NG" "$NG" | "$DUCK" :memory: -noheader -list 2>/dev/null | paste -sd,)"
ANS="$(printf '%s' "$COUNTS" | sha256sum | cut -c1-32)"
echo "LANE=duckdb_olap STATUS=ok ROWS_PER_S=$RPS ANSWER=$ANS"
