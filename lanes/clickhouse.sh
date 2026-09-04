#!/usr/bin/env bash
# ClickHouse columnar lane -- the SECOND columnar incumbent alongside DuckDB (single-purpose columnar
# warehouse vs Semurg single-source). Runs the SAME narrow COUNT(*) GROUP BY (row % G) over the SAME
# N rows / G groups as semurg_olap + duckdb_olap, EQUAL-ANSWER gated: the answer is the per-bucket
# counts in bucket order, sha256'd -- the IDENTICAL hash the Semurg + DuckDB sides emit (ref d54b754e
# for the 20,000,000-row / 64-group default). ClickHouse (MergeTree = columnar) is driven at MAX
# concurrency on its node (max_threads = all cores); the aggregate is in-core, so cores -- not disks --
# are the fair axis, exactly as the DuckDB :memory: side. Emits, matching the duckdb_olap schema:
#   LANE=clickhouse STATUS=ok ROWS_PER_S=.. ANSWER=<hash>
# ROBUST: a clean SKIP (never a crash, never a fake number) whenever docker / the image / the engine
# is unavailable; a STATUS=dnf line (with reason) on any query error.
set -uo pipefail; here="$(cd "$(dirname "$0")"&&pwd)"; . "$here/_common.sh"

ROWS="${OLAP_ROWS:-20000000}"; NG="${OLAP_GROUPS:-64}"; TH="${OLAP_THREADS:-$(nproc)}"
IMG="${CH_IMAGE:-clickhouse/clickhouse-server}"; C="arena_ch_$$"

# 1) docker gate -- one clean SKIP with the ONE actionable fix, never a cryptic error, never a crash.
st="$(arena_docker_status)"; [ "$st" = ok ] || { echo "SKIP clickhouse reason=docker-$st fix=[$(arena_docker_fix "$st")]"; exit 0; }
docker rm -f "$C" >/dev/null 2>&1 || true
trap 'docker rm -f "$C" >/dev/null 2>&1 || true' EXIT INT TERM
if ! docker run -d --rm --label "$ARENA_LABEL=1" --name "$C" --ulimit nofile=262144:262144 "$IMG" >/tmp/arena_ch_$$.err 2>&1; then
  echo "SKIP clickhouse reason=image-pull-or-start-failed detail=[$(tr -d '\n' </tmp/arena_ch_$$.err | tail -c 90)] fix=[docker pull $IMG]"; rm -f /tmp/arena_ch_$$.err; exit 0
fi
rm -f /tmp/arena_ch_$$.err

# 2) readiness -- bail to a clean SKIP if the server never answers or the container dies (never hang).
ready=0
for i in $(seq 1 60); do
  docker exec "$C" clickhouse-client -q "SELECT 1" >/dev/null 2>&1 && { ready=1; break; }
  docker ps -q --no-trunc | grep -q "$(docker inspect -f '{{.Id}}' "$C" 2>/dev/null)" || break
  sleep 1
done
[ "$ready" = 1 ] || { echo "SKIP clickhouse reason=engine-not-ready-in-60s(logs: docker logs $C)"; exit 0; }

chq(){ docker exec "$C" clickhouse-client -d default --max_threads="$TH" --query "$1"; }

run_ch(){
  # columnar table (MergeTree, no sort key -- a plain full-scan aggregate). Materialise the SAME rows
  # the other columnar lanes aggregate: numbers(1, ROWS) == 1..ROWS (== DuckDB range(1, ROWS+1)).
  chq "CREATE TABLE t (k UInt32) ENGINE=MergeTree ORDER BY tuple()" || return 1
  chq "INSERT INTO t SELECT number % $NG FROM numbers(1, $ROWS)" || return 1

  # timed aggregate: same query shape as the DuckDB side (GROUP BY k ORDER BY k LIMIT 1), best (min)
  # of 3 server-side elapsed times reported by clickhouse-client --time (bare seconds on stderr).
  # One warm-up first so the number reflects the steady columnar scan, not a cold parse.
  local AGG="SELECT k, count() AS c FROM t GROUP BY k ORDER BY k LIMIT 1"
  docker exec "$C" clickhouse-client -d default --max_threads="$TH" --query "$AGG" >/dev/null 2>&1 || return 1
  local best="" s i
  for i in 1 2 3; do
    s=$(docker exec "$C" clickhouse-client -d default --max_threads="$TH" --time --query "$AGG" 2>&1 >/dev/null | grep -oE '[0-9]+\.[0-9]+' | tail -1)
    [ -n "$s" ] || continue
    best=$(awk -v a="$best" -v b="$s" 'BEGIN{ if(a==""||b+0<a+0) print b; else print a }')
  done
  # wall-clock fallback (still a REAL measurement, just with docker-exec overhead) if --time yields nothing.
  if [ -z "$best" ]; then
    local t ms; t=$(now_ns); chq "$AGG" >/dev/null 2>&1 || return 1; ms=$(ms_since "$t")
    best=$(awk -v m="$ms" 'BEGIN{printf "%.3f", (m>0?m:1)/1000.0}')
  fi
  local RPS; RPS=$(awk -v r="$ROWS" -v t="$best" 'BEGIN{ if(t+0>0) printf "%d", r/t }')
  [ -n "$RPS" ] && [ "$RPS" -gt 0 ] 2>/dev/null || return 2

  # equal-answer: per-bucket counts in bucket order -> the SAME comma-joined string Semurg + DuckDB
  # hash (one count per line == Enum.map_join(",") == DuckDB paste -sd,), sha256 first 32 hex chars.
  local COUNTS ANS
  COUNTS=$(chq "SELECT count() FROM t GROUP BY k ORDER BY k" | paste -sd,) || return 1
  [ -n "$COUNTS" ] || return 1
  ANS=$(printf '%s' "$COUNTS" | sha256sum | cut -c1-32)
  echo "LANE=clickhouse STATUS=ok ROWS_PER_S=$RPS ANSWER=$ANS"
}

rc=0; run_ch || rc=$?
case "$rc" in
  0) : ;;
  2) echo "LANE=clickhouse STATUS=dnf REASON=no-timing(aggregate produced no measurable time)";;
  *) echo "LANE=clickhouse STATUS=dnf REASON=query-error(logs: docker logs $C)";;
esac
