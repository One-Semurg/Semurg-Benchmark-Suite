#!/usr/bin/env bash
# TimescaleDB TIME-SERIES lane (DOMAIN 3). Stands up the official timescale/timescaledb image, makes a
# genuine HYPERTABLE on the integer time column, bulk-loads the SAME deterministic ts_data.csv, and runs
# the canonical time-series DOWNSAMPLE query set (equal-answer to every other engine on the board):
#   Q1  DOWNSAMPLE / time-bucket rollup : per B-second bucket -> count, sum(value), min, max  (ORDER BY bucket)
#   Q2  PER-SERIES rollup               : one device_id -> count, sum(value), min, max over its whole series
#   Q3  RANGE-WINDOW count              : count of readings in [WIN_LO, WIN_HI)
# The bucket is expressed as exact integer arithmetic (ts - ts % B) -- the time_bucket() primitive in
# integer-exact form -- so the answer HASH is bit-identical across Timescale/QuestDB/kdb+ (no timezone or
# float drift). The hypertable + chunk exclusion is the real TS engine doing the work underneath.
# NOTE: no `set -e` -- a lane must NEVER abort the whole board; it isolates its own failure to one SKIP line.
set -uo pipefail; here="$(cd "$(dirname "$0")"&&pwd)"; . "$here/_common.sh"
DATA="${TS_DATA:?set TS_DATA=/path/to/ts_data.csv}"
IMG="${TS_IMAGE:-timescale/timescaledb:latest-pg16}"
B="${TS_BUCKET:-3600}"; WLO="${WIN_LO:-0}"; WHI="${WIN_HI:-9223372036854775807}"; DEV="${Q2_DEVICE:-42}"
C="arena_tsts_$$"
st="$(arena_docker_status)"
[ "$st" = ok ] || { echo "SKIP timescale_ts reason=docker-$st fix=[$(arena_docker_fix "$st")]"; exit 0; }
docker rm -f "$C" >/dev/null 2>&1 || true
trap 'docker rm -f "$C" >/dev/null 2>&1 || true' EXIT INT TERM
if ! docker run -d --rm --label "$ARENA_LABEL=1" --name "$C" -e POSTGRES_PASSWORD=a -e POSTGRES_DB=arena "$IMG" >/tmp/arena_tsts_$$.err 2>&1; then
  echo "SKIP timescale_ts reason=image-pull-or-start-failed detail=[$(tr -d '\n' </tmp/arena_tsts_$$.err | tail -c 90)] fix=[docker pull $IMG]"; rm -f /tmp/arena_tsts_$$.err; exit 0
fi
rm -f /tmp/arena_tsts_$$.err
ready=0
for i in $(seq 1 60); do
  docker exec "$C" pg_isready -U postgres >/dev/null 2>&1 && { ready=1; break; }
  docker ps -q --no-trunc | grep -q "$(docker inspect -f '{{.Id}}' "$C" 2>/dev/null)" || break
  sleep 1
done
[ "$ready" = 1 ] || { echo "SKIP timescale_ts reason=engine-not-ready-in-60s(logs: docker logs $C)"; exit 0; }
psql(){ docker exec -i "$C" psql -qtAX -U postgres -d arena -c "$1"; }
run_ts(){
  psql "CREATE EXTENSION IF NOT EXISTS timescaledb CASCADE;" >/dev/null || return 1
  psql "CREATE TABLE ts_data(device_id bigint, ts bigint, value bigint);" >/dev/null || return 1
  # genuine hypertable on the integer time column (1-day chunks) -> real chunk exclusion on Q3's window.
  psql "SELECT create_hypertable('ts_data','ts',chunk_time_interval=>86400);" >/dev/null || return 1
  local t; t=$(now_ns)
  docker exec -i "$C" psql -qtAX -U postgres -d arena -c "\copy ts_data FROM STDIN WITH (FORMAT csv, HEADER true)" < "$DATA" >/dev/null || return 1
  psql "CREATE INDEX ON ts_data(device_id, ts);" >/dev/null || return 1; local load_ms; load_ms=$(ms_since $t)
  local q1 q2 q3 q1ms q2ms q3ms
  t=$(now_ns); q1=$(psql "SELECT (ts-ts%$B)||':'||COUNT(*)||':'||SUM(value)||':'||MIN(value)||':'||MAX(value) FROM ts_data GROUP BY (ts-ts%$B) ORDER BY (ts-ts%$B) ASC;" | tr '\n' ';') || return 1; q1ms=$(ms_since $t)
  t=$(now_ns); q2=$(psql "SELECT COUNT(*)||':'||SUM(value)||':'||MIN(value)||':'||MAX(value) FROM ts_data WHERE device_id=$DEV;") || return 1; q2ms=$(ms_since $t)
  t=$(now_ns); q3=$(psql "SELECT COUNT(*) FROM ts_data WHERE ts>=$WLO AND ts<$WHI;") || return 1; q3ms=$(ms_since $t)
  echo "LANE=timescale_ts LOAD_MS=$load_ms Q1_MS=$q1ms Q2_MS=$q2ms Q3_MS=$q3ms ANSWER_HASH=$(hash_answer "$q1|$q2|$q3")"
}
run_ts || echo "LANE=timescale_ts status=FAILED reason=query-error(engine ran but a step failed; logs: docker logs $C)"
