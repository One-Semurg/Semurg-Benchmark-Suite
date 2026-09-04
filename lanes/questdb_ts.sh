#!/usr/bin/env bash
# QuestDB TIME-SERIES lane (DOMAIN 3). QuestDB is a purpose-built time-series engine with NO client tools
# in-container: drive it from the HOST over the REST API (:9000) -- load via POST /imp, query via GET /exec
# + jq (same transport as the existing questdb.sh orders lane). Runs the canonical DOWNSAMPLE query set,
# equal-answer to Timescale/kdb+:
#   Q1  DOWNSAMPLE / time-bucket rollup : per B-second bucket -> count, sum(value), min, max  (ORDER BY bucket)
#   Q2  PER-SERIES rollup               : one device_id -> count, sum(value), min, max
#   Q3  RANGE-WINDOW count              : count of readings in [WIN_LO, WIN_HI)
# The bucket is (ts - ts%B) integer-exact (QuestDB's SAMPLE BY primitive in exact-integer form) so the
# hash is bit-identical to the other engines with no timezone/alignment drift.
set -uo pipefail; here="$(cd "$(dirname "$0")"&&pwd)"; . "$here/_common.sh"
DATA="${TS_DATA:?set TS_DATA=/path/to/ts_data.csv}"
IMG="${QDB_IMAGE:-questdb/questdb:8.1.1}"
B="${TS_BUCKET:-3600}"; WLO="${WIN_LO:-0}"; WHI="${WIN_HI:-9223372036854775807}"; DEV="${Q2_DEVICE:-42}"
C="arena_qdbts_$$"
command -v curl >/dev/null 2>&1 || { echo "SKIP questdb_ts reason=host-missing-curl fix=[install curl]"; exit 0; }
command -v jq   >/dev/null 2>&1 || { echo "SKIP questdb_ts reason=host-missing-jq fix=[install jq]"; exit 0; }
st="$(arena_docker_status)"; [ "$st" = ok ] || { echo "SKIP questdb_ts reason=docker-$st fix=[$(arena_docker_fix "$st")]"; exit 0; }
docker rm -f "$C" >/dev/null 2>&1 || true
trap 'docker rm -f "$C" >/dev/null 2>&1 || true' EXIT INT TERM
if ! docker run -d --rm --label "$ARENA_LABEL=1" --name "$C" "$IMG" >/tmp/arena_qdbts_$$.err 2>&1; then
  echo "SKIP questdb_ts reason=image-pull-or-start-failed detail=[$(tr -d '\n' </tmp/arena_qdbts_$$.err | tail -c 90)] fix=[docker pull $IMG]"; rm -f /tmp/arena_qdbts_$$.err; exit 0
fi
rm -f /tmp/arena_qdbts_$$.err
IP="$(docker inspect -f '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' "$C" 2>/dev/null)"
[ -n "$IP" ] || { echo "SKIP questdb_ts reason=no-container-ip(logs: docker logs $C)"; exit 0; }
BASE="http://$IP:9000"
ready=0; for i in $(seq 1 60); do curl -sfG "$BASE/exec" --data-urlencode "query=SELECT 1" >/dev/null 2>&1 && { ready=1; break; }; docker ps -q --no-trunc | grep -q "$(docker inspect -f '{{.Id}}' "$C" 2>/dev/null)" || break; sleep 1; done
[ "$ready" = 1 ] || { echo "SKIP questdb_ts reason=engine-not-ready-in-60s(logs: docker logs $C)"; exit 0; }
qexec(){ curl -sG "$BASE/exec" --data-urlencode "query=$1"; }
run_qdb(){
  local t; t=$(now_ns)
  local imp; imp=$(curl -s -F schema='[{"name":"device_id","type":"LONG"},{"name":"ts","type":"LONG"},{"name":"value","type":"LONG"}]' -F data=@"$DATA" "$BASE/imp?name=ts_data&overwrite=true&fmt=json")
  echo "$imp" | jq -e '.status=="OK"' >/dev/null 2>&1 || return 1
  local load_ms; load_ms=$(ms_since $t)
  local q1 q2 q3 q1ms q2ms q3ms
  # Q1: implicit GROUP BY on the non-aggregate bucket key (same shape as questdb.sh orders lane).
  t=$(now_ns); q1=$(qexec "SELECT concat(b, ':', c, ':', s, ':', mn, ':', mx) v FROM (SELECT ts - ts%$B b, count() c, sum(value) s, min(value) mn, max(value) mx FROM ts_data ORDER BY b ASC)" | jq -r '.dataset[][0]' | tr '\n' ';') || return 1; q1ms=$(ms_since $t)
  # QuestDB will NOT nest aggregates directly inside concat() in the projection -> wrap them in a
  # subquery first (same shape as Q1), then concat the scalar results.
  t=$(now_ns); q2=$(qexec "SELECT concat(c, ':', s, ':', mn, ':', mx) v FROM (SELECT count() c, sum(value) s, min(value) mn, max(value) mx FROM ts_data WHERE device_id=$DEV)" | jq -r 'if (.dataset|length)>0 then .dataset[0][0] else empty end') || return 1; q2ms=$(ms_since $t)
  t=$(now_ns); q3=$(qexec "SELECT count() FROM ts_data WHERE ts>=$WLO AND ts<$WHI" | jq -r '.dataset[0][0]|tostring') || return 1; q3ms=$(ms_since $t)
  echo "LANE=questdb_ts LOAD_MS=$load_ms Q1_MS=$q1ms Q2_MS=$q2ms Q3_MS=$q3ms ANSWER_HASH=$(hash_answer "$q1|$q2|$q3")"
}
run_qdb || echo "LANE=questdb_ts status=FAILED reason=query-error(logs: docker logs $C)"
