#!/usr/bin/env bash
# clickhouse lane -- orders workload, equal-answer to postgres. MergeTree requires ORDER BY; || needs toString.
set -uo pipefail; here="$(cd "$(dirname "$0")"&&pwd)"; . "$here/_common.sh"
DATA="${ARENA_DATA:?}"; IMG="${CH_IMAGE:-clickhouse/clickhouse-server}"; C="arena_ch_$$"
st="$(arena_docker_status)"; [ "$st" = ok ] || { echo "SKIP clickhouse reason=docker-$st fix=[$(arena_docker_fix "$st")]"; exit 0; }
docker rm -f "$C" >/dev/null 2>&1 || true
trap 'docker rm -f "$C" >/dev/null 2>&1 || true' EXIT INT TERM
if ! docker run -d --rm --label "$ARENA_LABEL=1" --name "$C" --ulimit nofile=262144:262144 "$IMG" >/tmp/arena_ch_$$.err 2>&1; then
  echo "SKIP clickhouse reason=image-pull-or-start-failed detail=[$(tr -d '\n' </tmp/arena_ch_$$.err | tail -c 90)] fix=[docker pull $IMG]"; rm -f /tmp/arena_ch_$$.err; exit 0
fi
rm -f /tmp/arena_ch_$$.err
ready=0; for i in $(seq 1 60); do docker exec "$C" clickhouse-client -q "SELECT 1" >/dev/null 2>&1 && { ready=1; break; }; docker ps -q --no-trunc | grep -q "$(docker inspect -f '{{.Id}}' "$C" 2>/dev/null)" || break; sleep 1; done
[ "$ready" = 1 ] || { echo "SKIP clickhouse reason=engine-not-ready-in-60s(logs: docker logs $C)"; exit 0; }
chq(){ docker exec -i "$C" clickhouse-client -d default --query "$1"; }
run_ch(){
  chq "CREATE TABLE orders(order_id Int64, customer_id Int64, product_id Int64, amount_cents Int64, ts Int64) ENGINE=MergeTree ORDER BY order_id;" || return 1
  local t; t=$(now_ns)
  docker exec -i "$C" clickhouse-client -d default --query "INSERT INTO orders FORMAT CSVWithNames" < "$DATA/orders.csv" || return 1
  local load_ms; load_ms=$(ms_since $t)
  local q1 q2 q3 q1ms q2ms q3ms
  t=$(now_ns); q1=$(chq "SELECT concat(toString(product_id),':',toString(sum(amount_cents)),':',toString(count())) FROM orders GROUP BY product_id ORDER BY sum(amount_cents) DESC, product_id ASC LIMIT 10" | tr '\n' ';') || return 1; q1ms=$(ms_since $t)
  t=$(now_ns); q2=$(chq "SELECT amount_cents FROM orders WHERE order_id=424242") || return 1; q2ms=$(ms_since $t)
  t=$(now_ns); q3=$(chq "SELECT count() FROM orders WHERE amount_cents>500000") || return 1; q3ms=$(ms_since $t)
  echo "LANE=clickhouse LOAD_MS=$load_ms Q1_MS=$q1ms Q2_MS=$q2ms Q3_MS=$q3ms ANSWER_HASH=$(hash_answer "$q1|$q2|$q3")"
}
run_ch || echo "LANE=clickhouse status=FAILED reason=query-error(logs: docker logs $C)"
