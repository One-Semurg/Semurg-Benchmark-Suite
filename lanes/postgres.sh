#!/usr/bin/env bash
# NOTE: no `set -e` -- a lane must NEVER abort the whole board. It isolates its own failure and prints
# a single SKIP/FAILED line so run_all continues to the next engine.
set -uo pipefail; here="$(cd "$(dirname "$0")"&&pwd)"; . "$here/_common.sh"
DATA="${ARENA_DATA:?}"; IMG="${PG_IMAGE:-postgres:16}"; C="arena_pg_$$"
st="$(arena_docker_status)"
[ "$st" = ok ] || { echo "SKIP postgres reason=docker-$st fix=[$(arena_docker_fix "$st")]"; exit 0; }
docker rm -f "$C" >/dev/null 2>&1 || true                       # reap any stale same-name container
trap 'docker rm -f "$C" >/dev/null 2>&1 || true' EXIT INT TERM  # always clean up (Ctrl-C included)
if ! docker run -d --rm --label "$ARENA_LABEL=1" --name "$C" -e POSTGRES_PASSWORD=a -e POSTGRES_DB=arena "$IMG" >/tmp/arena_pg_$$.err 2>&1; then
  echo "SKIP postgres reason=image-pull-or-start-failed detail=[$(tr -d '\n' </tmp/arena_pg_$$.err | tail -c 90)] fix=[check network/registry, disk space, or: docker pull $IMG]"; rm -f /tmp/arena_pg_$$.err; exit 0
fi
rm -f /tmp/arena_pg_$$.err
ready=0
for i in $(seq 1 60); do
  docker exec "$C" pg_isready -U postgres >/dev/null 2>&1 && { ready=1; break; }
  docker ps -q --no-trunc | grep -q "$(docker inspect -f '{{.Id}}' "$C" 2>/dev/null)" || break   # container died
  sleep 1
done
[ "$ready" = 1 ] || { echo "SKIP postgres reason=engine-not-ready-in-60s(container crashed or slow; logs: docker logs $C)"; exit 0; }
psql(){ docker exec -i "$C" psql -qtAX -U postgres -d arena -c "$1"; }
run_pg(){
  psql "CREATE TABLE orders(order_id bigint, customer_id bigint, product_id bigint, amount_cents bigint, ts bigint);" >/dev/null || return 1
  local t; t=$(now_ns)
  docker exec -i "$C" psql -qtAX -U postgres -d arena -c "\copy orders FROM STDIN WITH (FORMAT csv, HEADER true)" < "$DATA/orders.csv" >/dev/null || return 1
  psql "CREATE INDEX ON orders(order_id);" >/dev/null || return 1; local load_ms; load_ms=$(ms_since $t)
  local q1 q2 q3 q1ms q2ms q3ms
  t=$(now_ns); q1=$(psql "SELECT product_id||':'||SUM(amount_cents)||':'||COUNT(*) FROM orders GROUP BY product_id ORDER BY SUM(amount_cents) DESC, product_id ASC LIMIT 10;" | tr '\n' ';') || return 1; q1ms=$(ms_since $t)
  t=$(now_ns); q2=$(psql "SELECT amount_cents FROM orders WHERE order_id=424242;") || return 1; q2ms=$(ms_since $t)
  t=$(now_ns); q3=$(psql "SELECT COUNT(*) FROM orders WHERE amount_cents>500000;") || return 1; q3ms=$(ms_since $t)
  echo "LANE=postgres LOAD_MS=$load_ms Q1_MS=$q1ms Q2_MS=$q2ms Q3_MS=$q3ms ANSWER_HASH=$(hash_answer "$q1|$q2|$q3")"
}
run_pg || echo "LANE=postgres status=FAILED reason=query-error(engine ran but a step failed; logs: docker logs $C)"
