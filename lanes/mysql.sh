#!/usr/bin/env bash
# mysql lane -- orders workload, equal-answer to postgres. No `set -e` (a lane must never abort the board).
set -uo pipefail; here="$(cd "$(dirname "$0")"&&pwd)"; . "$here/_common.sh"
DATA="${ARENA_DATA:?}"; IMG="${MYSQL_IMAGE:-mysql:8}"; C="arena_mysql_$$"
st="$(arena_docker_status)"; [ "$st" = ok ] || { echo "SKIP mysql reason=docker-$st fix=[$(arena_docker_fix "$st")]"; exit 0; }
docker rm -f "$C" >/dev/null 2>&1 || true
trap 'docker rm -f "$C" >/dev/null 2>&1 || true' EXIT INT TERM
if ! docker run -d --rm --label "$ARENA_LABEL=1" --name "$C" -e MYSQL_ROOT_PASSWORD=a -e MYSQL_DATABASE=arena "$IMG" --local-infile=1 >/tmp/arena_mysql_$$.err 2>&1; then
  echo "SKIP mysql reason=image-pull-or-start-failed detail=[$(tr -d '\n' </tmp/arena_mysql_$$.err | tail -c 90)] fix=[docker pull $IMG]"; rm -f /tmp/arena_mysql_$$.err; exit 0
fi
rm -f /tmp/arena_mysql_$$.err
ready=0; for i in $(seq 1 90); do docker exec -e MYSQL_PWD=a "$C" mysql -uroot -N -B -e "SELECT 1" arena >/dev/null 2>&1 && { ready=1; break; }; docker ps -q --no-trunc | grep -q "$(docker inspect -f '{{.Id}}' "$C" 2>/dev/null)" || break; sleep 1; done
[ "$ready" = 1 ] || { echo "SKIP mysql reason=engine-not-ready-in-90s(logs: docker logs $C)"; exit 0; }
myq(){ docker exec -i -e MYSQL_PWD=a "$C" mysql --local-infile=1 -uroot -N -B arena -e "$1"; }
run_mysql(){
  myq "CREATE TABLE orders(order_id BIGINT, customer_id BIGINT, product_id BIGINT, amount_cents BIGINT, ts BIGINT) ENGINE=InnoDB;" || return 1
  local t; t=$(now_ns)
  docker exec -i -e MYSQL_PWD=a "$C" mysql --local-infile=1 -uroot -N -B arena -e "LOAD DATA LOCAL INFILE '/dev/stdin' INTO TABLE orders FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n' IGNORE 1 LINES (order_id,customer_id,product_id,amount_cents,ts);" < "$DATA/orders.csv" || return 1
  myq "CREATE INDEX idx_oid ON orders(order_id);" >/dev/null || return 1; local load_ms; load_ms=$(ms_since $t)
  local q1 q2 q3 q1ms q2ms q3ms
  t=$(now_ns); q1=$(myq "SELECT CONCAT_WS(':', product_id, SUM(amount_cents), COUNT(*)) FROM orders GROUP BY product_id ORDER BY SUM(amount_cents) DESC, product_id ASC LIMIT 10;" | tr '\n' ';') || return 1; q1ms=$(ms_since $t)
  t=$(now_ns); q2=$(myq "SELECT amount_cents FROM orders WHERE order_id=424242;") || return 1; q2ms=$(ms_since $t)
  t=$(now_ns); q3=$(myq "SELECT COUNT(*) FROM orders WHERE amount_cents>500000;") || return 1; q3ms=$(ms_since $t)
  echo "LANE=mysql LOAD_MS=$load_ms Q1_MS=$q1ms Q2_MS=$q2ms Q3_MS=$q3ms ANSWER_HASH=$(hash_answer "$q1|$q2|$q3")"
}
run_mysql || echo "LANE=mysql status=FAILED reason=query-error(logs: docker logs $C)"
