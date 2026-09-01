#!/usr/bin/env bash
# questdb lane -- orders workload, equal-answer to postgres. QuestDB has NO client tools in-container:
# drive it from the HOST over the REST API (:9000), load via POST /imp, query via GET /exec + jq.
set -uo pipefail; here="$(cd "$(dirname "$0")"&&pwd)"; . "$here/_common.sh"
DATA="${ARENA_DATA:?}"; IMG="${QDB_IMAGE:-questdb/questdb:8.1.1}"; C="arena_qdb_$$"
command -v curl >/dev/null 2>&1 || { echo "SKIP questdb reason=host-missing-curl fix=[install curl]"; exit 0; }
command -v jq   >/dev/null 2>&1 || { echo "SKIP questdb reason=host-missing-jq fix=[install jq]"; exit 0; }
st="$(arena_docker_status)"; [ "$st" = ok ] || { echo "SKIP questdb reason=docker-$st fix=[$(arena_docker_fix "$st")]"; exit 0; }
docker rm -f "$C" >/dev/null 2>&1 || true
trap 'docker rm -f "$C" >/dev/null 2>&1 || true' EXIT INT TERM
if ! docker run -d --rm --label "$ARENA_LABEL=1" --name "$C" "$IMG" >/tmp/arena_qdb_$$.err 2>&1; then
  echo "SKIP questdb reason=image-pull-or-start-failed detail=[$(tr -d '\n' </tmp/arena_qdb_$$.err | tail -c 90)] fix=[docker pull $IMG]"; rm -f /tmp/arena_qdb_$$.err; exit 0
fi
rm -f /tmp/arena_qdb_$$.err
IP="$(docker inspect -f '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' "$C" 2>/dev/null)"
[ -n "$IP" ] || { echo "SKIP questdb reason=no-container-ip(logs: docker logs $C)"; exit 0; }
BASE="http://$IP:9000"
ready=0; for i in $(seq 1 60); do curl -sfG "$BASE/exec" --data-urlencode "query=SELECT 1" >/dev/null 2>&1 && { ready=1; break; }; docker ps -q --no-trunc | grep -q "$(docker inspect -f '{{.Id}}' "$C" 2>/dev/null)" || break; sleep 1; done
[ "$ready" = 1 ] || { echo "SKIP questdb reason=engine-not-ready-in-60s(logs: docker logs $C)"; exit 0; }
qexec(){ curl -sG "$BASE/exec" --data-urlencode "query=$1"; }
run_qdb(){
  local t; t=$(now_ns)
  local imp; imp=$(curl -s -F schema='[{"name":"order_id","type":"LONG"},{"name":"customer_id","type":"LONG"},{"name":"product_id","type":"LONG"},{"name":"amount_cents","type":"LONG"},{"name":"ts","type":"LONG"}]' -F data=@"$DATA/orders.csv" "$BASE/imp?name=orders&overwrite=true&fmt=json")
  echo "$imp" | jq -e '.status=="OK"' >/dev/null 2>&1 || return 1
  local load_ms; load_ms=$(ms_since $t)
  local q1 q2 q3 q1ms q2ms q3ms
  t=$(now_ns); q1=$(qexec "SELECT concat(product_id, ':', s, ':', c) v FROM (SELECT product_id, sum(amount_cents) s, count() c FROM orders ORDER BY s DESC, product_id ASC LIMIT 10)" | jq -r '.dataset[][0]' | tr '\n' ';') || return 1; q1ms=$(ms_since $t)
  t=$(now_ns); q2=$(qexec "SELECT amount_cents FROM orders WHERE order_id=424242" | jq -r 'if (.dataset|length)>0 then (.dataset[0][0]|tostring) else empty end') || return 1; q2ms=$(ms_since $t)
  t=$(now_ns); q3=$(qexec "SELECT count() FROM orders WHERE amount_cents>500000" | jq -r '.dataset[0][0]|tostring') || return 1; q3ms=$(ms_since $t)
  echo "LANE=questdb LOAD_MS=$load_ms Q1_MS=$q1ms Q2_MS=$q2ms Q3_MS=$q3ms ANSWER_HASH=$(hash_answer "$q1|$q2|$q3")"
}
run_qdb || echo "LANE=questdb status=FAILED reason=query-error(logs: docker logs $C)"
