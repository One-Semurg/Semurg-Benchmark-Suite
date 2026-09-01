#!/usr/bin/env bash
set -euo pipefail; here="$(cd "$(dirname "$0")"&&pwd)"; . "$here/_common.sh"
DATA="${ARENA_DATA:?}"; DB="$(mktemp -d)/a.db"
command -v sqlite3 >/dev/null || { echo "SKIP sqlite reason=sqlite3-not-installed(apt-get install -y sqlite3)"; exit 0; }
t=$(now_ns)
sqlite3 "$DB" "CREATE TABLE orders(order_id INTEGER, customer_id INTEGER, product_id INTEGER, amount_cents INTEGER, ts INTEGER);"
sqlite3 "$DB" ".mode csv" ".import --skip 1 $DATA/orders.csv orders" >/dev/null 2>&1
sqlite3 "$DB" "CREATE INDEX IF NOT EXISTS i ON orders(order_id);" >/dev/null
load_ms=$(ms_since $t)
t=$(now_ns); q1=$(sqlite3 "$DB" "SELECT product_id||':'||SUM(amount_cents)||':'||COUNT(*) FROM orders GROUP BY product_id ORDER BY SUM(amount_cents) DESC, product_id ASC LIMIT 10;" | tr '\n' ';'); q1ms=$(ms_since $t)
t=$(now_ns); q2=$(sqlite3 "$DB" "SELECT amount_cents FROM orders WHERE order_id=424242;"); q2ms=$(ms_since $t)
t=$(now_ns); q3=$(sqlite3 "$DB" "SELECT COUNT(*) FROM orders WHERE amount_cents>500000;"); q3ms=$(ms_since $t)
echo "LANE=sqlite LOAD_MS=$load_ms Q1_MS=$q1ms Q2_MS=$q2ms Q3_MS=$q3ms ANSWER_HASH=$(hash_answer "$q1|$q2|$q3")"
rm -rf "$(dirname "$DB")"
