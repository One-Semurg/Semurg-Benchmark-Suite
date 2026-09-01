#!/usr/bin/env bash
set -euo pipefail; here="$(cd "$(dirname "$0")"&&pwd)"; . "$here/_common.sh"
DATA="${ARENA_DATA:?}"; DUCK="${DUCKDB_BIN:-duckdb}"
command -v "$DUCK" >/dev/null || { echo "SKIP duckdb reason=duckdb-binary-not-found(set DUCKDB_BIN or install from duckdb.org)"; exit 0; }
DB="$(mktemp -d)/a.duckdb"; trap 'rm -rf "$(dirname "$DB")"' EXIT
sql(){ "$DUCK" "$DB" -noheader -list -c "$1"; }
t=$(now_ns)
sql "CREATE TABLE orders AS SELECT * FROM read_csv_auto('$DATA/orders.csv');" >/dev/null
load_ms=$(ms_since $t)
t=$(now_ns); q1=$(sql "SELECT product_id||':'||SUM(amount_cents)||':'||COUNT(*) FROM orders GROUP BY product_id ORDER BY SUM(amount_cents) DESC, product_id ASC LIMIT 10;" | tr '\n' ';'); q1ms=$(ms_since $t)
t=$(now_ns); q2=$(sql "SELECT amount_cents FROM orders WHERE order_id=424242;"); q2ms=$(ms_since $t)
t=$(now_ns); q3=$(sql "SELECT COUNT(*) FROM orders WHERE amount_cents>500000;"); q3ms=$(ms_since $t)
echo "LANE=duckdb LOAD_MS=$load_ms Q1_MS=$q1ms Q2_MS=$q2ms Q3_MS=$q3ms ANSWER_HASH=$(hash_answer "$q1|$q2|$q3")"
