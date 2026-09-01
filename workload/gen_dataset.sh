#!/usr/bin/env bash
# Deterministic dataset generator (no deps). Same seed -> byte-identical CSV on any machine, so every
# engine ingests the SAME data and the equal-answer check is meaningful. Cols: order_id,customer_id,
# product_id,amount_cents,ts.  Rows via $SEMURG_ARENA_ROWS (default 200000).
set -euo pipefail
ROWS="${SEMURG_ARENA_ROWS:-200000}"
OUT="${1:-orders.csv}"
awk -v n="$ROWS" 'BEGIN{
  s=1234567;  # LCG seed
  printf "order_id,customer_id,product_id,amount_cents,ts\n";
  for(i=1;i<=n;i++){
    s=(s*1103515245+12345)%2147483648; cust=1+ s%50000;
    s=(s*1103515245+12345)%2147483648; prod=1+ s%2000;
    s=(s*1103515245+12345)%2147483648; amt=1+ s%1000000;
    s=(s*1103515245+12345)%2147483648; ts=1700000000 + s%31536000;
    printf "%d,%d,%d,%d,%d\n", i, cust, prod, amt, ts;
  }
}' > "$OUT"
echo "generated $OUT rows=$ROWS sha=$(sha256sum "$OUT" | cut -c1-16)"
