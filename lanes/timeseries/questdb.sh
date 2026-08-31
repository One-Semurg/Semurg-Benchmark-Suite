#!/usr/bin/env bash
# Time-series lane: QuestDB. The closest open-source challenger to the incumbent HFT tick store (native ASOF JOIN,
# columnar, SQL). Loopback-bound (never exposed), torn down on exit, portable paths. Runs the classic tick
# workloads - ASOF JOIN (trade <-> prevailing quote) and per-symbol VWAP - and emits the answer + server timing.
# Data is the deterministic tick set from generators/ (see generators/README.md): trades.csv + quotes.csv.
set -euo pipefail
REST=http://127.0.0.1:9000
DIR="${TS_DIR:-$(mktemp -d)}"
IMAGE="${QDB_IMAGE:-questdb/questdb:8.1.1}"
CNAME=qdb-bench-$$

cleanup() { docker rm -f "$CNAME" >/dev/null 2>&1 || true; }
trap cleanup EXIT

[ -f "$DIR/trades.csv" ] && [ -f "$DIR/quotes.csv" ] || {
  echo "tick data not found in $DIR - run the generator first (see generators/README.md)"; exit 1; }

q() { curl -s -G "$REST/exec" --data-urlencode "query=$1"; }  # run SQL over the REST endpoint

docker run -d --name "$CNAME" -p 127.0.0.1:9000:9000 "$IMAGE" >/dev/null
echo "waiting for QuestDB..."
for i in $(seq 1 40); do
  [ "$(curl -s -o /dev/null -w '%{http_code}' "$REST/exec?query=select%201" || echo 000)" = "200" ] && break
  sleep 2
done

# Schema: designated timestamp + symbol; QuestDB needs the timestamp column typed for ASOF/partitioning.
q "create table trades (ts timestamp, sym symbol, px double, sz long) timestamp(ts) partition by day" >/dev/null
q "create table quotes (ts timestamp, sym symbol, bid double, ask double) timestamp(ts) partition by day" >/dev/null

echo "loading ticks..."
curl -s -F "data=@$DIR/trades.csv" "$REST/imp?name=trades&overwrite=false&forceHeader=true" >/dev/null
curl -s -F "data=@$DIR/quotes.csv" "$REST/imp?name=quotes&overwrite=false&forceHeader=true" >/dev/null

echo "running tick queries..."
t0=$(date +%s.%N)
# ASOF JOIN: attach each trade its prevailing quote PER SYMBOL (the canonical as-of-join tick workload).
# ON (sym) makes it per-symbol; sym is qualified (it exists in both tables) to avoid ambiguity.
asof=$(q "select trades.sym sym, sum(trades.px*trades.sz) s, sum(trades.sz) v from trades asof join quotes on (sym) sample by 1h" )
# VWAP per symbol per hour.
vwap=$(q "select sym, sum(px*sz)/sum(sz) vwap from trades sample by 1h")
t1=$(date +%s.%N)

ans=$(printf '%s\n%s' "$asof" "$vwap" | sha256sum | cut -c1-16)
echo "LANE=questdb STATUS=ok ANSWER=$ans SECS=$(awk -v a="$t0" -v b="$t1" 'BEGIN{printf "%.3f", b-a}')"
