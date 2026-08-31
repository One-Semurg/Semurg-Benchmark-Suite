#!/usr/bin/env bash
# Deterministic market-tick generator (no dependencies beyond awk). Produces, into $TS_DIR:
#   trades.csv   -- header ts,sym,px,sz     (a trade print per row)
#   quotes.csv   -- header ts,sym,bid,ask   (the prevailing quote, same instant)
# Both are globally sorted by ts (ISO-8601, within one trading day), so QuestDB's designated-timestamp
# import + ASOF JOIN are valid. Prices are a per-symbol seeded random walk => byte-identical on every run.
#
# Usage: TS_DIR=/tmp/ticks NROWS=100000 NSYM=100 SEED=1 ./ticks.sh
set -euo pipefail
DIR="${TS_DIR:-/tmp/ticks}"
NROWS="${NROWS:-100000}"
NSYM="${NSYM:-100}"
SEED="${SEED:-1}"
mkdir -p "$DIR"

awk -v nrows="$NROWS" -v nsym="$NSYM" -v seed="$SEED" -v tf="$DIR/trades.csv" -v qf="$DIR/quotes.csv" '
function rnd(){ s=(s*1103515245+12345)%2147483648; return s/2147483648.0 }
function iso(us,   sod,u,h,m,sec){ sod=int(us/1000000); u=us%1000000; h=int(sod/3600); m=int((sod%3600)/60); sec=sod%60
  return sprintf("2026-01-05T%02d:%02d:%02d.%06dZ", h, m, sec, u) }
BEGIN{
  s = seed % 2147483647; if (s<=0) s+=2147483646
  print "ts,sym,px,sz"   > tf
  print "ts,sym,bid,ask" > qf
  for(i=0;i<nsym;i++) px[i] = 100.0 + i*0.5          # per-symbol starting price
  start_us = 34200000000                              # 09:30:00.000000 as microseconds-of-day
  span_us  = 23400000000                              # 6.5h trading window
  step = (nrows>0) ? int(span_us / nrows) : 1; if (step<1) step=1
  for(r=0; r<nrows; r++){
    ts = iso(start_us + r*step)
    y  = r % nsym
    px[y] += (rnd()-0.5)*0.10                          # random-walk the price
    if (px[y] < 1) px[y] = 1
    sz = 1 + int(rnd()*1000)
    spread = 0.01 + rnd()*0.05
    printf "%s,SYM%03d,%.4f,%d\n",   ts, y, px[y], sz               >> tf
    printf "%s,SYM%03d,%.4f,%.4f\n", ts, y, px[y]-spread, px[y]+spread >> qf
  }
}' </dev/null

echo "wrote $DIR/trades.csv + $DIR/quotes.csv ($NROWS rows, $NSYM symbols)  [seed=$SEED, deterministic]"
