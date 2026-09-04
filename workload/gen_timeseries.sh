#!/usr/bin/env bash
# Deterministic TIME-SERIES dataset generator (no deps). Same seed -> byte-identical CSV on any machine,
# so every engine ingests the SAME data and the equal-answer downsample check is meaningful.
# Cols: device_id,ts,value  (all integers -> exact SUM/MIN/MAX/COUNT, no float divergence across engines).
#   * ts is STRICTLY INCREASING (cumulative deterministic increment 1..TS_STEP_MAX) -> a real ordered
#     tick/sensor stream (QuestDB designated-timestamp friendly; every engine sees the same ordering).
#   * device_id in 1..DEVICES ; value in 1..VALUE_MOD.
# Rows via $SEMURG_TS_ROWS (default 500000).  Reuses the same LCG shape as workload/gen_dataset.sh.
set -euo pipefail
ROWS="${SEMURG_TS_ROWS:-500000}"
DEVICES="${SEMURG_TS_DEVICES:-100}"
TS_START="${SEMURG_TS_START:-1700000000}"
TS_STEP_MAX="${SEMURG_TS_STEP_MAX:-20}"
VALUE_MOD="${SEMURG_TS_VALUE_MOD:-10000}"
OUT="${1:-ts_data.csv}"
awk -v n="$ROWS" -v ndev="$DEVICES" -v tstart="$TS_START" -v stepmax="$TS_STEP_MAX" -v vmod="$VALUE_MOD" 'BEGIN{
  s=1234567;            # LCG seed (same constants as gen_dataset.sh)
  ts=tstart;
  printf "device_id,ts,value\n";
  # NOTE: this glibc-style LCG has WEAK low-order bits (s%k cycles short, misses residues), so we draw
  # from the HIGH bits (int(s/256)%k) -> every device_id 1..DEVICES actually appears + values spread.
  for(i=1;i<=n;i++){
    s=(s*1103515245+12345)%2147483648; dev=1 + int(s/256)%ndev;
    s=(s*1103515245+12345)%2147483648; step=1 + int(s/256)%stepmax; ts=ts+step;   # strictly increasing timestamp
    s=(s*1103515245+12345)%2147483648; val=1 + int(s/256)%vmod;
    printf "%d,%d,%d\n", dev, ts, val;
  }
}' > "$OUT"
echo "generated $OUT rows=$ROWS devices=$DEVICES sha=$(sha256sum "$OUT" | cut -c1-16)"
