#!/usr/bin/env bash
# timeseries_run.sh -- DOMAIN 3 (TIME-SERIES) head-to-head: TimescaleDB vs QuestDB on the SAME
# deterministic tick/sensor stream, EQUAL-ANSWER gated against an INDEPENDENT awk reference (not one of
# the engines -- a lane counts ONLY if its ANSWER_HASH matches the reference). The canonical query is the
# time-series DOWNSAMPLE: per-bucket count/sum/min/max (Q1), a per-series rollup (Q2), and a range-window
# count (Q3), all integer-exact so the hash is bit-identical across engines with no timezone/float drift.
#
# Memgraph/TigerGraph-style DeWitt engines are NOT here; kdb+ is DeWitt/licence-restricted and lives under
# lanes/licensed/ (run it yourself, local-only, via the licensed driver -- its numbers are NEVER published).
set -uo pipefail
TS_MISMATCH=0
HERE="$(cd "$(dirname "$0")"&&pwd)"
LANES="${TS_LANES_DIR:-$HERE/lanes}"; WORK="${TS_WORK_DIR:-$HERE/workload}"
SCRATCH="${TS_SCRATCH:-${TMPDIR:-/tmp}/arena_timeseries}"
TO="${LANE_TIMEOUT:-600}"
. "$LANES/_common.sh"
mkdir -p "$SCRATCH"

ROWS="${SEMURG_TS_ROWS:-500000}"; export SEMURG_TS_ROWS="$ROWS"
B="${TS_BUCKET:-3600}"; DEV="${Q2_DEVICE:-}"
DATA="$SCRATCH/ts_data.csv"

echo "############################################################################################"
echo "# TIME-SERIES head-to-head: TimescaleDB (Apache-2.0 core) vs QuestDB (Apache-2.0)           #"
echo "# -- equal-answer time-bucket DOWNSAMPLE. kdb+ is DeWitt/licence-restricted: run it via the #"
echo "# licensed driver (lanes/licensed/timeseries_run_licensed.sh), NEVER on this public board.  #"
echo "############################################################################################"
bash "$WORK/gen_timeseries.sh" "$DATA" >/dev/null || { echo "FATAL: dataset generation failed"; exit 1; }

# derive the range window from the ACTUAL data (scale-robust): [min + 1/4 span, min + 1/2 span).
read -r WLO WHI < <(awk -F, 'NR>1{if(mn==""||$2<mn)mn=$2; if($2>mx)mx=$2} END{sp=mx-mn; printf "%d %d\n", mn+int(sp/4), mn+int(sp/2)}' "$DATA")
# derive Q2's device from the ACTUAL data (the busiest device, smallest id on a tie) unless pinned -> the
# per-series rollup always hits a device that exists (robust to any DEVICES / generator distribution).
[ -n "$DEV" ] || DEV="$(awk -F, 'NR>1{c[$1]++} END{best=-1;bid=0; for(k in c){ if(c[k]>best || (c[k]==best && k<bid)){best=c[k];bid=k} } print bid}' "$DATA")"
export TS_DATA="$DATA" TS_BUCKET="$B" WIN_LO="$WLO" WIN_HI="$WHI" Q2_DEVICE="$DEV"

# ---- INDEPENDENT reference (awk, not an engine) : same q1|q2|q3 string every lane hashes ----------
ref_answer(){
  local q1 q2 q3
  q1="$(awk -F, -v B="$B" '
    NR>1{ b=$2-$2%B; c[b]++; s[b]+=$3; if(mn[b]==""||$3<mn[b])mn[b]=$3; if($3>mx[b])mx[b]=$3 }
    END{ for(k in c) printf "%d %d %d %d %d\n", k, c[k], s[k], mn[k], mx[k] }' "$DATA" \
    | sort -n -k1,1 | awk '{printf "%d:%d:%d:%d:%d;", $1,$2,$3,$4,$5}')"
  q2="$(awk -F, -v D="$DEV" '
    NR>1 && $1==D { c++; s+=$3; if(mn==""||$3<mn)mn=$3; if($3>mx)mx=$3 }
    END{ printf "%d:%d:%d:%d", c+0, s+0, mn+0, mx+0 }' "$DATA")"
  q3="$(awk -F, -v LO="$WLO" -v HI="$WHI" 'NR>1 && $2>=LO && $2<HI{c++} END{printf "%d", c+0}' "$DATA")"
  printf '%s' "$q1|$q2|$q3"
}
REFSTR="$(ref_answer)"; REF="$(hash_answer "$REFSTR")"
NBUCK="$(awk -F';' '{print NF-1}' <<<"${REFSTR%%|*}")"

echo "== workload: DOWNSAMPLE $ROWS readings into $B-second buckets ($NBUCK buckets); Q2 device=$DEV; Q3 window=[$WLO,$WHI) =="
echo "   independent reference (awk) ANSWER_HASH = $REF"
printf "   %-16s %-10s %-9s %-9s %-9s %-13s\n" engine load_ms q1_ms q2_ms q3_ms equal-answer

run(){ timeout -k 10 "${TO}s" bash "$LANES/$1.sh" 2>/dev/null | grep -m1 '^LANE=' || echo "LANE=$1 STATUS=dnf REASON=timed-out-or-errored"; }
for lane in timescale_ts questdb_ts; do
  line="$(run "$lane")"
  h=$(sed -n 's/.*ANSWER_HASH=\([0-9a-f]*\).*/\1/p' <<<"$line")
  lm=$(sed -n 's/.*LOAD_MS=\([0-9]*\).*/\1/p' <<<"$line"); q1=$(sed -n 's/.*Q1_MS=\([0-9]*\).*/\1/p' <<<"$line")
  q2=$(sed -n 's/.*Q2_MS=\([0-9]*\).*/\1/p' <<<"$line"); q3=$(sed -n 's/.*Q3_MS=\([0-9]*\).*/\1/p' <<<"$line")
  if [ -n "$h" ]; then
    if [ "$h" = "$REF" ]; then eq="OK"; else eq="MISMATCH"; TS_MISMATCH=1; fi
    printf "   %-16s %-10s %-9s %-9s %-9s %-13s\n" "$lane" "${lm:-–}" "${q1:-–}" "${q2:-–}" "${q3:-–}" "$eq"
  else
    printf "   %-16s %-10s %-9s %-9s %-9s %-13s\n" "$lane" "–" "–" "–" "–" "$(sed -n 's/.*reason=\(.*\)/\1/p;s/.*REASON=\(.*\)/\1/p' <<<"$line")"
  fi
done

echo
echo "Reading it: 'equal-answer OK' means the engine returned the SAME per-bucket downsample + rollups as"
echo "the independent awk reference. A lane that disagrees is MISMATCH and is NEVER counted. Buckets use"
echo "(ts - ts%$B) integer arithmetic == time_bucket()/SAMPLE BY in exact-integer form, so the answer hash"
echo "is bit-identical across engines (no timezone or float divergence)."
rm -rf "$SCRATCH" 2>/dev/null || true
[ "${TS_MISMATCH:-0}" = 0 ] || { echo; echo "PARITY FAIL: an engine disagreed with the equal-answer reference (MISMATCH above). Exiting non-zero."; exit 3; }
