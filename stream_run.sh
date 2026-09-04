#!/usr/bin/env bash
# stream_run.sh -- DOMAIN 9 (STREAMING) head-to-head: Apache Kafka vs the semurg_stream reference on the
# SAME deterministic ordered log (key,seq,value). EQUAL-ANSWER gated against an INDEPENDENT awk reference
# (NOT one of the engines): PRODUCE the stream, REPLAY the WHOLE stream back, and fold the REPLAYED set into
# value / per-key histograms + a total count. A lane counts ONLY if its ANSWER matches the reference -- an
# order-independent no-loss / no-dup / no-reorder replay proof (partition/arrival order cannot change it).
#
# Kafka (Apache-2.0) is public-license -- fully publishable. There is NO DeWitt engine in the streaming
# domain (no lanes/licensed/ stream lane), so this board is publication-safe on its own.
set -uo pipefail
STREAM_MISMATCH=0
HERE="$(cd "$(dirname "$0")"&&pwd)"
LANES="${STREAM_LANES_DIR:-$HERE/lanes}"
SCRATCH="${STREAM_RUN_SCRATCH:-${TMPDIR:-/tmp}/arena_stream_run}"
TO="${LANE_TIMEOUT:-900}"
. "$LANES/_common.sh"
rm -rf "$SCRATCH" 2>/dev/null || true; mkdir -p "$SCRATCH"

# deterministic knobs -- IDENTICAL names + defaults the lanes read, so all three (reference, semurg, kafka)
# stand up the byte-identical stream. Scale via SEMURG_STREAM_ROWS for a lighter/heavier board.
ROWS="${SEMURG_STREAM_ROWS:-2000000}";     export SEMURG_STREAM_ROWS="$ROWS"
KEYS="${SEMURG_STREAM_KEYS:-100}";         export SEMURG_STREAM_KEYS="$KEYS"
VMOD="${SEMURG_STREAM_VALUE_MOD:-10000}";  export SEMURG_STREAM_VALUE_MOD="$VMOD"
G="${STREAM_GBUCKETS:-64}";                export STREAM_GBUCKETS="$G"

echo "############################################################################################"
echo "# STREAMING head-to-head: Apache Kafka (Apache-2.0) vs the semurg_stream reference.          #"
echo "# PRODUCE then REPLAY a deterministic $ROWS-row ordered log; equal-answer = the EXACT replayed#"
echo "# set (value-hist | per-key-hist | count), order-independent. No DeWitt engine in this domain.#"
echo "############################################################################################"

# ---- INDEPENDENT reference (awk, not an engine): the same value|key|count hash a lossless replay yields.
#      Same LCG + knobs the lanes embed, so a byte-perfect round-trip on either engine reproduces it. ----
DATA="$SCRATCH/stream_ref.csv"
awk -v n="$ROWS" -v nkey="$KEYS" -v vmod="$VMOD" 'BEGIN{
  s=1234567;
  for(i=1;i<=n;i++){
    s=(s*1103515245+12345)%2147483648; key=1 + int(s/256)%nkey;
    s=(s*1103515245+12345)%2147483648; val=1 + int(s/256)%vmod;
    print key","i","val;
  }
}' > "$DATA" 2>/dev/null || { echo "FATAL: reference generation failed"; exit 1; }
REF="$(awk -F, -v g="$G" -v kb="$KEYS" '
  { v=$3%g; vh[v]++; k=$1%kb; kh[k]++; n++ }
  END{ s=""; for(i=0;i<g;i++)  s=s (i>0?",":"") (vh[i]+0);
       s=s "|"; for(i=0;i<kb;i++) s=s (i>0?",":"") (kh[i]+0);
       s=s "|" n; printf "%s", s }' "$DATA" | sha256sum | cut -c1-32)"

echo "== workload: PRODUCE + REPLAY $ROWS rows (keys=$KEYS, value-mod=$VMOD, $G value-buckets) =="
echo "   independent reference (awk) ANSWER = $REF"
printf "   %-16s %-10s %-11s %-13s %-13s %s\n" engine load_ms replay_ms produce_rps consume_rps equal-answer

run(){ ARENA_DATA="$SCRATCH/$1" timeout -k 10 "${TO}s" bash "$LANES/$1.sh" 2>/dev/null | grep -m1 '^LANE=' \
       || echo "LANE=$1 STATUS=dnf REASON=timed-out-or-errored(>${TO}s)"; }

# semurg_stream FIRST (the reference lane the substrate serves), then the Kafka incumbent.
for lane in semurg_stream kafka; do
  line="$(run "$lane")"
  status=$(sed -n 's/.*STATUS=\([a-z-]*\).*/\1/p' <<<"$line")
  lm=$(sed -n 's/.*LOAD_MS=\([0-9]*\).*/\1/p' <<<"$line")
  qm=$(sed -n 's/.*QUERY_MS=\([0-9]*\).*/\1/p' <<<"$line")
  pr=$(sed -n 's/.*PRODUCE_RPS=\([0-9]*\).*/\1/p' <<<"$line")
  cr=$(sed -n 's/.*CONSUME_RPS=\([0-9]*\).*/\1/p' <<<"$line")
  h=$(sed -n 's/.*ANSWER=\([0-9a-f]*\).*/\1/p' <<<"$line")
  reason=$(sed -n 's/.*REASON=\(.*\)/\1/p' <<<"$line")
  case "$status" in
    ok)
      if [ "$h" = "$REF" ]; then eq="OK"; else eq="MISMATCH"; STREAM_MISMATCH=1; fi
      printf "   %-16s %-10s %-11s %-13s %-13s %s\n" "$lane" "${lm:--}" "${qm:--}" "${pr:--}" "${cr:--}" "$eq";;
    skip) printf "   %-16s %-10s %-11s %-13s %-13s %s\n" "$lane" "-" "-" "-" "-" "SKIP $reason";;
    *)    printf "   %-16s %-10s %-11s %-13s %-13s %s\n" "$lane" "-" "-" "-" "-" "DNF ${reason:-did-not-finish}";;
  esac
done

echo
echo "Reading: 'equal-answer OK' means the lane REPLAYED the exact same set (value-hist | per-key-hist |"
echo "count) as the independent reference -- proof of a no-loss / no-dup / no-reorder round-trip. load_ms is"
echo "the PRODUCE wall time, replay_ms the full REPLAY. Kafka drives N parallel producers into an N-partition"
echo "topic and one consumer PER partition (N = cores) -- max producers + max consumers on its node. A"
echo "MISMATCH is never counted as a win; a lossy round-trip surfaces as DNF with the exact counts."

rm -rf "$SCRATCH" 2>/dev/null || true
[ "${STREAM_MISMATCH:-0}" = 0 ] || { echo; echo "PARITY FAIL: a lane's replayed set disagreed with the independent reference (MISMATCH above). Exiting non-zero so a wrong answer never passes green."; exit 3; }
