#!/usr/bin/env bash
# ############################################################################################
# # STREAMING domain (9) -- KAFKA incumbent lane.  Image: apache/kafka (KRaft, single broker). #
# # NOTE: no `set -e` -- a lane must NEVER abort the board. It isolates its own failure into a  #
# # single STATUS=skip/dnf line and lets run_all_domains.sh continue to the next engine.        #
# #                                                                                            #
# # The streaming workload, EXACTLY what the semurg_stream reference expresses on the substrate: #
# #   PRODUCE : stream a deterministic ordered log (key,seq,value) INTO Kafka -- the exact write #
# #             path a stream producer drives. Driven at MAX PRODUCERS: N parallel console-      #
# #             producers (N = cores) fanning into an N-partition topic. Timed = LOAD_MS.        #
# #   REPLAY  : consume the WHOLE topic back at MAX CONSUMERS: one console-consumer PER          #
# #             partition, each draining its partition EXACTLY (--max-messages = high-watermark, #
# #             no idle wait). Timed = QUERY_MS. The replayed set is folded into the histograms. #
# #                                                                                            #
# # EQUAL-ANSWER = the EXACT REPLAYED SET, order-independent -- byte-identical to semurg_stream: #
# #   value histogram (value % G, G buckets) | per-key histogram (key % KEYS, KEYS buckets) |    #
# #   total count, then sha256(...)[:32]. Two engines that replay the SAME stream produce the    #
# #   SAME hash regardless of partition/arrival order -- a no-loss / no-dup / no-reorder proof.  #
# #   The dataset is the IDENTICAL deterministic LCG + defaults semurg_stream uses (same knob    #
# #   names), so the hashes match bit-for-bit when the round-trip is lossless.                   #
# #                                                                                            #
# # ROBUST: awk / docker / image / broker each gated -> a clean STATUS=skip (never a crash). A   #
# # lossy round-trip -> STATUS=dnf with the exact counts. Emits ONE machine line the orchestrator#
# # parses:  LANE=kafka STATUS=ok LOAD_MS=.. QUERY_MS=.. PRODUCE_RPS=.. CONSUME_RPS=.. ANSWER=<h>#
# ############################################################################################
set -uo pipefail
here="$(cd "$(dirname "$0")"&&pwd)"; . "$here/_common.sh"

# ---- knobs: IDENTICAL names + defaults to semurg_stream.sh so both lanes generate the SAME stream ----
ROWS="${SEMURG_STREAM_ROWS:-2000000}"          # ordered log length (seq 1..ROWS, strictly increasing)
KEYS="${SEMURG_STREAM_KEYS:-100}"              # routing/partition keys 1..KEYS (per-key rollup buckets)
VALUE_MOD="${SEMURG_STREAM_VALUE_MOD:-10000}"  # payload magnitude 1..VALUE_MOD
G="${STREAM_GBUCKETS:-64}"                      # value histogram buckets (value % G)
IMG="${KAFKA_IMAGE:-apache/kafka:3.8.0}"
NPROC="$(nproc 2>/dev/null || echo 4)"
N="${KAFKA_PARTS:-$NPROC}"                      # partitions == producers == consumers = MAX concurrency
[ "$N" -ge 1 ] 2>/dev/null || N=4

SCRATCH="${ARENA_DATA:-${STREAM_SCRATCH:-${TMPDIR:-/tmp}/arena_kafka}}"; mkdir -p "$SCRATCH"
D="$SCRATCH/kafka_stream_$$"; rm -rf "$D" 2>/dev/null || true; mkdir -p "$D"

# ---- gates: awk + docker (each a clean SKIP, never a crash) ----
command -v awk >/dev/null 2>&1 || { echo "LANE=kafka STATUS=skip REASON=awk-not-available"; rm -rf "$D"; exit 0; }
st="$(arena_docker_status)"
[ "$st" = ok ] || { echo "LANE=kafka STATUS=skip REASON=docker-$st([$(arena_docker_fix "$st")])"; rm -rf "$D"; exit 0; }

C="arena_kafka_$$"; K=/opt/kafka/bin; B=localhost:9092; TOPIC=arena-stream  # hyphen: avoids the '_'/'.' metric-collision WARNING
docker rm -f "$C" >/dev/null 2>&1 || true                                   # reap any stale same-name container
trap 'docker rm -f "$C" >/dev/null 2>&1 || true; rm -rf "$D" 2>/dev/null || true' EXIT INT TERM

# no published host port: every Kafka CLI call goes through `docker exec` -> no host port collision.
if ! docker run -d --rm --label "$ARENA_LABEL=1" --name "$C" "$IMG" >"$D/run.err" 2>&1; then
  echo "LANE=kafka STATUS=skip REASON=image-pull-or-start-failed([$(tr -d '\n' <"$D/run.err" | tail -c 90)];try:docker pull $IMG)"; exit 0
fi

# ---- 1) stand up the stream deterministically into N balanced chunks (same LCG/knobs as semurg_stream).
#      one pass writes row i (round-robin) to chunk (i-1)%N -> N near-equal producer feeds, no `split` dep.
#      draw from the HIGH bits (int(s/256)%m) -- the glibc-style LCG has weak low-order bits. ----
awk -v n="$ROWS" -v nkey="$KEYS" -v vmod="$VALUE_MOD" -v np="$N" -v dir="$D" 'BEGIN{
  s=1234567;
  for(i=1;i<=n;i++){
    s=(s*1103515245+12345)%2147483648; key=1 + int(s/256)%nkey;
    s=(s*1103515245+12345)%2147483648; val=1 + int(s/256)%vmod;
    print key","i","val > (dir "/chunk_" ((i-1)%np));
  }
}' 2>/dev/null || { echo "LANE=kafka STATUS=skip REASON=data-gen-failed"; exit 0; }

# ---- 2) create the N-partition topic (partitions = the parallel replay lanes). This RETRY LOOP doubles
#         as broker readiness: the KRaft controller accepting a create is the true "ready" signal (a bare
#         topics --list can pass while the controller quorum is still forming), so we retry up to 90s. ----
created=0
for i in $(seq 1 90); do
  if docker exec "$C" "$K/kafka-topics.sh" --bootstrap-server "$B" --create --topic "$TOPIC" \
       --partitions "$N" --replication-factor 1 >/dev/null 2>&1; then created=1; break; fi
  docker inspect -f '{{.State.Running}}' "$C" 2>/dev/null | grep -q true || break   # container died -> stop waiting
  sleep 1
done
[ "$created" = 1 ] || { echo "LANE=kafka STATUS=skip REASON=broker-not-ready-in-90s(logs:docker logs $C)"; exit 0; }

# ---- 3) PRODUCE at MAX PRODUCERS: N parallel console-producers, one per chunk (timed = LOAD_MS) ----
t0=$(now_ns)
for p in $(seq 0 $((N-1))); do
  [ -s "$D/chunk_$p" ] || continue
  ( timeout 600 docker exec -i "$C" "$K/kafka-console-producer.sh" --bootstrap-server "$B" --topic "$TOPIC" \
      --batch-size 131072 --producer-property linger.ms=100 --producer-property acks=1 \
      --producer-property compression.type=none < "$D/chunk_$p" ) &
done
wait
produce_ms=$(ms_since "$t0"); [ "$produce_ms" -ge 1 ] 2>/dev/null || produce_ms=1

# ---- verify PRODUCE completeness via per-partition high watermarks (no-loss proof, pre-replay) ----
if ! docker exec "$C" "$K/kafka-get-offsets.sh" --bootstrap-server "$B" --topic "$TOPIC" >"$D/offs" 2>/dev/null; then
  docker exec "$C" "$K/kafka-run-class.sh" kafka.tools.GetOffsetShell --bootstrap-server "$B" --topic "$TOPIC" >"$D/offs" 2>/dev/null || true
fi
produced=$(awk -F: '{s+=$3} END{print s+0}' "$D/offs" 2>/dev/null)
if [ "${produced:-0}" != "$ROWS" ]; then
  echo "LANE=kafka STATUS=dnf REASON=produce-count-mismatch(expected=$ROWS produced=${produced:-0})"; exit 0
fi

# ---- 4) REPLAY at MAX CONSUMERS: one console-consumer PER partition, draining it EXACTLY (timed=QUERY_MS).
#      --max-messages = that partition's high-watermark -> each consumer stops the instant its partition is
#      drained (no idle-timeout padding), so QUERY_MS is honest consume wall time and the count is exact. ----
t1=$(now_ns)
while IFS=: read -r topic p off; do
  [ "${off:-0}" -gt 0 ] 2>/dev/null || { : > "$D/out_$p"; continue; }
  ( timeout 600 docker exec "$C" "$K/kafka-console-consumer.sh" --bootstrap-server "$B" --topic "$TOPIC" \
      --partition "$p" --offset earliest --max-messages "$off" 2>/dev/null > "$D/out_$p" ) &
done < "$D/offs"
wait
consume_ms=$(ms_since "$t1"); [ "$consume_ms" -ge 1 ] 2>/dev/null || consume_ms=1

cat "$D"/out_* > "$D/replayed.csv" 2>/dev/null
replayed=$(wc -l < "$D/replayed.csv" 2>/dev/null | tr -d ' ')
if [ "${replayed:-0}" != "$ROWS" ]; then
  echo "LANE=kafka STATUS=dnf REASON=replay-count-mismatch(expected=$ROWS replayed=${replayed:-0})"; exit 0
fi

# ---- 5) EQUAL-ANSWER over the REPLAYED set (order-independent; byte-identical to semurg_stream).
#      value hist = value%G (G buckets, order 0..G-1) | per-key hist = key%KEYS (KEYS buckets) | total.
#      printf (NO trailing newline) so sha256 matches the substrate's :crypto.hash of the same string. ----
answer=$(awk -F, -v g="$G" -v kb="$KEYS" '
  { v=$3%g; vh[v]++; k=$1%kb; kh[k]++; n++ }
  END{ s=""; for(i=0;i<g;i++)  s=s (i>0?",":"") (vh[i]+0);
       s=s "|"; for(i=0;i<kb;i++) s=s (i>0?",":"") (kh[i]+0);
       s=s "|" n; printf "%s", s }' "$D/replayed.csv" | sha256sum | cut -c1-32)

produce_rps=$(awk -v n="$ROWS" -v ms="$produce_ms" 'BEGIN{printf "%d", n/(ms/1000.0)}')
consume_rps=$(awk -v n="$ROWS" -v ms="$consume_ms" 'BEGIN{printf "%d", n/(ms/1000.0)}')

echo "LANE=kafka STATUS=ok LOAD_MS=$produce_ms QUERY_MS=$consume_ms PRODUCE_RPS=$produce_rps CONSUME_RPS=$consume_rps ANSWER=$answer"
