#!/usr/bin/env bash
# keyvalue_run.sh -- the KEY-VALUE head-to-head (domain 6): Semurg vs Redis vs RocksDB on the SAME
# deterministic 1M-key keyspace, with an EQUAL-ANSWER parity gate. All three engines are public-license
# (Redis 7.2 BSD/RSAL public-OK, RocksDB Apache-2.0/GPLv2, Semurg native) -- NO DeWitt engine (Dragonfly
# stays out of the public board), so this runner is fully publishable.
#
# THE KEYSET (byte-identical across engines): key = id, value(id) = 20-byte big-endian id == printf
# '%040x' id (40 lowercase hex chars). The canonical ANSWER = first 32 hex chars of sha256 over the
# concatenated lowercase-hex of the VALUES for ids 1..ANSWER_SAMPLE, in id order. An INDEPENDENT awk/
# printf reference computes that ground truth here; a lane counts ONLY if the ANSWER it derives from what
# the engine actually returns hashes equal to that reference. The reported metric is ROWS_PER_S = the
# warm (3rd-cycle) point-GET throughput -- the apples-to-apples KV number every engine reports.
#
# FAIRNESS (founder benchmark-fairness law): every incumbent is driven at MAX CONCURRENCY on its node --
# Redis with io-threads = all cores + many concurrent clients, RocksDB with all cores + both NVMe pipes,
# Semurg whole-cluster striped. Each lane prints its drive level (threads/clients/pipes) so it is never a
# hidden single-thread/single-disk under-drive.
#
# WHY A `for` LOOP (not `while read`): a Semurg lane runs the release `eval`, which DRAINS stdin. A
# `while read < <(...)` board would have its remaining lines eaten by that eval (the first eval-lane
# starves the rest). A static `for` list is immune, and each lane is additionally run with </dev/null so
# nothing an engine reads can perturb the runner. This is the same class the arena runner hardened.
set -uo pipefail
KV_MISMATCH=0
HERE="$(cd "$(dirname "$0")" && pwd)"
LANES="${KV_LANES_DIR:-$HERE/lanes}"
SCRATCH="${KV_SCRATCH:-${TMPDIR:-/tmp}/arena_keyvalue}"
TO="${LANE_TIMEOUT:-900}"
. "$LANES/_common.sh" 2>/dev/null || true

# dataset knobs (deterministic; same on any box). Defaults match the semurg_kv + redis lanes exactly so
# the answer is cross-engine identical; scale via KV_KEYS / KV_GET_SAMPLE / KV_ANSWER_SAMPLE.
KEYS="${KV_KEYS:-1000000}"
GSAMPLE="${KV_GET_SAMPLE:-200000}"
ASAMPLE="${KV_ANSWER_SAMPLE:-4096}"
export KV_KEYS="$KEYS" KV_GET_SAMPLE="$GSAMPLE" KV_ANSWER_SAMPLE="$ASAMPLE"

rm -rf "$SCRATCH"; mkdir -p "$SCRATCH"
echo "############################################################################################"
echo "# KEY-VALUE head-to-head: Semurg vs Redis (BSD/RSAL) vs RocksDB (Apache-2.0/GPLv2).          #"
echo "# All engines public-license (no DeWitt in this board): fully publishable. Equal-answer gated.#"
echo "############################################################################################"
echo "== deterministic keyspace: KEYS=$KEYS, value(id)=20-byte BE id, GET_SAMPLE=$GSAMPLE, ANSWER_SAMPLE=$ASAMPLE =="

# independent reference: sha256 over concat of printf '%040x' id for id 1..ASAMPLE (no separators, no
# trailing newline) -> first 32 hex. Byte-identical crypto to semurg_kv (:crypto.hash(:sha256, ahex)).
REF="$(printf '%040x' $(seq 1 "$ASAMPLE") | sha256sum | cut -c1-32)"
echo "   reference_answer_hash (independent printf+sha256) = $REF"
echo
printf "   %-14s %-9s %-15s %-15s %-11s %s\n" engine load_ms indiv_get/s multiget/s equal-ans note

# lanes: Semurg (native, self-skips if the release is not installed) + Redis + RocksDB (both docker,
# self-skip cleanly if docker/image is unavailable). The incumbent board runs on its own if Semurg is
# absent; Semurg lights up the moment the release is installed.
LANESET=""
[ -f "$LANES/semurg_kv.sh" ] && LANESET="$LANESET semurg_kv"
LANESET="$LANESET redis rocksdb"

semurg_warm=""
for lane in $LANESET; do
  [ -f "$LANES/$lane.sh" ] || { printf "   %-14s %-9s %-15s %-15s %-11s %s\n" "$lane" "-" "-" "-" "SKIP" "lane-script-absent(lanes/$lane.sh)"; continue; }
  raw="$( ARENA_DATA="$SCRATCH/$lane" KV_KEYS="$KEYS" KV_GET_SAMPLE="$GSAMPLE" KV_ANSWER_SAMPLE="$ASAMPLE" \
      timeout -k 10 "${TO}s" bash "$LANES/$lane.sh" </dev/null 2>/dev/null )"
  line="$(grep -m1 '^LANE=' <<<"$raw")"
  [ -n "$line" ] || line="LANE=$lane STATUS=dnf REASON=timed-out-or-errored(>${TO}s)"
  status=$(sed -n 's/.*STATUS=\([A-Za-z]*\).*/\1/p' <<<"$line" | tr 'A-Z' 'a-z')
  ans=$(sed -n 's/.*ANSWER=\([0-9a-f]*\).*/\1/p' <<<"$line")
  lm=$(sed -n 's/.*LOAD_MS=\([0-9]*\).*/\1/p' <<<"$line")
  warm=$(sed -n 's/.*ROWS_PER_S=\([0-9]*\).*/\1/p' <<<"$line")
  [ -n "$warm" ] || warm=$(sed -n 's/.*GET_RPS_C3=\([0-9]*\).*/\1/p' <<<"$line")
  # BATCHED (multiget / batch-the-crossing) throughput: Semurg BATCH_RPS_C3 (deep-QD one-crossing fan),
  # RocksDB BATCH_RPS_C3 (MultiGet). Engines with no batched form show "-". This is the fair batched-vs-
  # batched row pairing Semurg's native batch path against RocksDB MultiGet (never batch-vs-individual).
  batch=$(sed -n 's/.*BATCH_RPS_C3=\([0-9]*\).*/\1/p' <<<"$line")
  reason=$(sed -n 's/.*REASON=\(.*\)/\1/p' <<<"$line")
  pipes=$(sed -n 's/.*PIPES=\([0-9]*\).*/\1/p' <<<"$line"); thr=$(sed -n 's/.*THREADS=\([0-9]*\).*/\1/p' <<<"$line")
  case "$status" in
    ok)
      if [ "$ans" = "$REF" ]; then eq="OK"; else eq="MISMATCH"; KV_MISMATCH=1; fi
      note=""
      [ "$lane" = semurg_kv ] && semurg_warm="$warm"
      [ -n "$semurg_warm" ] && [ "$lane" != semurg_kv ] && [ -n "$warm" ] && [ "$warm" -gt 0 ] 2>/dev/null && \
        note="Semurg GET $(awk -v a="$semurg_warm" -v b="$warm" 'BEGIN{if(b>0)printf "%.2fx", a/b; else print "-"}') this lane"
      [ -n "$thr$pipes" ] && note="${note:+$note; }drive[threads=${thr:-na} pipes=${pipes:-na}]"
      printf "   %-14s %-9s %-15s %-15s %-11s %s\n" "$lane" "${lm:--}" "${warm:--}" "${batch:--}" "$eq" "$note";;
    skip) printf "   %-14s %-9s %-15s %-15s %-11s %s\n" "$lane" "-" "-" "-" "SKIP" "$reason";;
    *)    printf "   %-14s %-9s %-15s %-15s %-11s %s\n" "$lane" "-" "-" "-" "DNF" "${reason:-did-not-finish}";;
  esac
done

echo
echo "Reading it: 'equal-answer OK' means the lane's point-GETs returned the SAME values as the independent"
echo "reference (a disagreeing lane is MISMATCH and never counted). indiv_get/s = 3rd-cycle INDIVIDUAL"
echo "point-GET/s driven at MAX CONCURRENCY (Semurg octopus = N BEAM callers over id partitions; RocksDB ="
echo "N threads of db->Get). multiget/s = the BATCHED form (Semurg deep-QD one-crossing fetch_batch vs"
echo "RocksDB MultiGet; '-' = that engine lane has no batched number yet). Both rows are equal-answer gated"
echo "and driven at max concurrency on each node. A SKIP carries the exact fix; nothing here is ever faked."
rm -rf "$SCRATCH" 2>/dev/null || true
[ "${KV_MISMATCH:-0}" = 0 ] || { echo; echo "PARITY FAIL: a lane's values disagreed with the independent reference (MISMATCH above). Exiting non-zero so a wrong answer never passes green."; exit 3; }
