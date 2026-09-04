#!/usr/bin/env bash
# object_run.sh -- the OBJECT head-to-head: MinIO (docker:minio/minio, AGPLv3, public-OK) vs Semurg on
# the SAME deterministic small-object set. One board, equal-answer gated byte-for-byte.
#
#   Every lane PUTs then GET/STAT/LISTs the SAME deterministic object set (OBJ_N objects of OBJ_SIZE
#   bytes, AES-256-CTR keystream seeded by OBJ_SEED, obj-%08d.bin keys -- byte-identical on any box),
#   and each lane INDEPENDENTLY reproduces the equal-answer from the ACTUAL bytes it stored + served:
#     GET   = sha256( join_"\n"( "key<TAB>sha256hex(bytes_returned)" for sorted keys ) )[:32]
#     STAT  = integer sum of the size the engine reports for every object
#     LIST  = sha256( join_"\n"( sorted keys the engine enumerates ) )[:32]
#     ANSWER= sha256( GET "|" STAT "|" LIST )[:32]     -- the single gate value
#   A lane counts ONLY if its measured ANSWER matches the reference (the first OK lane establishes it).
#   A disagreeing lane is MISMATCH and never counted; a wrong answer never passes green.
#
#   Semurg stores objects as value node-chains on the substrate (served through the ONE conveyor); MinIO
#   stores them S3-native with single-node multi-drive ERASURE CODING across FOUR drives spread over BOTH
#   physical NVMe, and is driven at MAX CONCURRENCY ($(nproc) parallel client streams) -- fair max-node.
#
#   MinIO (AGPLv3) is public-license: this board is fully publishable. There is NO DeWitt engine in the
#   object domain and NO lanes/licensed/ object lane.
set -uo pipefail
OBJ_MISMATCH=0
HERE="$(cd "$(dirname "$0")"&&pwd)"
LANES="${OBJ_LANES_DIR:-$HERE/lanes}"
SCRATCH="${OBJ_SCRATCH:-${TMPDIR:-/tmp}/arena_object_run}"
TO="${LANE_TIMEOUT:-900}"
. "$LANES/_common.sh"
export LC_ALL=C

# dataset knobs (deterministic; identical on any box; passed to EVERY lane so all engines use one set).
N="${OBJ_N:-4000}"; SIZE="${OBJ_SIZE:-4096}"; SEED="${OBJ_SEED:-1234567}"; CYCLES="${OBJ_CYCLES:-3}"

rm -rf "$SCRATCH" 2>/dev/null; mkdir -p "$SCRATCH"
echo "############################################################################################"
echo "# OBJECT head-to-head: MinIO (docker:minio/minio, AGPLv3) vs Semurg on the SAME object set.   #"
echo "# PUT then GET/STAT/LIST, equal-answer gated byte-for-byte (content + size-sum + keyset).     #"
echo "# MinIO is public-license (no DeWitt clause in this domain): fully publishable.               #"
echo "############################################################################################"
echo "== object set: N=$N objects, SIZE=$SIZE bytes, SEED=$SEED, GET cycles=$CYCLES (cold/warm/warm) =="

# Semurg native lane runs when the release is installed; it skips cleanly otherwise so the incumbent
# board is always runnable on its own. Semurg first so it establishes the equal-answer reference.
LANESET="minio"
[ -f "$LANES/semurg_object.sh" ] && LANESET="semurg_object minio"

echo
printf "   %-16s %-9s %-9s %-9s %-9s %-13s %s\n" engine load_ms get_ms stat_ms list_ms equal-answer note
REF=""; minioq=""; semq=""
for lane in $LANESET; do
  raw="$( OBJ_N="$N" OBJ_SIZE="$SIZE" OBJ_SEED="$SEED" OBJ_CYCLES="$CYCLES" \
      ARENA_DATA="$SCRATCH/$lane" \
      timeout -k 10 "${TO}s" bash "$LANES/$lane.sh" 2>/dev/null )"
  line="$(grep -m1 '^LANE=' <<<"$raw")"
  skipl="$(grep -m1 -iE '^SKIP ' <<<"$raw")"
  if [ -z "$line" ] && [ -n "$skipl" ]; then
    reason="$(sed -n 's/^SKIP [^ ]* *reason=\(.*\)/\1/Ip' <<<"$skipl")"
    printf "   %-16s %-9s %-9s %-9s %-9s %-13s %s\n" "$lane" "-" "-" "-" "-" "SKIP" "$reason"; continue
  fi
  [ -n "$line" ] || line="LANE=$lane STATUS=dnf REASON=timed-out-or-errored(>${TO}s)"
  status=$(sed -n 's/.*STATUS=\([A-Za-z]*\).*/\1/p' <<<"$line"); [ -n "$status" ] || status=$(sed -n 's/.*status=\([A-Za-z]*\).*/\1/p' <<<"$line")
  status=$(printf '%s' "$status" | tr 'A-Z' 'a-z')
  h=$(sed -n 's/.*ANSWER_HASH=\([0-9a-f]*\).*/\1/p' <<<"$line")
  lm=$(sed -n 's/.*LOAD_MS=\([0-9]*\).*/\1/p' <<<"$line")
  gm=$(sed -n 's/.*GET_MS=\([0-9]*\).*/\1/p' <<<"$line")
  sm=$(sed -n 's/.*STAT_MS=\([0-9]*\).*/\1/p' <<<"$line")
  lsm=$(sed -n 's/.*LIST_MS=\([0-9]*\).*/\1/p' <<<"$line")
  reason=$(sed -n 's/.*REASON=\(.*\)/\1/p;s/.*reason=\(.*\)/\1/p' <<<"$line")
  case "$status" in
    ok)
      if [ -z "$REF" ]; then REF="$h"; eq="OK-ref"; else [ "$h" = "$REF" ] && eq="OK" || { eq="MISMATCH"; OBJ_MISMATCH=1; }; fi
      note=""
      [ "$lane" = semurg_object ] && semq="$gm"
      [ "$lane" = minio ] && minioq="$gm"
      [ -n "$semq" ] && [ "$lane" = minio ] && [ -n "$gm" ] && [ "$gm" -gt 0 ] 2>/dev/null && \
        note="Semurg GET $(awk -v a="$gm" -v b="$semq" 'BEGIN{if(a>0&&b>0)printf "%.1fx", a/b; else print "-"}') this lane"
      printf "   %-16s %-9s %-9s %-9s %-9s %-13s %s\n" "$lane" "${lm:--}" "${gm:--}" "${sm:--}" "${lsm:--}" "$eq" "$note";;
    skip) printf "   %-16s %-9s %-9s %-9s %-9s %-13s %s\n" "$lane" "-" "-" "-" "-" "SKIP" "$reason";;
    failed) printf "   %-16s %-9s %-9s %-9s %-9s %-13s %s\n" "$lane" "-" "-" "-" "-" "FAILED" "${reason:-engine-ran-but-a-step-failed}";;
    *) printf "   %-16s %-9s %-9s %-9s %-9s %-13s %s\n" "$lane" "-" "-" "-" "-" "DNF" "${reason:-did-not-finish}";;
  esac
done

echo
echo "Reading the board: 'equal-answer OK' means the lane stored + served the SAME bytes (GET content hash),"
echo "reported the SAME total size (STAT), and enumerated the SAME keyset (LIST) as the reference. Every lane"
echo "reproduces the answer from the ACTUAL data it served -- never a hardcoded/faked value. MinIO runs 4-drive"
echo "erasure over BOTH disks at max client concurrency; Semurg serves the objects through the ONE conveyor."

rm -rf "$SCRATCH" 2>/dev/null || true
[ "${OBJ_MISMATCH:-0}" = 0 ] || { echo; echo "PARITY FAIL: a lane's object answer disagreed with the independent reference (MISMATCH above). Exiting non-zero so a wrong answer never passes green."; exit 3; }
