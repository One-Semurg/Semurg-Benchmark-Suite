#!/usr/bin/env bash
# universal_run.sh -- the UNIVERSAL (multi-modal / single-source-of-truth) head-to-head: Semurg vs
# SurrealDB on ONE deterministic dataset carried under TWO models over the SAME records (document +
# graph). This is the headline single-store-vs-single-store comparison: SurrealDB is the direct
# multi-model competitor (one store, many models), and so is Semurg (Law 1, one substrate).
#
# ONE dataset, THREE cross-model queries, equal-answer gated against an INDEPENDENT awk reference:
#   Q1  DOCUMENT point-lookup            amount of a person record
#   Q2  DOCUMENT group-by aggregation    per-category count
#   Q3  GRAPH + DOCUMENT join (headline) count friend edges whose linked person is in a region
# A lane counts ONLY when its Q1|Q2|Q3 hash matches the reference; a disagreeing engine is MISMATCH and
# is never counted (exit non-zero). Missing docker/release -> a clean SKIP, never a crash or a fake number.
#
# Same deterministic dataset as run_all_domains.sh domain 11 (both lanes call lanes/_universal_gen.sh),
# so this standalone runner and the master board agree.
set -uo pipefail
UNIV_MISMATCH=0
HERE="$(cd "$(dirname "$0")" && pwd)"
LANES="${UNIV_LANES_DIR:-$HERE/lanes}"
SCRATCH="${UNIV_SCRATCH:-${TMPDIR:-/tmp}/arena_universal}"
TO="${LANE_TIMEOUT:-900}"
. "$LANES/_common.sh"; . "$LANES/_universal_gen.sh"

N="${UNIV_ROWS:-50000}"; C="${UNIV_CATEGORIES:-32}"; R="${UNIV_REGIONS:-16}"

echo "############################################################################################"
echo "# UNIVERSAL (multi-modal) head-to-head: Semurg vs SurrealDB (Apache-2.0-era image) on ONE     #"
echo "# deterministic dataset under TWO models (document + graph) -- one store, many models, both.   #"
echo "# The headline single-source-of-truth-vs-single-source-of-truth comparison. Publication-safe.  #"
echo "############################################################################################"

# entry cleanup so a prior interrupted run never wedges this one
arena_cleanup_containers

rm -rf "$SCRATCH"; mkdir -p "$SCRATCH"
echo "== generating deterministic dataset: N=$N persons, C=$C categories, R=$R regions, +N friend edges =="
univ_gen_dataset "$SCRATCH" || { echo "   dataset generation FAILED"; exit 2; }
univ_compute_reference "$SCRATCH" || { echo "   reference computation FAILED"; exit 2; }
echo "   independent awk reference: Q1 person:$UNIV_Q1ID amount=$REF_Q1 | Q2 per-category=[$REF_Q2]"
echo "   Q3 friend->region=$UNIV_Q3REG -> $REF_Q3   reference_answer_hash=$REF_HASH"
echo

echo "== UNIVERSAL cross-model board (equal-answer gated: Q1 doc point-get | Q2 doc group-by | Q3 graph+doc join) =="
printf "   %-18s %-9s %-8s %-8s %-8s %-13s %s\n" engine load_ms q1_ms q2_ms q3_ms equal-answer note

# Semurg first (its number is the point), then SurrealDB. Each lane regenerates the byte-identical
# dataset internally (shared generator) and gates against its own independent reference.
LANESET="semurg_universal surrealdb"
surreal_load=""
for lane in $LANESET; do
  raw="$( ARENA_DATA="$SCRATCH/$lane" UNIV_ROWS="$N" UNIV_CATEGORIES="$C" UNIV_REGIONS="$R" \
      timeout -k 10 "${TO}s" bash "$LANES/$lane.sh" 2>/dev/null )"
  line="$(grep -m1 '^LANE=' <<<"$raw")"
  [ -n "$line" ] || line="LANE=$lane STATUS=dnf REASON=timed-out-or-errored(>${TO}s)"
  status="$(sed -n 's/.*STATUS=\([a-z]*\).*/\1/p' <<<"$line")"
  h="$(sed -n 's/.*ANSWER_HASH=\([0-9a-f]*\).*/\1/p' <<<"$line")"
  lm="$(sed -n 's/.*LOAD_MS=\([0-9]*\).*/\1/p' <<<"$line")"
  q1="$(sed -n 's/.*Q1_MS=\([0-9]*\).*/\1/p' <<<"$line")"
  q2="$(sed -n 's/.*Q2_MS=\([0-9]*\).*/\1/p' <<<"$line")"
  q3="$(sed -n 's/.*Q3_MS=\([0-9]*\).*/\1/p' <<<"$line")"
  reason="$(sed -n 's/.*REASON=\(.*\)/\1/p' <<<"$line")"
  case "$status" in
    ok)
      if [ "$h" = "$REF_HASH" ]; then eq="OK"; else eq="MISMATCH"; UNIV_MISMATCH=1; fi
      note=""
      [ "$lane" = surrealdb ] && surreal_load="$lm"
      printf "   %-18s %-9s %-8s %-8s %-8s %-13s %s\n" "$lane" "${lm:--}" "${q1:--}" "${q2:--}" "${q3:--}" "$eq" "$note";;
    skip) printf "   %-18s %-9s %-8s %-8s %-8s %-13s %s\n" "$lane" "-" "-" "-" "-" "SKIP" "$reason";;
    *)    printf "   %-18s %-9s %-8s %-8s %-8s %-13s %s\n" "$lane" "-" "-" "-" "-" "DNF" "${reason:-did-not-finish}";;
  esac
done

echo
echo "Reading the board: 'equal-answer OK' means the engine returned the SAME Q1|Q2|Q3 as the independent"
echo "awk reference over the identical dataset -- a disagreeing engine is MISMATCH and never counted. Q3 is"
echo "the single-source query both engines exist for: walk the friend relation AND read the linked record's"
echo "document field, in one store. Semurg serves it through the ONE conveyor (belt evidence on stderr);"
echo "SurrealDB serves it from its multi-model store at its fastest (in-memory) path."

# exit cleanup
arena_cleanup_containers
rm -rf "$SCRATCH" 2>/dev/null || true
[ "${UNIV_MISMATCH:-0}" = 0 ] || { echo; echo "PARITY FAIL: an engine's cross-model answer disagreed with the independent reference (MISMATCH above). Exiting non-zero so a wrong answer never passes green."; exit 3; }
