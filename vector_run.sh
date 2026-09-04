#!/usr/bin/env bash
# vector_run.sh -- the VECTOR head-to-head: FAISS vs Qdrant (+ Semurg when installed) on the SAME
# deterministic vectors. It prints TWO boards on the SAME base/query set:
#
#   BOARD 1  EXACT (equal-answer gated)  -- brute-force top-K nearest-neighbour by squared-L2. An
#            INDEPENDENT awk reference computes the ground-truth top-K, and a lane counts ONLY if its
#            neighbour-id hash matches. Exact -- not approximate -- because only an exact answer is
#            gate-able bit-exact. Here Semurg is byte-correct but SLOW (an index-free full scan over the
#            full float vectors); FAISS IndexFlatL2 + Qdrant Euclid-exact are the exact incumbents.
#
#   BOARD 2  ANN (recall@K + latency)    -- each engine runs its APPROXIMATE index and is scored by
#            RECALL@K against the exact reference, plus query latency. This is Semurg's REAL fast vector
#            path: the founder-locked SimHash / Hamming ANN (Shard.top_k_hamming over 44-byte codes).
#            FAISS runs HNSW (IndexHNSWFlat); Qdrant runs its native HNSW. Same task, same data, all
#            approximate, all recall-scored -- the ann-benchmarks-style recall-vs-latency board. Recall
#            below 1.0 is EXPECTED and honest; it is never a MISMATCH and never gates.
#
# No DeWitt engines in the vector domain -- FAISS (MIT) and Qdrant (Apache-2.0) are both public-OK, so
# both boards are fully publishable and there is NO lanes/licensed/ vector lane.
set -uo pipefail
VEC_MISMATCH=0
HERE="$(cd "$(dirname "$0")"&&pwd)"
LANES="${VEC_LANES_DIR:-$HERE/lanes}"; WORK="${VEC_WORK_DIR:-$HERE/workload}"
SCRATCH="${VEC_SCRATCH:-${TMPDIR:-/tmp}/arena_vector}"
TO="${LANE_TIMEOUT:-900}"
. "$LANES/_common.sh"

# dataset knobs (deterministic; same on any box). Modest defaults keep the awk reference quick; scale up
# via VEC_N / VEC_D / VEC_Q for a heavier board.
N="${VEC_N:-20000}"; D="${VEC_D:-64}"; Q="${VEC_Q:-100}"; K="${VEC_K:-10}"

rm -rf "$SCRATCH"; mkdir -p "$SCRATCH"; base="$SCRATCH/vec"
echo "############################################################################################"
echo "# VECTOR head-to-head: FAISS (MIT) vs Qdrant (Apache-2.0) (+ Semurg). TWO boards on the SAME #"
echo "# vectors: EXACT (equal-answer top-K) and ANN (recall@K + latency, the approximate indexes). #"
echo "# Both engines are public-license (no DeWitt clause in this domain): fully publishable.      #"
echo "############################################################################################"
echo "== generating deterministic dataset: N=$N base, D=$D dims, Q=$Q queries, top-$K exact L2 =="
VEC_N="$N" VEC_D="$D" VEC_Q="$Q" VEC_K="$K" bash "$WORK/gen_vectors.sh" "$base" | sed 's/^/   /'
REF="$(cat "$base.answer.hash")"
REF_ANSWER="$base.answer.txt"
echo "   reference_answer_hash (independent awk exact-KNN) = $REF"

# =========================================================================================
# BOARD 1 -- EXACT (equal-answer gated)
# =========================================================================================
echo
echo "== BOARD 1 -- EXACT top-$K KNN (equal-answer gated) =="
printf "   %-16s %-9s %-9s %-13s %s\n" engine load_ms query_ms equal-answer note

# lanes: FAISS + Qdrant are the public board. semurg_vector runs too when the native lane is present
# (install Semurg first); it is skipped cleanly if the lane script is absent, so the incumbent board is
# always runnable on its own.
LANESET="faiss qdrant"
[ -f "$LANES/semurg_vector.sh" ] && LANESET="semurg_vector $LANESET"

faissq=""
for lane in $LANESET; do
  raw="$( VEC_BASE_CSV="$base.base.csv" VEC_QUERY_CSV="$base.query.csv" VEC_META="$base.meta" \
      VEC_K="$K" VEC_SCRATCH="$SCRATCH/$lane" \
      timeout -k 10 "${TO}s" bash "$LANES/$lane.sh" 2>/dev/null )"
  line="$(grep -m1 '^LANE=' <<<"$raw")"
  [ -n "$line" ] || line="LANE=$lane STATUS=dnf REASON=timed-out-or-errored(>${TO}s)"
  status=$(sed -n 's/.*STATUS=\([a-z]*\).*/\1/p' <<<"$line")
  h=$(sed -n 's/.*ANSWER_HASH=\([0-9a-f]*\).*/\1/p' <<<"$line")
  lm=$(sed -n 's/.*LOAD_MS=\([0-9]*\).*/\1/p' <<<"$line"); qm=$(sed -n 's/.*QUERY_MS=\([0-9]*\).*/\1/p' <<<"$line")
  reason=$(sed -n 's/.*REASON=\(.*\)/\1/p' <<<"$line")
  case "$status" in
    ok)
      if [ "$h" = "$REF" ]; then eq="OK"; else eq="MISMATCH"; VEC_MISMATCH=1; fi
      note=""
      [ "$lane" = faiss ] && faissq="$qm"
      [ -n "$faissq" ] && [ "$lane" != faiss ] && [ -n "$qm" ] && [ "$qm" -gt 0 ] 2>/dev/null && \
        note="FAISS query $(awk -v a="$qm" -v b="$faissq" 'BEGIN{if(a>0)printf "%.1fx", a/b; else print "-"}') this lane"
      printf "   %-16s %-9s %-9s %-13s %s\n" "$lane" "${lm:--}" "${qm:--}" "$eq" "$note";;
    skip) printf "   %-16s %-9s %-9s %-13s %s\n" "$lane" "-" "-" "SKIP" "$reason";;
    *)    printf "   %-16s %-9s %-9s %-13s %s\n" "$lane" "-" "-" "DNF" "${reason:-did-not-finish}";;
  esac
done

echo
echo "Reading BOARD 1: 'equal-answer OK' means the lane returned the SAME exact top-$K neighbour ids as the"
echo "independent reference (a disagreeing lane is MISMATCH and never counted). All three run EXACT here, so"
echo "they are directly gate-able. Semurg is byte-correct but slow -- an index-free full scan over the FULL"
echo "float vectors. BOARD 2 below is Semurg's real fast vector path (approximate, recall-scored)."

# =========================================================================================
# BOARD 2 -- ANN (recall@K + latency). All approximate; recall<1.0 is expected and never gates.
# =========================================================================================
echo
echo "== BOARD 2 -- ANN recall@$K + latency (approximate indexes; NOT equal-answer) =="
printf "   %-16s %-9s %-9s %-11s %s\n" engine load_ms query_ms recall@$K note

# Semurg native SimHash/Hamming ANN first (its number is the point of this board), then the incumbents' HNSW.
LANESET_ANN="faiss_ann qdrant_ann"
[ -f "$LANES/semurg_vector_ann.sh" ] && LANESET_ANN="semurg_vector_ann $LANESET_ANN"

faissannq=""
for lane in $LANESET_ANN; do
  raw="$( VEC_BASE_CSV="$base.base.csv" VEC_QUERY_CSV="$base.query.csv" VEC_META="$base.meta" \
      VEC_REF_ANSWER="$REF_ANSWER" VEC_K="$K" VEC_SCRATCH="$SCRATCH/$lane" \
      timeout -k 10 "${TO}s" bash "$LANES/$lane.sh" 2>/dev/null )"
  line="$(grep -m1 '^LANE=' <<<"$raw")"
  [ -n "$line" ] || line="LANE=$lane STATUS=dnf REASON=timed-out-or-errored(>${TO}s)"
  status=$(sed -n 's/.*STATUS=\([a-z-]*\).*/\1/p' <<<"$line")
  rc=$(sed -n 's/.*RECALL_AT_K=\([0-9.]*\).*/\1/p' <<<"$line")
  lm=$(sed -n 's/.*LOAD_MS=\([0-9]*\).*/\1/p' <<<"$line"); qm=$(sed -n 's/.*QUERY_MS=\([0-9]*\).*/\1/p' <<<"$line")
  mode=$(sed -n 's/.*MODE=\([^ ]*\).*/\1/p' <<<"$line")
  reason=$(sed -n 's/.*REASON=\(.*\)/\1/p' <<<"$line")
  case "$status" in
    ok-recall)
      note="$mode"
      [ "$lane" = faiss_ann ] && faissannq="$qm"
      [ -n "$faissannq" ] && [ "$lane" != faiss_ann ] && [ -n "$qm" ] && [ "$qm" -gt 0 ] 2>/dev/null && \
        note="$mode  (FAISS-HNSW query $(awk -v a="$qm" -v b="$faissannq" 'BEGIN{if(a>0)printf "%.1fx", a/b; else print "-"}') this lane)"
      printf "   %-16s %-9s %-9s %-11s %s\n" "$lane" "${lm:--}" "${qm:--}" "${rc:--}" "$note";;
    skip) printf "   %-16s %-9s %-9s %-11s %s\n" "$lane" "-" "-" "-" "SKIP $reason";;
    *)    printf "   %-16s %-9s %-9s %-11s %s\n" "$lane" "-" "-" "-" "DNF ${reason:-did-not-finish}";;
  esac
done

echo
echo "Reading BOARD 2: each engine runs its OWN approximate index over the SAME vectors and is scored by"
echo "recall@$K vs the exact L2 reference plus query latency (the ann-benchmarks trade-off). Semurg uses its"
echo "native SimHash / Hamming binary codes (Shard.top_k_hamming) -- fast, with real binary-quantisation"
echo "recall loss shown honestly; FAISS and Qdrant use float HNSW graphs (typically near-1.0 recall). A"
echo "recall below 1.0 is expected here and is NOT a mismatch -- it is the point of an approximate board."

rm -rf "$SCRATCH" 2>/dev/null || true
[ "${VEC_MISMATCH:-0}" = 0 ] || { echo; echo "PARITY FAIL: a BOARD 1 lane's exact top-K disagreed with the independent reference (MISMATCH above). Exiting non-zero so a wrong answer never passes green."; exit 3; }
