#!/usr/bin/env bash
# gen_vectors.sh -- deterministic VECTOR dataset + INDEPENDENT exact-KNN ground truth for the arena
# VECTOR lane. Same knobs -> byte-identical files on any machine, so FAISS + Qdrant (+ Semurg) ingest
# the SAME base set and answer the SAME query set, and the equal-answer check is meaningful.
#
# The canonical vector query for this domain is EXACT (brute-force) top-K nearest-neighbour search by
# squared-L2 distance. Exact -- not approximate -- because only an exact answer can be gated bit-exact
# against an independent reference. (Approximate ANN / HNSW returns a recall<100% set that will NOT
# hash-match; that is a separate, non-gated recall metric, reported alongside but never counted as an
# equal-answer win. See report.md.)
#
# Emits (basename $OUT):
#   $OUT.base.csv   N lines  "id,f0,f1,...,f{D-1}"   (id 0..N-1, %.6f components -- identical text for
#                   every engine, so FAISS/Qdrant parse identical float32 and awk identical double)
#   $OUT.query.csv  Q lines  "qid,f0,...,f{D-1}"     (qid 0..Q-1)
#   $OUT.meta       one line "N=.. D=.. Q=.. K=.. METRIC=l2"
#   $OUT.answer.txt one line: the CANONICAL answer string -- for each query in qid order, the K nearest
#                   base ids sorted (dist ASC, id ASC) joined by ",", queries joined by ";".
#   $OUT.answer.hash  the reference hash = first 32 hex of sha256(answer.txt content) -- the gate value
#                     every lane must reproduce.
# Components are continuous floats, so exact-distance TIES are probability-zero: the top-K ordering is
# unambiguous and stable across float32 (FAISS/Qdrant) and double (this reference). The (dist ASC,id ASC)
# rule is applied everywhere as a defensive tiebreak. This is the standard ann-benchmarks ground-truth
# construction (float32 exact KNN), reproduced independently here in awk (engine-agnostic).
set -euo pipefail
OUT="${1:-vectors}"                       # output basename
N="${VEC_N:-20000}"                       # number of base vectors
D="${VEC_D:-64}"                          # dimensionality
Q="${VEC_Q:-100}"                         # number of query vectors
K="${VEC_K:-10}"                          # neighbours per query
SEED="${VEC_SEED:-1234567}"               # LCG seed (determinism)

base="$OUT.base.csv"; query="$OUT.query.csv"; meta="$OUT.meta"
answer="$OUT.answer.txt"; ahash="$OUT.answer.hash"

# --- 1. base + query vectors. One shared LCG stream: base first (N*D draws), then queries (Q*D draws),
#        so the two sets are disjoint deterministic slices of the same stream. Component = (s mod 100000)
#        / 100000 in [0,1), printed %.6f -> byte-identical text everyone parses the same way. ---
awk -v n="$N" -v d="$D" -v seed="$SEED" 'BEGIN{
  s=seed;
  for(i=0;i<n;i++){
    printf "%d", i;
    for(j=0;j<d;j++){ s=(s*1103515245+12345)%2147483648; printf ",%.6f", (s%100000)/100000.0 }
    printf "\n";
  }
}' > "$base"

awk -v n="$N" -v d="$D" -v q="$Q" -v seed="$SEED" 'BEGIN{
  s=seed;
  # advance the stream past the N*D base draws so queries are a disjoint slice (same recurrence).
  skip=n*d; for(t=0;t<skip;t++){ s=(s*1103515245+12345)%2147483648 }
  for(i=0;i<q;i++){
    printf "%d", i;
    for(j=0;j<d;j++){ s=(s*1103515245+12345)%2147483648; printf ",%.6f", (s%100000)/100000.0 }
    printf "\n";
  }
}' > "$query"

printf "N=%s D=%s Q=%s K=%s METRIC=l2\n" "$N" "$D" "$Q" "$K" > "$meta"

# --- 2. INDEPENDENT reference: exact brute-force top-K by squared-L2, tiebreak (dist ASC, id ASC).
#        O(Q*(N*D + K*N)) double-precision -- engine-agnostic, computed from the CSV text only. ---
awk -v d="$D" -v k="$K" '
  BEGIN{ FS="," }
  # first read base.csv (NR over first file), then query.csv.
  FNR==NR{
    bid[FNR-1]=$1;
    for(j=1;j<=d;j++) B[(FNR-1)*d + (j-1)] = $(j+1);
    NB=FNR; next;
  }
  {
    # a query row: compute distance to every base, then select K smallest (dist ASC, id ASC).
    qi=$1;
    for(b=0;b<NB;b++){
      dist=0.0;
      off=b*d;
      for(j=0;j<d;j++){ diff=$(j+2)-B[off+j]; dist+=diff*diff }
      dd[b]=dist;
    }
    line="";
    for(r=0;r<k;r++){
      best=-1; bestd=0;
      for(b=0;b<NB;b++){
        if(dd[b]<0) continue;                          # already taken
        if(best<0 || dd[b]<bestd){ best=b; bestd=dd[b] } # strict < + ascending id scan => id-ASC tiebreak
      }
      if(best<0) break;
      line = (r==0? bid[best] : line "," bid[best]);
      dd[best]=-1;                                      # mark taken
    }
    ans = (qi==0? line : ans ";" line);
    delete dd;
  }
  END{ printf "%s", ans }
' "$base" "$query" > "$answer"

# --- 3. reference hash = first 32 hex of sha256(answer content) -- the gate every lane reproduces. ---
H="$(sha256sum "$answer" | cut -c1-32)"
printf "%s" "$H" > "$ahash"

echo "gen_vectors: N=$N D=$D Q=$Q K=$K metric=l2 base=$base query=$query"
echo "gen_vectors: reference_answer_hash=$H  (over $Q queries x top-$K, exact squared-L2)"
