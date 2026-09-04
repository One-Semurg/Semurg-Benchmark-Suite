#!/usr/bin/env bash
# _universal_gen.sh -- the SHARED deterministic UNIVERSAL (multi-modal) workload used by BOTH the
# Semurg lane (semurg_universal.sh) and the SurrealDB lane (surrealdb.sh), so the two engines run on
# BYTE-IDENTICAL data and their equal-answer hashes are directly comparable. Sourced, never executed.
#
# The dataset is one "single source of truth" with TWO models over the SAME records (SurrealDB's own
# multi-model pitch, and Semurg's Law-1 one-substrate):
#   * DOCUMENT model : person records  id -> {category_id, region_id, amount_cents}
#   * GRAPH model    : a `friend` relation, exactly one out-edge per person (src -> dst)
# Generation is a fixed LCG (identical constants + seed to the suite's other generators) so the same
# awk on the same box yields the same rows every run and in both lanes -- determinism is what the
# equal-answer gate needs (each engine is also gated against its OWN independent awk reference below).
#
# Files written under <dir>:
#   persons.csv : id,category_id,region_id,amount_cents          (header + N rows)
#   edges.csv   : src,dst                                        (header + N friend edges)
#
# The THREE canonical cross-model queries (the answer the gate compares):
#   Q1  DOCUMENT point-lookup            amount_cents of person Q1ID
#   Q2  DOCUMENT group-by aggregation    count of persons per category_id (0..C-1, comma-joined)
#   Q3  GRAPH + DOCUMENT join            count of friend edges whose TARGET person is in region Q3REG
# Q3 is the headline single-source query: it walks the graph relation AND reads a document field of the
# linked record -- the exact thing a one-store multi-model engine exists to do.

# univ_gen_dataset <dir> : write persons.csv + edges.csv deterministically. Honours env knobs:
#   UNIV_ROWS UNIV_CATEGORIES UNIV_REGIONS UNIV_AMOUNT_MOD
univ_gen_dataset(){
  local dir="$1"; mkdir -p "$dir" 2>/dev/null || true
  local N="${UNIV_ROWS:-50000}" C="${UNIV_CATEGORIES:-32}" R="${UNIV_REGIONS:-16}" A="${UNIV_AMOUNT_MOD:-1000000}"
  awk -v n="$N" -v C="$C" -v R="$R" -v A="$A" -v pf="$dir/persons.csv" -v ef="$dir/edges.csv" 'BEGIN{
    s=1234567;
    print "id,category_id,region_id,amount_cents" > pf;
    print "src,dst" > ef;
    for(i=1;i<=n;i++){
      s=(s*1103515245+12345)%2147483648; cat = int(s/256)%C;
      s=(s*1103515245+12345)%2147483648; reg = int(s/256)%R;
      s=(s*1103515245+12345)%2147483648; amt = 1 + s%A;
      s=(s*1103515245+12345)%2147483648; dst = 1 + int(s/256)%n;
      printf "%d,%d,%d,%d\n", i,cat,reg,amt > pf;
      printf "%d,%d\n", i,dst > ef;
    }
  }' 2>/dev/null
  [ -s "$dir/persons.csv" ] && [ -s "$dir/edges.csv" ]
}

# univ_compute_reference <dir> : compute the INDEPENDENT ground truth (awk over the CSVs, NEVER the
# engine) and export: REF_Q1 REF_Q2 REF_Q3 UNIV_Q1ID UNIV_Q3REG REF_HASH. REF_HASH uses the same
# hash_answer() from _common.sh both lanes source, so the two lanes emit an identical ANSWER_HASH.
univ_compute_reference(){
  local dir="$1"; local persons="$dir/persons.csv" edges="$dir/edges.csv"
  local C="${UNIV_CATEGORIES:-32}" N="${UNIV_ROWS:-50000}"
  UNIV_Q1ID="${UNIV_Q1_ID:-$(( 1 + N/3 ))}"; [ "$UNIV_Q1ID" -gt "$N" ] && UNIV_Q1ID="$N"
  # Q3 target region = the modal region (most persons; lowest id on a tie) -- deterministic, data-derived.
  UNIV_Q3REG="${UNIV_Q3_REGION:-$(awk -F, 'NR>1{c[$3]++} END{best=-1;b=0; for(k in c){ if(c[k]>best||(c[k]==best&&k<b)){best=c[k];b=k} } print b+0}' "$persons")}"
  REF_Q1="$(awk -F, -v T="$UNIV_Q1ID" 'NR>1 && $1==T{print $4; exit}' "$persons")"
  REF_Q2="$(awk -F, -v C="$C" 'NR>1{c[$2]++} END{for(i=0;i<C;i++) printf "%s%d",(i?",":""),c[i]+0}' "$persons")"
  # Q3 = friend edges whose TARGET (dst) person is in region UNIV_Q3REG. Join edges -> persons(region).
  REF_Q3="$(awk -F, -v R="$UNIV_Q3REG" 'FNR==NR{ if(FNR>1) reg[$1]=$3; next } FNR>1{ if(reg[$2]==R) n++ } END{ print n+0 }' "$persons" "$edges")"
  REF_HASH="$(hash_answer "$REF_Q1|$REF_Q2|$REF_Q3")"
  [ -n "$REF_Q1" ] && [ -n "$REF_Q2" ] && [ -n "$REF_Q3" ]
}
