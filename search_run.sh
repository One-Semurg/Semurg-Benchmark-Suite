#!/usr/bin/env bash
# search_run.sh -- the SEARCH head-to-head: OpenSearch vs Semurg on the SAME deterministic full-text
# document corpus, with an EQUAL-ANSWER parity gate. The canonical query set is the four boolean/aggregate
# SEARCH ops (S1 full-text term match, S2 exact term filter, S3 terms aggregation, S4 numeric range); an
# INDEPENDENT awk pass over the corpus computes the ground-truth answer, and a lane counts ONLY if its
# answer hash matches that reference. BM25 relevance RANKING is the incumbent's home turf and is a
# SEPARATE, non-gated metric, never claimed here.
#
# No DeWitt engines in this public board -- OpenSearch (Apache-2.0) is public-OK. Elasticsearch is a
# DeWitt-clause engine and is therefore NEVER driven here; it runs ONLY via run_all_domains.sh --licensed
# (labelled [DeWitt-internal]) and its numbers never leave the box. This runner is fully publishable.
#
# Both lanes self-generate the SAME corpus from the SAME knobs (identical LCG => byte-identical corpus on
# any box), so each lane's internal equal-answer gate AND this runner's cross-engine gate agree on the one
# reference hash (default knobs => 30bac289...). Scale the board via SEARCH_DOCS / SEARCH_NCAT etc.
set -uo pipefail
SEARCH_MISMATCH=0
HERE="$(cd "$(dirname "$0")" && pwd)"
LANES="${SEARCH_LANES_DIR:-$HERE/lanes}"
SCRATCH="${SEARCH_RUN_SCRATCH:-${TMPDIR:-/tmp}/arena_search}"
TO="${LANE_TIMEOUT:-900}"
. "$LANES/_common.sh"

# deterministic corpus knobs (identical defaults to lanes/semurg_search.sh + lanes/opensearch.sh)
DOCS="${SEARCH_DOCS:-200000}"; SEED="${SEARCH_SEED:-1234567}"; NCAT="${SEARCH_NCAT:-3}"
S1_TERM="${SEARCH_S1_TERM:-foxtrot}"; S2_CAT="${SEARCH_S2_CAT:-1}"; S4_PRI="${SEARCH_S4_PRI:-4}"
VOCAB="alpha bravo charlie delta echo foxtrot golf hotel india juliet kilo lima"

rm -rf "$SCRATCH"; mkdir -p "$SCRATCH"
echo "############################################################################################"
echo "# SEARCH head-to-head: OpenSearch (Apache-2.0) vs Semurg -- equal-answer full-text query set. #"
echo "# OpenSearch is public-license (no DeWitt clause here): fully publishable. Elasticsearch is    #"
echo "# DeWitt-restricted and is NEVER run here (only via run_all_domains.sh --licensed, internal).  #"
echo "############################################################################################"
echo "== generating the independent reference over N=$DOCS docs (NCAT=$NCAT, S1=$S1_TERM, S2_CAT=$S2_CAT, S4_PRI>=$S4_PRI) =="

# INDEPENDENT reference answer -- the SAME LCG corpus + the SAME O(N) awk fold every lane must reproduce.
refcorpus="$SCRATCH/ref.corpus.tsv"
awk -v n="$DOCS" -v seed="$SEED" -v ncat="$NCAT" -v vocab="$VOCAB" 'BEGIN{
  m=split(vocab, W, " "); s=seed; OFS="\t";
  for(i=1;i<=n;i++){
    s=(s*1103515245+12345)%2147483648; ti=1+int(s/131072)%m;
    s=(s*1103515245+12345)%2147483648; a=1+int(s/131072)%m;
    s=(s*1103515245+12345)%2147483648; b=1+int(s/131072)%m;
    s=(s*1103515245+12345)%2147483648; cat=int(s/131072)%ncat;
    s=(s*1103515245+12345)%2147483648; pri=1+int(s/131072)%5;
    body="the " W[ti] " subsystem streams through the " W[a] " " W[b] " tier";
    print i, W[ti], body, cat, pri;
  }
}' > "$refcorpus"
read -r R_S1 R_S2 R_S3 R_S4 < <(awk -v term="$S1_TERM" -v c="$S2_CAT" -v ncat="$NCAT" -v p="$S4_PRI" '
  BEGIN{ FS="\t"; s1=0; s4=0; for(k=0;k<ncat;k++) g[k]=0 }
  { if($2==term) s1++; g[$4+0]++; if($5+0>=p) s4++; }
  END{ grp=""; for(k=0;k<ncat;k++){ grp=(k==0?g[k]:grp","g[k]) } print s1, g[c], grp, s4 }' "$refcorpus")
REF_ANS="S1=${R_S1};S2=${R_S2};S3=${R_S3};S4=${R_S4}"
REF="$(hash_answer "$REF_ANS")"
echo "   reference_answer            = $REF_ANS"
echo "   reference_answer_hash       = $REF"
echo
printf "   %-16s %-9s %-9s %-13s %s\n" engine load_ms query_ms equal-answer note

# lanes: OpenSearch is the public incumbent board; semurg_search runs too when the native lane is present
# (install Semurg first) and is skipped cleanly if absent, so the incumbent board is always runnable alone.
LANESET="opensearch"
[ -f "$LANES/semurg_search.sh" ] && LANESET="semurg_search $LANESET"

osq=""
for lane in $LANESET; do
  raw="$( SEARCH_DOCS="$DOCS" SEARCH_SEED="$SEED" SEARCH_NCAT="$NCAT" \
          SEARCH_S1_TERM="$S1_TERM" SEARCH_S2_CAT="$S2_CAT" SEARCH_S4_PRI="$S4_PRI" \
          ARENA_DATA="$SCRATCH/$lane" \
          timeout -k 10 "${TO}s" bash "$LANES/$lane.sh" 2>/dev/null )"
  line="$(grep -m1 '^LANE=' <<<"$raw")"
  [ -n "$line" ] || line="LANE=$lane STATUS=dnf REASON=timed-out-or-errored(>${TO}s)"
  status=$(sed -n 's/.*STATUS=\([a-z]*\).*/\1/p' <<<"$line")
  h=$(sed -n 's/.*ANSWER_HASH=\([0-9a-f]*\).*/\1/p' <<<"$line")
  [ -n "$h" ] || h=$(sed -n 's/.*\bANSWER=\([0-9a-f]*\).*/\1/p' <<<"$line")
  lm=$(sed -n 's/.*LOAD_MS=\([0-9]*\).*/\1/p' <<<"$line"); qm=$(sed -n 's/.*QUERY_MS=\([0-9]*\).*/\1/p' <<<"$line")
  reason=$(sed -n 's/.*REASON=\(.*\)/\1/p' <<<"$line")
  case "$status" in
    ok)
      if [ "$h" = "$REF" ]; then eq="OK"; else eq="MISMATCH"; SEARCH_MISMATCH=1; fi
      note=""
      [ "$lane" = opensearch ] && osq="$qm"
      [ -n "$osq" ] && [ "$lane" != opensearch ] && [ -n "$qm" ] && [ "$qm" -gt 0 ] 2>/dev/null && \
        note="OpenSearch query $(awk -v a="$osq" -v b="$qm" 'BEGIN{if(b>0)printf "%.1fx", a/b; else print "-"}') this lane"
      printf "   %-16s %-9s %-9s %-13s %s\n" "$lane" "${lm:--}" "${qm:--}" "$eq" "$note";;
    skip) printf "   %-16s %-9s %-9s %-13s %s\n" "$lane" "-" "-" "SKIP" "$reason";;
    *)    printf "   %-16s %-9s %-9s %-13s %s\n" "$lane" "-" "-" "DNF" "${reason:-did-not-finish}";;
  esac
done

echo
echo "Reading it: 'equal-answer OK' means the lane reproduced the SAME S1/S2/S3/S4 answer (same doc-id"
echo "result set) as the independent awk reference; a disagreeing lane is MISMATCH and is never counted."
echo "OpenSearch is driven at max concurrency on its node (number_of_shards = all cores, index striped"
echo "across both NVMe data disks). QUERY_MS is the warm2 steady-state query-set wall (see COLD/WARM in"
echo "the raw LANE line). Full-text BM25 RANKING is a separate, non-gated metric and is not claimed here."
rm -rf "$SCRATCH" 2>/dev/null || true
[ "${SEARCH_MISMATCH:-0}" = 0 ] || { echo; echo "PARITY FAIL: a lane's answer disagreed with the independent reference (MISMATCH above). Exiting non-zero so a wrong answer never passes green."; exit 3; }
