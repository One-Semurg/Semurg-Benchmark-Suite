#!/usr/bin/env bash
# document_run.sh -- the DOCUMENT head-to-head: MongoDB (mongo:7) vs Semurg on the SAME deterministic
# JSON document collection, with an EQUAL-ANSWER parity gate. The canonical query set is the three JSON
# document ops (Q1 point-lookup by _id -> amount_cents, Q2 group-by aggregation count-per-category, Q3
# nested-field filter count shipping.region_id == R); an INDEPENDENT awk pass over the collection computes
# the ground-truth answer, and a lane counts ONLY if its answer hash matches that reference.
#
# No DeWitt engines in this public board -- MongoDB (SSPL, self-host-ok) is the public incumbent and is
# fully publishable. There is NO lanes/licensed/ document lane.
#
# Both lanes self-generate the SAME collection from the SAME knobs (identical gawk LCG => byte-identical
# per-record values on any box), so each lane's internal equal-answer gate AND this runner's cross-engine
# gate agree on the one reference hash (default knobs DOC_ROWS=2000000 => 4128f675...). Scale the board via
# DOC_ROWS / DOC_CATEGORIES / DOC_REGIONS; drive MongoDB concurrency via DOC_MONGO_CLIENTS (default 16).
set -uo pipefail
DOC_MISMATCH=0
HERE="$(cd "$(dirname "$0")" && pwd)"
LANES="${DOC_LANES_DIR:-$HERE/lanes}"
SCRATCH="${DOC_RUN_SCRATCH:-${TMPDIR:-/tmp}/arena_document}"
TO="${LANE_TIMEOUT:-900}"
. "$LANES/_common.sh"

# deterministic collection knobs (IDENTICAL defaults to lanes/semurg_doc.sh + lanes/mongodb.sh) --------
ROWS="${DOC_ROWS:-2000000}"
CATS="${DOC_CATEGORIES:-32}"
REGS="${DOC_REGIONS:-16}"
STATS="${DOC_STATUSES:-8}"
TSTART="${DOC_TS_START:-1700000000}"
TSSPAN="${DOC_TS_SPAN:-31536000}"
AMTMOD="${DOC_AMOUNT_MOD:-1000000}"
CUSTMOD="${DOC_CUSTOMERS:-50000}"
NTAGS="${DOC_TAG_UNIVERSE:-48}"
NCLIENTS="${DOC_MONGO_CLIENTS:-16}"

rm -rf "$SCRATCH"; mkdir -p "$SCRATCH"
echo "############################################################################################"
echo "# DOCUMENT head-to-head: MongoDB (mongo:7, SSPL self-host-ok) vs Semurg -- equal-answer JSON  #"
echo "# document query set. MongoDB is public-license (no DeWitt clause here): fully publishable.    #"
echo "############################################################################################"
echo "== generating the independent reference over N=$ROWS docs (CATS=$CATS, REGS=$REGS) =="

# INDEPENDENT reference -- the SAME gawk LCG collection + the SAME O(N) awk fold every lane must reproduce.
refcsv="$SCRATCH/ref.docs.csv"
awk -v n="$ROWS" -v C="$CATS" -v R="$REGS" -v S="$STATS" -v tstart="$TSTART" -v tsspan="$TSSPAN" \
    -v amtmod="$AMTMOD" -v custmod="$CUSTMOD" -v ntags="$NTAGS" 'BEGIN{
  s=1234567;
  print "id,customer_id,category_id,region_id,status_id,amount_cents,ts,tags_bitmap";
  for(i=1;i<=n;i++){
    s=(s*1103515245+12345)%2147483648; cust = 1 + s%custmod;
    s=(s*1103515245+12345)%2147483648; cat  = int(s/256)%C;
    s=(s*1103515245+12345)%2147483648; reg  = int(s/256)%R;
    s=(s*1103515245+12345)%2147483648; st   = int(s/256)%S;
    s=(s*1103515245+12345)%2147483648; amt  = 1 + s%amtmod;
    s=(s*1103515245+12345)%2147483648; ts   = tstart + s%tsspan;
    s=(s*1103515245+12345)%2147483648; t1   = int(s/256)%ntags;
    s=(s*1103515245+12345)%2147483648; t2   = int(s/256)%ntags;
    s=(s*1103515245+12345)%2147483648; prio = int(s/256)%5;
    bm = lshift_or(t1, t2);
    printf "%d,%d,%d,%d,%d,%d,%d,%d\n", i,cust,cat,reg,st,amt,ts,bm;
  }
}
function pow2(b,  r){ r=1; while(b-->0) r*=2; return r; }
function lshift_or(a,b,  x,y){ x=pow2(a); y=pow2(b); if(a==b) return x; return x+y; }
' > "$refcsv"
Q1ID="${DOC_Q1_ID:-$(( 1 + ROWS/3 ))}"; [ "$Q1ID" -gt "$ROWS" ] && Q1ID="$ROWS"
Q3REG="${DOC_Q3_REGION:-$(awk -F, 'NR>1{c[$4]++} END{best=-1;b=0; for(k in c){ if(c[k]>best||(c[k]==best&&k<b)){best=c[k];b=k} } print b}' "$refcsv")}"
R_Q1="$(awk -F, -v T="$Q1ID" 'NR>1 && $1==T{print $6; exit}' "$refcsv")"
R_Q2="$(awk -F, -v C="$CATS" 'NR>1{c[$3]++} END{for(i=0;i<C;i++) printf "%s%d",(i?",":""),c[i]+0}' "$refcsv")"
R_Q3="$(awk -F, -v R="$Q3REG" 'NR>1 && $4==R{n++} END{print n+0}' "$refcsv")"
REF_ANS="Q1=${R_Q1};Q2=[${R_Q2}];Q3=${R_Q3}"
REF="$(hash_answer "$R_Q1|$R_Q2|$R_Q3")"
echo "   reference_answer            = Q1(_id=$Q1ID)->$R_Q1 | Q2 per-category=[$R_Q2] | Q3(shipping.region_id=$Q3REG)->$R_Q3"
echo "   reference_answer_hash       = $REF"
echo
printf "   %-16s %-9s %-8s %-8s %-8s %-13s %s\n" engine load_ms q1_ms q2_ms q3_ms equal-answer note

# lanes: MongoDB is the public incumbent board; semurg_doc runs too when the native lane is present
# (install Semurg first) and is skipped cleanly if absent, so the incumbent board is always runnable alone.
LANESET="mongodb"
[ -f "$LANES/semurg_doc.sh" ] && LANESET="semurg_doc $LANESET"

sdq1=""; sdq2=""; sdq3=""
for lane in $LANESET; do
  raw="$( DOC_ROWS="$ROWS" DOC_CATEGORIES="$CATS" DOC_REGIONS="$REGS" DOC_STATUSES="$STATS" \
          DOC_TS_START="$TSTART" DOC_TS_SPAN="$TSSPAN" DOC_AMOUNT_MOD="$AMTMOD" \
          DOC_CUSTOMERS="$CUSTMOD" DOC_TAG_UNIVERSE="$NTAGS" DOC_Q1_ID="$Q1ID" DOC_Q3_REGION="$Q3REG" \
          DOC_MONGO_CLIENTS="$NCLIENTS" \
          ARENA_DATA="$SCRATCH/$lane" \
          timeout -k 10 "${TO}s" bash "$LANES/$lane.sh" 2>/dev/null )"
  line="$(grep -m1 '^LANE=' <<<"$raw")"
  [ -n "$line" ] || line="LANE=$lane STATUS=dnf REASON=timed-out-or-errored(>${TO}s)"
  status=$(sed -n 's/.*STATUS=\([a-z]*\).*/\1/p' <<<"$line")
  h=$(sed -n 's/.*ANSWER_HASH=\([0-9a-f]*\).*/\1/p' <<<"$line")
  lm=$(sed -n 's/.*LOAD_MS=\([0-9]*\).*/\1/p' <<<"$line")
  q1=$(sed -n 's/.*Q1_MS=\([0-9]*\).*/\1/p' <<<"$line")
  q2=$(sed -n 's/.*Q2_MS=\([0-9]*\).*/\1/p' <<<"$line")
  q3=$(sed -n 's/.*Q3_MS=\([0-9]*\).*/\1/p' <<<"$line")
  reason=$(sed -n 's/.*REASON=\(.*\)/\1/p' <<<"$line")
  case "$status" in
    ok)
      if [ "$h" = "$REF" ]; then eq="OK"; else eq="MISMATCH"; DOC_MISMATCH=1; fi
      note=""
      if [ "$lane" = semurg_doc ]; then sdq1="$q1"; sdq2="$q2"; sdq3="$q3"; fi
      printf "   %-16s %-9s %-8s %-8s %-8s %-13s %s\n" "$lane" "${lm:--}" "${q1:--}" "${q2:--}" "${q3:--}" "$eq" "$note";;
    skip) printf "   %-16s %-9s %-8s %-8s %-8s %-13s %s\n" "$lane" "-" "-" "-" "-" "SKIP" "$reason";;
    *)    printf "   %-16s %-9s %-8s %-8s %-8s %-13s %s\n" "$lane" "-" "-" "-" "-" "DNF" "${reason:-did-not-finish}";;
  esac
done

echo
echo "Reading it: 'equal-answer OK' means the lane reproduced the SAME Q1/Q2/Q3 answer as the independent"
echo "awk reference; a disagreeing lane is MISMATCH and is never counted. MongoDB is driven at max"
echo "concurrency on its node ($NCLIENTS concurrent clients over all cores; --numInsertionWorkers=$NCLIENTS on"
echo "load), and Q*_MS is the per-operation wall latency each client sees under that concurrent load."
rm -rf "$SCRATCH" 2>/dev/null || true
[ "${DOC_MISMATCH:-0}" = 0 ] || { echo; echo "PARITY FAIL: a lane's answer disagreed with the independent reference (MISMATCH above). Exiting non-zero so a wrong answer never passes green."; exit 3; }
