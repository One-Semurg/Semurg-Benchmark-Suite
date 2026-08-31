#!/usr/bin/env bash
# Deterministic search corpus generator (no dependencies beyond awk). Produces, into $BM25_DIR:
#   bulk.ndjson  -- Elasticsearch _bulk format: {"index":{"_id":N}}\n{"body":"<doc>"}  (one doc per 2 lines)
#   docs.tsv     -- id<TAB>body                (the same corpus, for any lane that wants plain rows)
#   queries.txt  -- one query per line          (drawn from the frequent tail of the vocab)
# The corpus is a seeded Zipf mixture, so every machine generates byte-identical data (equal-answer safe).
#
# Usage: BM25_DIR=/tmp/bm25bench NDOCS=50000 NQUERIES=200 SEED=1 ./search_corpus.sh
set -euo pipefail
DIR="${BM25_DIR:-/tmp/bm25bench}"
NDOCS="${NDOCS:-50000}"
NQUERIES="${NQUERIES:-200}"
SEED="${SEED:-1}"
VOCAB="${VOCAB:-2000}"          # distinct terms
DLEN="${DLEN:-24}"              # mean doc length (terms)
mkdir -p "$DIR"

awk -v ndocs="$NDOCS" -v nq="$NQUERIES" -v seed="$SEED" -v vocab="$VOCAB" -v dlen="$DLEN" \
    -v bulk="$DIR/bulk.ndjson" -v tsv="$DIR/docs.tsv" -v qf="$DIR/queries.txt" '
# deterministic LCG (glibc constants) -> u in [0,1); uses global s
function rnd(){ s=(s*1103515245+12345)%2147483648; return s/2147483648.0 }
# binary-search the cumulative Zipf table cum[] (global) for a sampled term id
function zipf(  u,lo,hi,mid){ u=rnd(); lo=0; hi=vocab-1
  while(lo<hi){ mid=int((lo+hi)/2); if(cum[mid]<u) lo=mid+1; else hi=mid }
  return lo }
BEGIN{
  s = seed % 2147483647; if (s<=0) s+=2147483646
  Z=0; for(i=0;i<vocab;i++){ w[i]=1.0/(i+1); Z+=w[i] }
  c=0; for(i=0;i<vocab;i++){ c+=w[i]/Z; cum[i]=c }
  for(d=0; d<ndocs; d++){
    L = int(dlen*0.5 + rnd()*dlen) + 1
    body=""
    for(t=0;t<L;t++){ tid=zipf(); body = body (t?" ":"") "term" tid }
    printf "{\"index\":{\"_id\":%d}}\n{\"body\":\"%s\"}\n", d, body >> bulk
    printf "%d\t%s\n", d, body >> tsv
  }
  # queries: 2-3 frequent terms each (the head of the Zipf, so they actually match many docs)
  for(q=0;q<nq;q++){
    n = 2 + int(rnd()*2)
    line=""
    for(t=0;t<n;t++){ tid = int(rnd()*rnd()*vocab); line = line (t?" ":"") "term" tid }
    print line >> qf
  }
}' </dev/null

echo "wrote $DIR/bulk.ndjson ($(wc -l < "$DIR/bulk.ndjson") lines = $NDOCS docs), $DIR/docs.tsv, $DIR/queries.txt ($NQUERIES queries)  [seed=$SEED, deterministic]"
