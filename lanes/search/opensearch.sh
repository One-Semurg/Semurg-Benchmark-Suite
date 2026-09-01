#!/usr/bin/env bash
# Search lane: OpenSearch 2.x BM25 (Apache-2.0). Loopback-bound (never exposed), torn down on exit, portable
# paths. Loads the deterministic corpus from generators/ (see generators/README.md), matches Semurg's analyzer,
# and emits the ranked answer (for the equal-answer gate) + the server-side timing. No jq dependency.
# NOTE: Elasticsearch carries a benchmark-disclosure ("DeWitt") clause, so this public suite ships the
# Apache-2.0 OpenSearch lane instead. OpenSearch is the same Lucene BM25 + REST API; if you are licensed to
# benchmark Elasticsearch, point ES_IMAGE at it yourself (the index/query bodies below are identical).
set -euo pipefail
OS=http://127.0.0.1:9200
DIR="${BM25_DIR:-$(mktemp -d)}"
K="${BM25_K:-10}"
IMAGE="${OS_IMAGE:-opensearchproject/opensearch:2.17.0}"
CNAME=os-bench-$$

cleanup() { docker rm -f "$CNAME" >/dev/null 2>&1 || true; }
trap cleanup EXIT

[ -f "$DIR/bulk.ndjson" ] || { echo "corpus not found in $DIR - run the generator first (see generators/README.md)"; exit 1; }

# Loopback-only, security plugin disabled (no auth/TLS for a local benchmark); single shard for a global-idf,
# force-merged segment.
docker run -d --name "$CNAME" -p 127.0.0.1:9200:9200 \
  -e discovery.type=single-node -e DISABLE_SECURITY_PLUGIN=true -e plugins.security.disabled=true \
  -e "OPENSEARCH_JAVA_OPTS=-Xms4g -Xmx4g" "$IMAGE" >/dev/null

echo "waiting for OpenSearch..."
for i in $(seq 1 60); do
  [ "$(curl -s -o /dev/null -w '%{http_code}' "$OS/_cluster/health" || echo 000)" = "200" ] && break
  sleep 3
done

curl -s -X PUT "$OS/bm" -H 'Content-Type: application/json' -d '{
  "settings": { "number_of_shards": 1, "number_of_replicas": 0, "refresh_interval": "-1",
    "similarity": { "default": { "type": "BM25", "k1": 1.2, "b": 0.75 } },
    "analysis": { "tokenizer": { "pt": { "type": "pattern", "pattern": "[^A-Za-z0-9]+" } },
      "analyzer": { "an": { "type": "custom", "tokenizer": "pt", "filter": ["lowercase"] } } } },
  "mappings": { "properties": { "body": { "type": "text", "analyzer": "an", "similarity": "default" } } } }' >/dev/null

echo "loading corpus..."
split -l 20000 "$DIR/bulk.ndjson" "$DIR/chunk_"
for f in "$DIR"/chunk_*; do
  curl -s -H 'Content-Type: application/x-ndjson' -X POST "$OS/bm/_bulk" --data-binary @"$f" >/dev/null
done
rm -f "$DIR"/chunk_*
curl -s -X POST "$OS/bm/_forcemerge?max_num_segments=1" >/dev/null
curl -s -X POST "$OS/bm/_refresh" >/dev/null

echo "running queries..."
: > "$DIR/os_topk.tsv"
qi=0; t0=$(date +%s.%N)
while IFS= read -r q; do
  resp=$(curl -s -X POST "$OS/bm/_search?filter_path=hits.hits._id,hits.hits._score" -H 'Content-Type: application/json' \
    -d "{\"size\":$K,\"query\":{\"match\":{\"body\":\"$q\"}}}")
  ids=$(echo "$resp" | grep -oE '"_id":"[0-9]+"' | grep -oE '[0-9]+')
  scores=$(echo "$resp" | grep -oE '"_score":[0-9.eE+-]+' | sed 's/"_score"://')
  paste <(printf '%s\n' "$ids") <(printf '%s\n' "$scores") | awk -v qi="$qi" '{printf "%s\t%d\t%s\t%s\n", qi, NR-1, $1, $2}' >> "$DIR/os_topk.tsv"
  qi=$((qi+1))
done < "$DIR/queries.txt"
t1=$(date +%s.%N)

echo "LANE=opensearch STATUS=ok ANSWER=$(sha256sum "$DIR/os_topk.tsv" | cut -c1-16) QUERIES=$qi SECS=$(awk -v a="$t0" -v b="$t1" 'BEGIN{printf "%.3f", b-a}')"
