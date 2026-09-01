#!/usr/bin/env bash
# Semurg Benchmark Suite runner.
#   ./run.sh <domain>   # run one domain's incumbent lane(s) + the Semurg lane, then the equal-answer gate
#   ./run.sh all        # run every domain
#
# Domain names accept hyphens or not (time-series == timeseries, key-value == keyvalue).
# Requires: docker. The Semurg lane downloads/runs the RELEASED Semurg binary or image (see lanes/semurg/README.md);
# incumbents are pulled from their official public images. Each lane binds to 127.0.0.1 and tears down on exit.
set -uo pipefail
here="$(cd "$(dirname "$0")" && pwd)"
DOMAIN="${1:-}"

DOMAINS=(key-value graph time-series analytics search streaming relational object document vector universal)

if [ -z "${DOMAIN:-}" ]; then
  echo "usage: ./run.sh <domain>    # one domain, e.g. ./run.sh search"
  echo "       ./run.sh all        # every domain (pulls each incumbent image; can take a while)"
  echo "domains: ${DOMAINS[*]}"
  exit 2
fi

# Canonical domain name -> lane directory under lanes/ (dirs have no hyphens).
lane_dir() { echo "$here/lanes/$(echo "$1" | tr -d '-')"; }

run_domain() {
  local d="$1" dir; dir="$(lane_dir "$d")"
  echo "== domain: $d =="
  # Generate this domain's deterministic dataset (if a generator exists), into a shared per-domain dir
  # that both the incumbent lane and the Semurg lane read.
  local data="$here/.data/$(echo "$d" | tr -d '-')"; mkdir -p "$data"
  case "$d" in
    search)      BM25_DIR="$data" bash "$here/generators/search_corpus.sh" ;;
    time-series) TS_DIR="$data"   bash "$here/generators/ticks.sh" ;;
  esac
  if [ -d "$dir" ]; then
    for lane in "$dir"/*.sh; do
      [ -f "$lane" ] || continue
      echo "  -> incumbent: $(basename "$lane")"
      BM25_DIR="$data" TS_DIR="$data" bash "$lane" || echo "     lane failed: $(basename "$lane")"
    done
  else
    echo "  (no incumbent lane yet for '$d' - work in progress)"
  fi
  # The Semurg lane is the same script for every domain; it takes the domain as its argument.
  if [ -f "$here/lanes/semurg/semurg_lane.sh" ]; then
    echo "  -> semurg: semurg_lane.sh $d"
    bash "$here/lanes/semurg/semurg_lane.sh" "$d" || echo "     semurg lane failed"
  fi
}

# Normalise the requested domain to a canonical name (so 'timeseries' and 'time-series' both work).
canon() {
  local q; q="$(echo "$1" | tr -d '-')"
  for d in "${DOMAINS[@]}"; do [ "$(echo "$d" | tr -d '-')" = "$q" ] && { echo "$d"; return; }; done
  echo "$1"
}

if [ "$DOMAIN" = "all" ]; then
  for d in "${DOMAINS[@]}"; do run_domain "$d"; done
else
  run_domain "$(canon "$DOMAIN")"
fi
