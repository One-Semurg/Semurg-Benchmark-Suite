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
DOMAIN="${1:-all}"

DOMAINS=(key-value graph time-series analytics search streaming relational object document vector universal)

# Canonical domain name -> lane directory under lanes/ (dirs have no hyphens).
lane_dir() { echo "$here/lanes/$(echo "$1" | tr -d '-')"; }

run_domain() {
  local d="$1" dir; dir="$(lane_dir "$d")"
  echo "== domain: $d =="
  if [ -d "$dir" ]; then
    for lane in "$dir"/*.sh; do
      [ -f "$lane" ] || continue
      echo "  -> incumbent: $(basename "$lane")"
      bash "$lane" || echo "     lane failed: $(basename "$lane")"
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
