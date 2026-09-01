#!/usr/bin/env bash
# Semurg OLAP lane: the SAME narrow COUNT(*) GROUP BY the DuckDB side runs, over the SAME rows, via the
# installed release's eval (starts substrate_core, native engine engaged, THROWAWAY store). Reports BOTH
# numbers honestly: RAW SCAN (O(N) SIMD sweep) and FOLD-AT-INGEST (O(1) point-get). Emits:
#   LANE=semurg_olap STATUS=ok SCAN_RPS=.. FOLD_RPS=.. SELF_EQUAL=.. ANSWER=<hash>
set -uo pipefail; here="$(cd "$(dirname "$0")"&&pwd)"; . "$here/_common.sh"
EXS="${OLAP_EXS:?}"; SCRATCH="${OLAP_SCRATCH:?}"; mkdir -p "$SCRATCH"
REL="${SEMURG_REL_BIN:-/opt/semurg/bin/r11}"; [ -x "$REL" ] || REL="$(command -v r11 2>/dev/null || true)"
[ -n "$REL" ] && [ -x "$REL" ] || { echo "LANE=semurg_olap STATUS=skip REASON=release-not-installed(run:semurg-arena install)"; exit 0; }
[ -f "$EXS" ] || { echo "LANE=semurg_olap STATUS=skip REASON=olap-script-missing"; exit 0; }

D="$SCRATCH/semurg_olap_data"; mkdir -p "$D" "$(dirname "$(dirname "$REL")")/tmp" 2>/dev/null || true
[ -f /etc/semurg/semurg.env ] && SKB="$(sed -n 's/^SECRET_KEY_BASE=//p' /etc/semurg/semurg.env | head -1)"
OUT="$( \
  RELEASE_TMP="$(dirname "$(dirname "$REL")")/tmp" \
  SEMURG_DATA_DIR="$D" SEMURG_STRIPE_ROOTS="$D/s0" \
  SECRET_KEY_BASE="${SKB:-arena_scratch_secret_key_base_0000000000000000}" \
  PORT=4990 SEMURG_BIND=127.0.0.1 \
  OLAP_ROWS="${OLAP_ROWS:-20000000}" OLAP_GROUPS="${OLAP_GROUPS:-64}" OLAP_DIR="$D/store" \
  "$REL" eval "$(cat "$EXS")" 2>&1 )"
rm -rf "$D"
LINE="$(printf '%s\n' "$OUT" | grep -m1 '^SEMURG_OLAP ')"
[ -n "$LINE" ] || { echo "LANE=semurg_olap STATUS=dnf REASON=engine-error([$(printf '%s' "$OUT"|tr '\n' ' '|tail -c 100)])"; exit 0; }
sc=$(sed -n 's/.*scan_rps=\([0-9]*\).*/\1/p' <<<"$LINE")
fo=$(sed -n 's/.*fold_rps=\([0-9]*\).*/\1/p' <<<"$LINE")
se=$(sed -n 's/.*self_equal=\([a-z]*\).*/\1/p' <<<"$LINE")
an=$(sed -n 's/.*answer=\([0-9a-f]*\).*/\1/p' <<<"$LINE")
echo "LANE=semurg_olap STATUS=ok SCAN_RPS=$sc FOLD_RPS=$fo SELF_EQUAL=$se ANSWER=$an"
