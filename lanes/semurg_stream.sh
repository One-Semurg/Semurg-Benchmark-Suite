#!/usr/bin/env bash
# Semurg STREAMING lane (domain 9). Maps the streaming domain onto the substrate's NATIVE append-only
# log, driven through the INSTALLED release's `eval` in a THROWAWAY store -- exactly the shape the wired
# semurg_olap.sh / semurg_graph.sh lanes use (never touches the live node's store, never the dev trunk).
#
#   PRODUCE  == Shard.append        : stream a deterministic ordered log (key,seq,value) into 64B
#                                     content-addressed containers -- the exact write path a stream
#                                     producer drives, expressed on the ONE substrate. Timed = LOAD_MS.
#   REPLAY   == Shard.scan_histogram: a full-log replay that FOLDS as it reads (disk -> RAM -> L3 -> L2
#                                     -> L1 -> ALU through the ONE conveyor; the belt warms cold->warm
#                                     across a warm-up call + min-of-3). Timed = QUERY_MS.
#
# EQUAL-ANSWER = the EXACT REPLAYED SET, order-independent: the value histogram (offset 24, G buckets) |
# the per-key histogram (offset 16, K buckets) | the total count. Two engines that replay the SAME stream
# produce the SAME hash regardless of arrival order -- a no-loss / no-dup / no-reorder replay proof.
#
# Self-contained: the deterministic generator and the workload are embedded (same LCG shape as the suite
# generators), so a third party gets ONE file and a REAL measured number. If the release is not installed
# the lane emits a clean SKIP (never crashes). Emits ONE machine line run_all_domains.sh parses:
#   LANE=semurg_stream STATUS=ok LOAD_MS=.. QUERY_MS=.. PRODUCE_RPS=.. CONSUME_RPS=.. ANSWER=<hash>
set -uo pipefail
here="$(cd "$(dirname "$0")"&&pwd)"; . "$here/_common.sh"

# scratch: run_all_domains.sh passes ARENA_DATA for domain 9; else a local default (standalone run).
SCRATCH="${ARENA_DATA:-${STREAM_SCRATCH:-${TMPDIR:-/tmp}/arena_stream}}"; mkdir -p "$SCRATCH"

# deterministic dataset knobs (same seed -> byte-identical stream -> reproducible replay hash).
ROWS="${SEMURG_STREAM_ROWS:-2000000}"          # ordered log length (seq 1..ROWS, strictly increasing)
KEYS="${SEMURG_STREAM_KEYS:-100}"              # routing/partition keys 1..KEYS (per-key rollup buckets)
VALUE_MOD="${SEMURG_STREAM_VALUE_MOD:-10000}"  # payload magnitude 1..VALUE_MOD
G="${STREAM_GBUCKETS:-64}"                      # value histogram buckets (value % G)

# locate the INSTALLED release binary a third party has after Semurg-Install (never the dev trunk).
REL="${SEMURG_REL_BIN:-/opt/semurg/bin/r11}"; [ -x "$REL" ] || REL="$(command -v r11 2>/dev/null || true)"
[ -n "$REL" ] && [ -x "$REL" ] || { echo "LANE=semurg_stream STATUS=skip REASON=release-not-installed(run:semurg-arena install)"; exit 0; }
command -v awk >/dev/null 2>&1 || { echo "LANE=semurg_stream STATUS=skip REASON=awk-not-available"; exit 0; }

D="$SCRATCH/semurg_stream_data"; rm -rf "$D" 2>/dev/null || true
mkdir -p "$D" "$(dirname "$(dirname "$REL")")/tmp" 2>/dev/null || true
DATA="$D/stream_data.csv"; EXS="$D/semurg_stream.exs"

# ---- 1) stand up the stream deterministically: key,seq,value; seq strictly increasing (log offset). ----
#      draw from the HIGH bits (int(s/256)%m) -- the glibc-style LCG has weak low-order bits.
awk -v n="$ROWS" -v nkey="$KEYS" -v vmod="$VALUE_MOD" 'BEGIN{
  s=1234567; printf "key,seq,value\n";
  for(i=1;i<=n;i++){
    s=(s*1103515245+12345)%2147483648; key=1 + int(s/256)%nkey;
    s=(s*1103515245+12345)%2147483648; val=1 + int(s/256)%vmod;
    printf "%d,%d,%d\n", key, i, val;
  }
}' > "$DATA" 2>/dev/null || { echo "LANE=semurg_stream STATUS=skip REASON=data-gen-failed"; rm -rf "$D"; exit 0; }

# ---- 2) the embedded workload (produce -> replay -> per-key rollup -> count), run by the release eval. ----
cat > "$EXS" <<'STREAM_EXS_EOF'
# Semurg STREAMING core on the substrate's native append-only log.
#   PRODUCE  == Shard.append          (pack each (key,seq,value) row into ONE 64B node container)
#   REPLAY   == Shard.scan_histogram  (full-log fold; belt warms cold->warm, min-of-3)
# Container layout: entity_id@8 = seq (offset), type_tag@16 = key, attr_bitmap@24 = value.
{:ok, _} = Application.ensure_all_started(:substrate_core)
Process.sleep(600)
alias SubstrateCore.{Shard, Container}

env_int = fn k, d -> case System.get_env(k) do nil -> d; "" -> d; s -> String.to_integer(s) end end
data = System.fetch_env!("STREAM_DATA")
g = env_int.("STREAM_GBUCKETS", 64)
kbk = env_int.("STREAM_KEYS", 100)
dir = System.get_env("STREAM_DIR") || "/tmp/semurg_stream_#{System.system_time(:millisecond)}"

File.rm_rf(dir); File.mkdir_p!(dir)
path = Path.join(dir, "stream.bin")
{:ok, h} = Shard.open(path)
inline = <<0::160>>

# ---- PRODUCE: stream the CSV, pack each row into a 64B container, batch-append to the log. ----
{produce_us, n} =
  :timer.tc(fn ->
    data
    |> File.stream!([], :line)
    |> Stream.drop(1)
    |> Stream.chunk_every(200_000)
    |> Enum.reduce(0, fn lines, acc ->
      io =
        for l <- lines, into: <<>> do
          [k, s, v] = l |> String.trim_trailing() |> String.split(",", parts: 3)
          Container.node(String.to_integer(s), String.to_integer(k), String.to_integer(v), inline, 0, 1)
        end

      {:ok, _} = Shard.append(h, io)
      acc + length(lines)
    end)
  end)

Shard.checkpoint(h)
produce_us = max(produce_us, 1)
produce_rps = round(n / (produce_us / 1_000_000.0))

# ---- REPLAY (Q1): full-log scan folding the VALUE histogram (offset 24, G buckets). ----
_ = Shard.scan_histogram(h, 24, g)
{q1_us, q1_counts} =
  for(_ <- 1..3, do: :timer.tc(fn -> Shard.scan_histogram(h, 24, g) end)) |> Enum.min_by(fn {us, _} -> us end)

q1_us = max(q1_us, 1)
consume_rps = round(n / (q1_us / 1_000_000.0))

# ---- Q2 PER-KEY rollup: histogram of the key field (offset 16, K buckets). ----
{q2_us, q2_counts} =
  for(_ <- 1..3, do: :timer.tc(fn -> Shard.scan_histogram(h, 16, kbk) end)) |> Enum.min_by(fn {us, _} -> us end)

# ---- Q3 COUNT: replayed length == sum of the histogram (no-loss / no-dup proof). ----
{q3_us, total} = :timer.tc(fn -> Enum.sum(q1_counts) end)

# equal-answer = the EXACT replayed set, order-independent: value-hist | per-key-hist | count.
q1s = Enum.map_join(q1_counts, ",", &Integer.to_string/1)
q2s = Enum.map_join(q2_counts, ",", &Integer.to_string/1)
answer =
  :crypto.hash(:sha256, "#{q1s}|#{q2s}|#{total}")
  |> Base.encode16(case: :lower)
  |> binary_part(0, 32)

File.rm_rf(dir)

IO.puts(
  "SEMURG_STREAM rows=#{n} produce_ms=#{div(produce_us, 1000)} produce_rps=#{produce_rps} " <>
    "q1_ms=#{div(q1_us, 1000)} consume_rps=#{consume_rps} q2_ms=#{div(q2_us, 1000)} " <>
    "q3_ms=#{div(q3_us, 1000)} answer=#{answer}"
)
STREAM_EXS_EOF

# ---- 3) drive the INSTALLED node in a THROWAWAY store (separate PORT/data dir; live node untouched). ----
[ -f /etc/semurg/semurg.env ] && SKB="$(sed -n 's/^SECRET_KEY_BASE=//p' /etc/semurg/semurg.env | head -1)"
STRIPE="${SEMURG_STRIPE_ROOTS:-$D/s0}"
OUT="$( \
  RELEASE_TMP="$(dirname "$(dirname "$REL")")/tmp" \
  SEMURG_DATA_DIR="$D" SEMURG_STRIPE_ROOTS="$STRIPE" \
  SECRET_KEY_BASE="${SKB:-arena_scratch_secret_key_base_0000000000000000}" \
  PORT=4992 SEMURG_BIND=127.0.0.1 \
  STREAM_DATA="$DATA" STREAM_GBUCKETS="$G" STREAM_KEYS="$KEYS" STREAM_DIR="$D/store" \
  "$REL" eval "$(cat "$EXS")" </dev/null 2>&1 )"   # detach stdin: the release eval must not drain the caller's stream
rm -rf "$D" 2>/dev/null || true

LINE="$(printf '%s\n' "$OUT" | grep -m1 '^SEMURG_STREAM ')"
if [ -z "$LINE" ]; then
  if printf '%s' "$OUT" | grep -qiE 'killed|out of memory|oom'; then
    echo "LANE=semurg_stream STATUS=dnf REASON=oom-killed"
  else
    echo "LANE=semurg_stream STATUS=dnf REASON=engine-error([$(printf '%s' "$OUT" | tr '\n' ' ' | tail -c 120)])"
  fi
  exit 0
fi
# translate the SEMURG_STREAM k=v line -> the LANE= line run_all_domains.sh parses.
pm=$(sed -n 's/.*produce_ms=\([0-9]*\).*/\1/p' <<<"$LINE")
pr=$(sed -n 's/.*produce_rps=\([0-9]*\).*/\1/p' <<<"$LINE")
rm=$(sed -n 's/.*q1_ms=\([0-9]*\).*/\1/p' <<<"$LINE")
cr=$(sed -n 's/.*consume_rps=\([0-9]*\).*/\1/p' <<<"$LINE")
an=$(sed -n 's/.*answer=\([0-9a-f]*\).*/\1/p' <<<"$LINE")
echo "LANE=semurg_stream STATUS=ok LOAD_MS=$pm QUERY_MS=$rm PRODUCE_RPS=$pr CONSUME_RPS=$cr ANSWER=$an"
