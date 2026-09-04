#!/usr/bin/env bash
# Semurg RELATIONAL lane: the SAME three orders-board queries the sqlite/postgres/mysql/mariadb lanes
# run (ref d1ebda45), over the SAME deterministic orders.csv, but the TABLE IS THE SUBSTRATE. Each row
# is a 64-byte content-addressed container appended to a THROWAWAY native Shard via the INSTALLED
# release's `eval` (starts substrate_core, native engine engaged; never touches the live node's store):
#   order_id -> entity_id (the point-get key) | product_id -> type_tag@16 | amount_cents -> value@24.
# The rows are then read back through the engine's OWN read paths -- batched deep-QD io_uring scan
# (Shard.fetch_batch) for the two aggregates, a single point-get (Shard.fetch) for the by-id SELECT:
#   Q1  top-10 products by SUM(amount_cents) DESC, product_id ASC
#   Q2  point SELECT amount_cents WHERE order_id=424242   (empty when absent, exactly like SQL)
#   Q3  COUNT(*) WHERE amount_cents > 500000
# The three answer strings are hashed with the SHARED hash_answer() the incumbent lanes use, so the
# board gates this lane equal-answer against every SQL engine. NEVER a hardcoded/faked number: the
# number is produced by the installed engine and gated by the cross-engine answer hash. Emits one line:
#   LANE=semurg_relational LOAD_MS=.. Q1_MS=.. Q2_MS=.. Q3_MS=.. ANSWER_HASH=<hash>
# Robust: if the release is not installed or the data CSV is unreachable, emits a clean SKIP (never crashes).
set -uo pipefail; here="$(cd "$(dirname "$0")"&&pwd)"; . "$here/_common.sh"

# workload constants -- MUST match the SQL lanes' hardcoded by-id key + filter threshold (equal-answer).
Q2_ID="${REL_Q2_ID:-424242}"; Q3_THR="${REL_Q3_THR:-500000}"

# the installed release surface a third party has after `semurg-arena install` (NOT the dev trunk).
REL="${SEMURG_REL_BIN:-/opt/semurg/bin/r11}"
[ -x "$REL" ] || REL="$(command -v r11 2>/dev/null || true)"
[ -n "$REL" ] && [ -x "$REL" ] || { echo "LANE=semurg_relational STATUS=skip REASON=release-not-installed(run:semurg-arena install)"; exit 0; }

# the orders dataset (the board sets ARENA_DATA to the generated orders/ scratch dir holding orders.csv).
DATA="${ARENA_DATA:-}"
CSV=""
[ -n "$DATA" ] && [ -f "$DATA/orders.csv" ] && CSV="$DATA/orders.csv"
[ -z "$CSV" ] && [ -n "$DATA" ] && [ -f "$DATA" ] && CSV="$DATA"   # ARENA_DATA may already be the csv path
[ -n "$CSV" ] && [ -f "$CSV" ] || { echo "LANE=semurg_relational STATUS=skip REASON=orders-csv-missing(expected \$ARENA_DATA/orders.csv)"; exit 0; }

# throwaway store dir (never the live node's SEMURG_DATA_DIR); RELEASE_TMP under the release root.
SCRATCH="${REL_SCRATCH:-$(mktemp -d)}"; mkdir -p "$SCRATCH" "$(dirname "$(dirname "$REL")")/tmp" 2>/dev/null || true
D="$SCRATCH/semurg_rel_data"; mkdir -p "$D"
[ -f /etc/semurg/semurg.env ] && SKB="$(sed -n 's/^SECRET_KEY_BASE=//p' /etc/semurg/semurg.env | head -1)"

# the engine driver -- embedded so the lane is a single self-contained file. Quoted heredoc: the Elixir
# (with its #{} interpolation, <<>> binaries and $) is preserved verbatim, no shell expansion.
EXS="$D/rel.exs"
cat > "$EXS" <<'ELIXIR'
{:ok, _} = Application.ensure_all_started(:substrate_core)
Process.sleep(600)
alias SubstrateCore.{Shard, Container}

csv = System.get_env("REL_CSV") || raise "REL_CSV not set"
dir = System.get_env("REL_DIR") || "/tmp/semurg_rel_#{System.system_time(:millisecond)}"
q2_id = case System.get_env("REL_Q2_ID") do nil -> 424_242; "" -> 424_242; s -> String.to_integer(s) end
q3_thr = case System.get_env("REL_Q3_THR") do nil -> 500_000; "" -> 500_000; s -> String.to_integer(s) end

File.rm_rf(dir); File.mkdir_p!(dir)
path = Path.join(dir, "orders.bin")
{:ok, h} = Shard.open(path)
inline = <<0::160>>

# LOAD: parse orders.csv -> 64B containers -> append -> checkpoint (timed like the SQL .import + index).
t0 = System.monotonic_time(:microsecond)
max_id =
  File.stream!(csv, [], :line)
  |> Stream.drop(1)
  |> Stream.chunk_every(100_000)
  |> Enum.reduce(0, fn lines, acc ->
    {io, mx} =
      Enum.reduce(lines, {[], acc}, fn line, {io, mx} = st ->
        case line |> String.trim_trailing() |> String.split(",") do
          [oid, _cust, prod, amt, _ts] ->
            oid = String.to_integer(oid); prod = String.to_integer(prod); amt = String.to_integer(amt)
            {[Container.node(oid, prod, amt, inline, 0, 1) | io], max(mx, oid)}
          _ -> st
        end
      end)
    {:ok, _} = Shard.append(h, IO.iodata_to_binary(Enum.reverse(io)))
    mx
  end)
Shard.checkpoint(h)
load_ms = div(System.monotonic_time(:microsecond) - t0, 1000)

# batched deep-QD read of every row (ids 1..max_id, request order = id order), decoding product_id@16 +
# amount_cents@24 straight off the raw 64 bytes -> the engine's own read path (Shard.fetch_batch, io_uring).
scan_reduce = fn fun, acc0 ->
  Enum.reduce(1..max_id//50_000, acc0, fn lo, acc ->
    hi = min(lo + 50_000 - 1, max_id)
    packed = Shard.fetch_batch(h, Enum.to_list(lo..hi))
    reduce_bin = fn
      <<>>, a, _f -> a
      <<_::binary-size(16), prod::little-64, amt::little-64, _::binary-size(32), rest::binary>>, a, f ->
        f.(rest, fun.(prod, amt, a), f)
    end
    reduce_bin.(packed, acc, reduce_bin)
  end)
end

# Q1: GROUP BY product_id -> {sum(amount), count}; ORDER BY sum DESC, product_id ASC; top 10.
t1 = System.monotonic_time(:microsecond)
agg = scan_reduce.(fn prod, amt, m -> Map.update(m, prod, {amt, 1}, fn {s, c} -> {s + amt, c + 1} end) end, %{})
q1 =
  agg
  |> Enum.sort_by(fn {pid, {sum, _c}} -> {-sum, pid} end)
  |> Enum.take(10)
  |> Enum.map_join("", fn {pid, {sum, cnt}} -> "#{pid}:#{sum}:#{cnt};" end)
q1_ms = div(System.monotonic_time(:microsecond) - t1, 1000)

# Q2: point SELECT amount_cents WHERE order_id=q2_id. Absent -> "" (matches SQL empty result). A real
# node's first byte is non-zero (type nibble 0x1); nil/all-zero => absent, so this is data-independent.
t2 = System.monotonic_time(:microsecond)
q2 =
  case Shard.fetch(h, q2_id) do
    <<first::8, _::binary-size(23), amt::little-64, _::binary>> = c when byte_size(c) == 64 and first != 0 ->
      Integer.to_string(amt)
    _ -> ""
  end
q2_ms = div(System.monotonic_time(:microsecond) - t2, 1000)

# Q3: COUNT(*) WHERE amount_cents > q3_thr.
t3 = System.monotonic_time(:microsecond)
q3 = scan_reduce.(fn _prod, amt, n -> if(amt > q3_thr, do: n + 1, else: n) end, 0) |> Integer.to_string()
q3_ms = div(System.monotonic_time(:microsecond) - t3, 1000)

File.rm_rf(dir)

# The three answer strings go out VERBATIM (no newline inside any of them) so the bash wrapper hashes
# q1|q2|q3 with the shared hash_answer(), byte-identical to the sqlite/postgres lanes.
IO.puts("SEMURG_REL_META rows=#{max_id} load_ms=#{load_ms} q1_ms=#{q1_ms} q2_ms=#{q2_ms} q3_ms=#{q3_ms}")
IO.puts("SEMURG_REL_Q1 " <> q1)
IO.puts("SEMURG_REL_Q2 " <> q2)
IO.puts("SEMURG_REL_Q3 " <> q3)
ELIXIR

OUT="$( \
  RELEASE_TMP="$(dirname "$(dirname "$REL")")/tmp" \
  SEMURG_DATA_DIR="$D" SEMURG_STRIPE_ROOTS="$D/s0" \
  SECRET_KEY_BASE="${SKB:-arena_scratch_secret_key_base_0000000000000000}" \
  PORT="${REL_PORT:-4989}" SEMURG_BIND=127.0.0.1 \
  REL_CSV="$CSV" REL_DIR="$D/store" REL_Q2_ID="$Q2_ID" REL_Q3_THR="$Q3_THR" \
  "$REL" eval "$(cat "$EXS")" </dev/null 2>&1 )"   # </dev/null: BEAM must NOT drain the board loop's stdin

# clean up the throwaway store no matter the outcome (never leave a scratch store behind).
[ -n "${REL_SCRATCH:-}" ] || rm -rf "$SCRATCH" 2>/dev/null || true

META="$(printf '%s\n' "$OUT" | grep -m1 '^SEMURG_REL_META ')"
if [ -z "$META" ]; then
  if printf '%s' "$OUT" | grep -qiE 'killed|out of memory|oom'; then
    echo "LANE=semurg_relational STATUS=skip REASON=oom-killed(raise the box budget or lower SEMURG_ARENA_ROWS)"
  else
    echo "LANE=semurg_relational STATUS=skip REASON=engine-unreachable([$(printf '%s' "$OUT" | tr '\n' ' ' | tail -c 110)])"
  fi
  exit 0
fi

lm=$(sed -n 's/.*load_ms=\([0-9]*\).*/\1/p' <<<"$META")
q1m=$(sed -n 's/.*q1_ms=\([0-9]*\).*/\1/p' <<<"$META")
q2m=$(sed -n 's/.*q2_ms=\([0-9]*\).*/\1/p' <<<"$META")
q3m=$(sed -n 's/.*q3_ms=\([0-9]*\).*/\1/p' <<<"$META")
q1=$(printf '%s\n' "$OUT" | sed -n 's/^SEMURG_REL_Q1 //p' | head -1)
q2=$(printf '%s\n' "$OUT" | sed -n 's/^SEMURG_REL_Q2 //p' | head -1)
q3=$(printf '%s\n' "$OUT" | sed -n 's/^SEMURG_REL_Q3 //p' | head -1)

# equal-answer hash: the SAME hash_answer() (_common.sh) the sqlite/postgres/mysql/mariadb lanes use.
echo "LANE=semurg_relational LOAD_MS=$lm Q1_MS=$q1m Q2_MS=$q2m Q3_MS=$q3m ANSWER_HASH=$(hash_answer "$q1|$q2|$q3")"
