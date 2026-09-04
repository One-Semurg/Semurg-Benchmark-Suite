#!/usr/bin/env bash
# Semurg VECTOR lane: EXACT top-K nearest-neighbour (squared-L2) over vectors STORED IN and SERVED BY the
# installed Semurg release (SubstrateCore engine, THROWAWAY store -- never touches the live node's store).
# The full float vectors are ingested as 64B content-addressed containers (LOAD), then the query streams
# EVERY base vector back through the ONE deep-QD O_DIRECT conveyor (read_vec_codes -> the coalesced belt)
# and computes the exact top-K -- an index-free full scan, fanned across every core (Task.async_stream),
# byte-identical to the independent awk / FAISS IndexFlatL2 reference. This is the SAME exact query the
# other vector lanes answer, so it is directly equal-answer gate-able (matches the generator reference).
#
# HONESTY: this is the EXACT brute-force top-K, NOT the Semurg-native SimHash/Hamming ANN (that is an
# approximation of cosine/L2 -- 44-byte codes + top_k_hamming, recall-measured in the internal harness).
# Only an EXACT answer can be hashed bit-exact against the exact reference, so this lane answers the exact
# query and NEVER fakes it: the top-K is really computed over the vectors Semurg served through its belt.
#
# Emits ONE line the orchestrator parses:
#   LANE=semurg_vector STATUS=ok LOAD_MS=.. QUERY_MS=.. ANSWER_HASH=<32hex>
# Robust: if the release is not installed it SKIPs with the exact fix; an engine error is a clean DNF. It
# never crashes and never emits a fabricated number (the hash is produced by the engine run or not at all).
set -uo pipefail; here="$(cd "$(dirname "$0")"&&pwd)"; . "$here/_common.sh"

# Shared VECTOR contract (identical vars every vector lane reads, set by the orchestrator / vector_run.sh).
BASE_CSV="${VEC_BASE_CSV:?}"; QUERY_CSV="${VEC_QUERY_CSV:?}"; K="${VEC_K:-10}"
SCRATCH="${VEC_SCRATCH:-${TMPDIR:-/tmp}/arena_semurg_vector.$$}"

REL="${SEMURG_REL_BIN:-/opt/semurg/bin/r11}"; [ -x "$REL" ] || REL="$(command -v r11 2>/dev/null || true)"
[ -n "$REL" ] && [ -x "$REL" ] || { echo "LANE=semurg_vector STATUS=skip REASON=release-not-installed(run:semurg-arena install)"; exit 0; }
[ -f "$BASE_CSV" ] && [ -f "$QUERY_CSV" ] || { echo "LANE=semurg_vector STATUS=skip REASON=vector-dataset-missing(base/query csv)"; exit 0; }

D="$SCRATCH/semurg_vec_data"; mkdir -p "$D/s0" "$(dirname "$(dirname "$REL")")/tmp" 2>/dev/null || true
EXS="$D/semurg_vec_l2.exs"
[ -f /etc/semurg/semurg.env ] && SKB="$(sed -n 's/^SECRET_KEY_BASE=//p' /etc/semurg/semurg.env | head -1)"

# ---- the driver (embedded so the lane is one self-contained file inside lanes/) ----
cat > "$EXS" <<'EXS'
{:ok, _} = Application.ensure_all_started(:substrate_core)
Process.sleep(300)
alias SubstrateCore.{Shard, Container}

geti = fn k, d -> case System.get_env(k) do nil -> d; "" -> d; s -> String.to_integer(s) end end
store = System.fetch_env!("SEMURG_STORE")
base_csv = System.fetch_env!("BASE_CSV")
query_csv = System.fetch_env!("QUERY_CSV")
k = geti.("VEC_K", 10)

# each vector -> 12 T_VEC containers holding its 512-byte float64 payload (44 code bytes each, 12*44=528>=512).
codes_per_vec = 12
ids_per_vec = 16

parse = fn path ->
  path
  |> File.stream!()
  |> Stream.map(&String.trim/1)
  |> Stream.reject(&(&1 == ""))
  |> Enum.map(fn line ->
    [id | rest] = String.split(line, ",")
    {String.to_integer(id), Enum.map(rest, &String.to_float/1)}
  end)
end

base = parse.(base_csv)
queries = parse.(query_csv) |> Enum.sort_by(fn {qid, _} -> qid end)
n = length(base)

pack = fn vals -> for(f <- vals, into: <<>>, do: <<f::float-little-64>>) end
split_codes = fn payload ->
  for j <- 0..(codes_per_vec - 1) do
    off = j * 44
    len = min(44, max(0, byte_size(payload) - off))
    if len > 0, do: binary_part(payload, off, len), else: <<>>
  end
end

# LOAD: ingest full float vectors as 64B containers into the throwaway store.
Enum.each(Path.wildcard(store <> "*"), &File.rm/1)
File.mkdir_p!(Path.dirname(store))
{:ok, h} = Shard.open(store)
t_load = System.monotonic_time(:millisecond)
base
|> Enum.chunk_every(4096)
|> Enum.each(fn chunk ->
  io =
    for {id, vals} <- chunk, {code, j} <- Enum.with_index(split_codes.(pack.(vals))) do
      Container.vec(id * ids_per_vec + j, code, 1)
    end
  {:ok, _} = Shard.append(h, IO.iodata_to_binary(io))
end)
Shard.checkpoint(h)
load_ms = System.monotonic_time(:millisecond) - t_load

# QUERY: exact top-K squared-L2 in the Rust AVX-512 kernel over the SAME belt (Shard.top_k_l2) -- ONE NIF
# call, ALL queries, fanned across cores IN RUST. The Elixir side only marshals ids in/out: no boxed-float
# list decode, no per-query O(N log N) sort. The kernel does the index-free tiered scan (belt + prefetch +
# janitor, so the conveyor counters below reflect it) AND the diff-square + bounded-heap top-K itself. This
# is the SAME exact top-K over the SAME vectors, so the equal-answer hash is unchanged -- just fast.
Shard.reset_stats(h)
a = Shard.conveyor_report()

# vector dimension D (from the parsed base) and the row-major q x d f32 query matrix the kernel takes.
dim = length(elem(hd(base), 1))
qblob = for {_qid, v} <- queries, f <- v, into: <<>>, do: <<f::float-little-32>>
read_ms = 0

t0 = System.monotonic_time(:millisecond)
{:ok, tk} = Shard.top_k_l2(h, qblob, length(queries), dim, k)
ans =
  tk
  |> Enum.map(fn pairs ->
    pairs |> Enum.map(fn {_dist, id} -> Integer.to_string(id) end) |> Enum.join(",")
  end)
  |> Enum.join(";")
query_ms = System.monotonic_time(:millisecond) - t0

hash = :crypto.hash(:sha256, ans) |> Base.encode16(case: :lower) |> binary_part(0, 32)
b = Shard.conveyor_report()
d = fn key -> Map.get(b, key, 0) - Map.get(a, key, 0) end

IO.puts(
  "SEMURG_VEC load_ms=#{load_ms} query_ms=#{query_ms} read_ms=#{read_ms} answer=#{hash} n=#{n} codes=#{n * codes_per_vec}" <>
    " odirect_open_ok=#{d.(:odirect_open_ok)} odirect_open_fallback=#{d.(:odirect_open_fallback)}" <>
    " buffered_preads=#{d.(:buffered_preads)} sqes_submitted=#{d.(:sqes_submitted)}" <>
    " prefetch_sqes_ahead=#{d.(:prefetch_sqes_ahead)} fifo_hits=#{d.(:fifo_hits)}"
)
EXS

OUT="$( \
  RELEASE_TMP="$(dirname "$(dirname "$REL")")/tmp" \
  SEMURG_DATA_DIR="$D" SEMURG_STRIPE_ROOTS="$D/s0" \
  SECRET_KEY_BASE="${SKB:-arena_scratch_secret_key_base_0000000000000000}" \
  PORT=4995 SEMURG_BIND=127.0.0.1 \
  BASE_CSV="$BASE_CSV" QUERY_CSV="$QUERY_CSV" VEC_K="$K" SEMURG_STORE="$D/vec.bin" \
  "$REL" eval "$(cat "$EXS")" </dev/null 2>&1 )"
# NOTE: </dev/null is load-bearing. `r11 eval` reads stdin; without this redirect it would DRAIN the
# orchestrator's `while read ... done < <(plan_lines)` loop input and silently swallow the lanes that
# follow this one in the same domain. Redirecting from /dev/null isolates the engine run from that FD.

rm -rf "$D" 2>/dev/null || true

LINE="$(printf '%s\n' "$OUT" | grep -m1 '^SEMURG_VEC ')"
if [ -z "$LINE" ]; then
  if printf '%s' "$OUT" | grep -qiE 'killed|out of memory|oom'; then
    echo "LANE=semurg_vector STATUS=dnf REASON=oom-killed"
  else
    echo "LANE=semurg_vector STATUS=dnf REASON=engine-error([$(printf '%s' "$OUT" | tr '\n' ' ' | tail -c 100)])"
  fi
  exit 0
fi
lm=$(sed -n 's/.*load_ms=\([0-9]*\).*/\1/p' <<<"$LINE")
qm=$(sed -n 's/.*query_ms=\([0-9]*\).*/\1/p' <<<"$LINE")
an=$(sed -n 's/.*answer=\([0-9a-f]*\).*/\1/p' <<<"$LINE")
[ -n "$an" ] || { echo "LANE=semurg_vector STATUS=dnf REASON=no-answer-hash"; exit 0; }
echo "LANE=semurg_vector STATUS=ok LOAD_MS=${lm:-0} QUERY_MS=${qm:-0} ANSWER_HASH=$an"
