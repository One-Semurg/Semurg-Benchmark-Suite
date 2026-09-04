#!/usr/bin/env bash
# Semurg VECTOR ANN lane -- the REAL fast Semurg-native vector path (SimHash / Hamming ANN), scored by
# RECALL@K against the exact L2 reference, NOT by exact-equal. This is the companion to the exact lane
# (lanes/semurg_vector.sh): that one is byte-correct but slow (an index-free full-scan over the FULL
# float vectors); THIS one is the approximate index the vector world actually competes on.
#
# HOW IT IS THE NATIVE PATH (no faked numbers):
#   * The SAME deterministic base/query FLOAT vectors every other vector lane sees are SimHash-encoded to
#     the founder-locked 44-byte (352-bit) sign-projection codes -- the substrate's native vector
#     representation (design/05 SimHash + bit-serial popcount MAC). Encoding = centre by the base mean
#     (translation is L2-invariant and balances the sign distribution), project onto 352 deterministic
#     sign hyperplanes, take the sign bit. Deterministic on any box.
#   * Each code is stored as ONE T_VEC 64B content-addressed container in a THROWAWAY store on the data
#     disk (never the live node's store), served back through the ONE deep-QD O_DIRECT conveyor.
#   * The query runs Shard.top_k_hamming -- the native index-free full Hamming scan (SIMD popcount, fanned
#     across every core, belt + prefetch + janitor on-path) -- the SAME native ANN the internal RAG seam
#     uses. Its top-K ids are compared to the EXACT L2 reference to MEASURE recall@K. SimHash is an
#     ANGULAR (cosine) LSH: on this dataset exact centred cosine overlaps the exact-L2 top-K at ~0.81, so
#     the codes are a good L2 proxy; the residual gap to that ceiling is the honest 1-bit-per-hyperplane
#     quantisation loss -- reported as whatever recall it achieves, never inflated.
#
# It emits ONE machine line the ANN board parses (labelled ANN/recall, NOT exact-equal):
#   LANE=semurg_vector_ann STATUS=ok-recall RECALL_AT_K=0.NN QUERY_MS=.. LOAD_MS=.. K=.. BITS=352 MODE=simhash-hamming
# If the release is not installed, or the store is not the native ANN surface, it emits a clean SKIP with
# the exact reason. It never crashes and never fabricates a recall or a latency (both come from the engine
# run or not at all).
set -uo pipefail; here="$(cd "$(dirname "$0")"&&pwd)"; . "$here/_common.sh"

# Shared VECTOR contract (identical vars every vector lane reads, set by vector_run.sh / the orchestrator).
BASE_CSV="${VEC_BASE_CSV:?}"; QUERY_CSV="${VEC_QUERY_CSV:?}"; K="${VEC_K:-10}"
# the independent exact-L2 reference (canonical answer text) sits beside the base csv as "<base>.answer.txt".
REF_ANSWER="${VEC_REF_ANSWER:-${BASE_CSV%.base.csv}.answer.txt}"
SCRATCH="${VEC_SCRATCH:-${TMPDIR:-/tmp}/arena_semurg_vector_ann.$$}"
QD="${SEMURG_QD:-64}"

REL="${SEMURG_REL_BIN:-/opt/semurg/bin/r11}"; [ -x "$REL" ] || REL="$(command -v r11 2>/dev/null || true)"
[ -n "$REL" ] && [ -x "$REL" ] || { echo "LANE=semurg_vector_ann STATUS=skip REASON=release-not-installed(run:semurg-arena install)"; exit 0; }
[ -f "$BASE_CSV" ] && [ -f "$QUERY_CSV" ] || { echo "LANE=semurg_vector_ann STATUS=skip REASON=vector-dataset-missing(base/query csv)"; exit 0; }
[ -f "$REF_ANSWER" ] || { echo "LANE=semurg_vector_ann STATUS=skip REASON=exact-reference-missing($REF_ANSWER; run gen_vectors.sh first)"; exit 0; }

D="$SCRATCH/semurg_vec_ann_data"; mkdir -p "$D/s0" "$(dirname "$(dirname "$REL")")/tmp" 2>/dev/null || true
EXS="$D/semurg_vec_ann.exs"
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
ref_path = System.fetch_env!("REF_ANSWER")
k = geti.("VEC_K", 10)
qd = geti.("SEMURG_QD", 64)
bits = 352            # 44-byte T_VEC code = 352 sign-projection bits (the substrate default width)

# read "id,f0,f1,..." -> [{id, [floats]}] in file order (ids 0..N-1 / qids 0..Q-1)
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
{_, first_vec} = hd(base)
dim = length(first_vec)

# --- deterministic SimHash (the founder-locked native vector representation: random sign-projection /
# bit-serial popcount MAC). Centre by the base mean, project onto `bits` +/-1 hyperplanes, take the sign
# bit -> a 352-bit angular LSH code. Same hyperplanes + same packing for base AND query => one code space.
sums = Enum.reduce(base, List.duplicate(0.0, dim), fn {_id, v}, acc ->
  Enum.zip_with(acc, v, fn a, x -> a + x end)
end)
mean = Enum.map(sums, fn s -> s / n end)

{hps, _} =
  Enum.map_reduce(1..bits, 987_654_321, fn _, s0 ->
    Enum.map_reduce(1..dim, s0, fn _, s ->
      s = rem(s * 1_103_515_245 + 12_345, 2_147_483_648)
      # sign from the HIGH bit (>= 2^30): the low-order bits of a power-of-2-modulus LCG are degenerate
      # (period ~2), so a `rem(s, 2)` sign would be a fixed alternating pattern, not a random hyperplane.
      {if(s >= 1_073_741_824, do: 1.0, else: -1.0), s}
    end)
  end)

centre = fn v -> Enum.zip_with(v, mean, fn a, m -> a - m end) end
encode = fn v ->
  cv = centre.(v)
  for(hp <- hps, into: <<>>, do: (
    dot = Enum.zip_reduce(cv, hp, 0.0, fn a, b, acc -> acc + a * b end)
    <<(if dot >= 0.0, do: 1, else: 0)::1>>
  ))
end

# LOAD: SimHash-encode the base set and ingest as T_VEC 64B containers into the throwaway store (the
# "index build" IS the ingest -- top_k_hamming is an index-free full scan, there is no separate index).
Enum.each(Path.wildcard(store <> "*"), &File.rm/1)
File.mkdir_p!(Path.dirname(store))
{:ok, h} = Shard.open(store)
sched = System.schedulers_online()
t_load = System.monotonic_time(:millisecond)
base
|> Enum.chunk_every(1000)
|> Task.async_stream(fn chunk -> for {id, v} <- chunk, do: Container.vec(id, encode.(v), 1) end,
     max_concurrency: sched, ordered: true, timeout: :infinity)
|> Enum.each(fn {:ok, io} -> {:ok, _} = Shard.append(h, IO.iodata_to_binary(io)) end)
Shard.checkpoint(h)
load_ms = System.monotonic_time(:millisecond) - t_load

# encode the query codes up front (not part of the timed query -- the query time is the native ANN scan).
qcodes = Enum.map(queries, fn {qid, v} -> {qid, encode.(v)} end)

# PROBE: confirm this store exposes the native ANN surface before timing/scoring (never fake a hit).
probe =
  case qcodes do
    [{_qid, c0} | _] -> Shard.top_k_hamming(h, c0, k, qd)
    _ -> {:error, :no_queries}
  end

case probe do
  {:error, reason} ->
    IO.puts("SEMURG_VEC_ANN status=skip reason=ann-surface-unavailable(#{inspect(reason)})")

  {:ok, _} ->
    # QUERY: the native SimHash/Hamming ANN -- Shard.top_k_hamming full Hamming scan on the ONE conveyor.
    Shard.reset_stats(h)
    a = Shard.conveyor_report()
    t0 = System.monotonic_time(:millisecond)
    ann =
      qcodes
      |> Enum.map(fn {qid, code} ->
        {:ok, pairs} = Shard.top_k_hamming(h, code, k, qd)   # [{dist, id}] ascending (dist, id)
        ids =
          pairs
          |> Enum.sort_by(fn {dd, id} -> {dd, id} end)
          |> Enum.take(k)
          |> Enum.map(fn {_dd, id} -> id end)
        {qid, MapSet.new(ids)}
      end)
      |> Enum.sort_by(fn {qid, _} -> qid end)
    query_ms = System.monotonic_time(:millisecond) - t0
    b = Shard.conveyor_report()
    d = fn key -> Map.get(b, key, 0) - Map.get(a, key, 0) end

    # RECALL@K vs the independent EXACT L2 reference (set overlap per query, averaged). ids come ONLY from
    # the engine's returned hits -- if the ANN missed, recall simply falls; nothing is papered over.
    ref =
      ref_path
      |> File.read!()
      |> String.trim()
      |> String.split(";")
      |> Enum.map(fn s -> s |> String.split(",") |> Enum.map(&String.to_integer/1) |> MapSet.new() end)
    ann_sets = Enum.map(ann, fn {_qid, set} -> set end)

    {hit, tot} =
      Enum.zip(ann_sets, ref)
      |> Enum.reduce({0, 0}, fn {a_set, r_set}, {hh, tt} ->
        {hh + MapSet.size(MapSet.intersection(a_set, r_set)), tt + MapSet.size(r_set)}
      end)

    recall = if tot > 0, do: hit / tot, else: 0.0

    IO.puts(
      "SEMURG_VEC_ANN status=ok load_ms=#{load_ms} query_ms=#{query_ms} recall=#{Float.round(recall, 4)}" <>
        " n=#{n} q=#{length(queries)} dim=#{dim} bits=#{bits} k=#{k}" <>
        " odirect_open_ok=#{d.(:odirect_open_ok)} odirect_open_fallback=#{d.(:odirect_open_fallback)}" <>
        " buffered_preads=#{d.(:buffered_preads)} sqes_submitted=#{d.(:sqes_submitted)}" <>
        " prefetch_sqes_ahead=#{d.(:prefetch_sqes_ahead)} fifo_hits=#{d.(:fifo_hits)}"
    )
end
EXS

OUT="$( \
  RELEASE_TMP="$(dirname "$(dirname "$REL")")/tmp" \
  SEMURG_DATA_DIR="$D" SEMURG_STRIPE_ROOTS="$D/s0" \
  SECRET_KEY_BASE="${SKB:-arena_scratch_secret_key_base_0000000000000000}" \
  PORT=4994 SEMURG_BIND=127.0.0.1 \
  BASE_CSV="$BASE_CSV" QUERY_CSV="$QUERY_CSV" REF_ANSWER="$REF_ANSWER" VEC_K="$K" SEMURG_QD="$QD" \
  SEMURG_STORE="$D/vec.bin" \
  "$REL" eval "$(cat "$EXS")" </dev/null 2>&1 )"
# NOTE: </dev/null is load-bearing -- `r11 eval` reads stdin; without it, it would DRAIN the orchestrator's
# `while read ... done < <(plan_lines)` loop input and swallow the lanes that follow this one.

rm -rf "$D" 2>/dev/null || true

LINE="$(printf '%s\n' "$OUT" | grep -m1 '^SEMURG_VEC_ANN ')"
if [ -z "$LINE" ]; then
  if printf '%s' "$OUT" | grep -qiE 'killed|out of memory|oom'; then
    echo "LANE=semurg_vector_ann STATUS=dnf REASON=oom-killed"
  else
    echo "LANE=semurg_vector_ann STATUS=dnf REASON=engine-error([$(printf '%s' "$OUT" | tr '\n' ' ' | tail -c 120)])"
  fi
  exit 0
fi
# clean SKIP if the store did not expose the native ANN surface.
if printf '%s' "$LINE" | grep -q 'status=skip'; then
  reason="$(sed -n 's/.*reason=\(.*\)/\1/p' <<<"$LINE")"
  echo "LANE=semurg_vector_ann STATUS=skip REASON=${reason:-ann-surface-unavailable}"; exit 0
fi
lm=$(sed -n 's/.*load_ms=\([0-9]*\).*/\1/p' <<<"$LINE")
qm=$(sed -n 's/.*query_ms=\([0-9]*\).*/\1/p' <<<"$LINE")
rc=$(sed -n 's/.*recall=\([0-9.]*\).*/\1/p' <<<"$LINE")
[ -n "$rc" ] || { echo "LANE=semurg_vector_ann STATUS=dnf REASON=no-recall-value"; exit 0; }
echo "LANE=semurg_vector_ann STATUS=ok-recall RECALL_AT_K=$rc QUERY_MS=${qm:-0} LOAD_MS=${lm:-0} K=$K BITS=352 MODE=simhash-hamming"
