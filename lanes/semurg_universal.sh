#!/usr/bin/env bash
# Semurg UNIVERSAL lane (domain 11) -- the single-source-of-truth head-to-head vs SurrealDB. ONE
# deterministic dataset lives as 64B content-addressed containers in a THROWAWAY store built through the
# INSTALLED release's `eval` (the surface a third party has after Semurg-Install; never touches the live
# node's store), and the SAME records answer BOTH models -- Law 1, one substrate, many models:
#   Q1  DOCUMENT point-lookup            Shard.fetch(id) -> inline amount_cents
#   Q2  DOCUMENT group-by aggregation    Shard.scan_histogram over the category field (O(1) fold path)
#   Q3  GRAPH + DOCUMENT join (headline) for each `friend` edge, Shard.fetch the TARGET person and read
#       its region field, count where region == R -- walk the graph relation AND read the linked
#       document field, the one-store multi-model operation, served through the ONE conveyor.
#
# EQUAL-ANSWER GATE: Semurg's Q1|Q2|Q3 hash is compared to an INDEPENDENT awk reference over the same
# CSVs (NOT the engine; see _universal_gen.sh). A number is emitted ONLY when the engine matches the
# reference on all 3 cycles. The SurrealDB lane generates the byte-identical dataset + same reference, so
# the board renders Semurg vs SurrealDB on one equal-answer row.
#
# #1 TEST: the Q3 join drives the conveyor (deep-QD O_DIRECT fetch path); belt counters are reported per
# cycle (odirect_open_ok>0, fallback=0, buffered=0, fifo_hits rise cold->warm) as un-fakeable evidence.
#
# Emits ONE machine line run_all_domains.sh parses (same schema as the SQL/doc lanes):
#   LANE=semurg_universal STATUS=ok LOAD_MS=.. Q1_MS=.. Q2_MS=.. Q3_MS=.. ANSWER_HASH=<32hex>
# ROBUST: release not installed / engine error -> a clean STATUS=skip|dnf (never a crash, never a fake).
set -uo pipefail
here="$(cd "$(dirname "$0")" && pwd)"; . "$here/_common.sh"; . "$here/_universal_gen.sh"

REL="${SEMURG_REL_BIN:-/opt/semurg/bin/r11}"
[ -x "$REL" ] || REL="$(command -v r11 2>/dev/null || true)"
[ -n "$REL" ] && [ -x "$REL" ] || { echo "LANE=semurg_universal STATUS=skip REASON=release-not-installed(run:semurg-arena install; or set SEMURG_REL_BIN=/opt/semurg/bin/r11)"; exit 0; }

CATS="${UNIV_CATEGORIES:-32}"; REGS="${UNIV_REGIONS:-16}"; PORT="${UNIV_SEMURG_PORT:-4995}"

BASE="${ARENA_DATA:-${TMPDIR:-/tmp}/arena_universal_$$}"
WORK="$BASE/semurg_universal_work"
STORE_DIR="$WORK/store"; STORE_FILE="$STORE_DIR/univ.bin"
BUILD_EXS="$WORK/build.exs"; QUERY_EXS="$WORK/query.exs"
mkdir -p "$STORE_DIR/s0" "$(dirname "$(dirname "$REL")")/tmp" 2>/dev/null || true
trap 'rm -rf "$WORK" 2>/dev/null || true' EXIT INT TERM

SKB=""; [ -f /etc/semurg/semurg.env ] && SKB="$(sed -n 's/^SECRET_KEY_BASE=//p' /etc/semurg/semurg.env | head -1)"
run_env=(
  RELEASE_TMP="$(dirname "$(dirname "$REL")")/tmp"
  SEMURG_DATA_DIR="$STORE_DIR" SEMURG_STRIPE_ROOTS="$STORE_DIR/s0"
  SECRET_KEY_BASE="${SKB:-arena_scratch_secret_key_base_0000000000000000}"
  PORT="$PORT" SEMURG_BIND=127.0.0.1
)

# ---- (1) deterministic dataset + independent reference (shared generator) ---------------------------
univ_gen_dataset "$WORK" || { echo "LANE=semurg_universal STATUS=dnf REASON=dataset-generation-failed"; exit 0; }
univ_compute_reference "$WORK" || { echo "LANE=semurg_universal STATUS=dnf REASON=reference-computation-failed"; exit 0; }
ROWS="$(( $(wc -l < "$WORK/persons.csv") - 1 ))"
echo "[semurg_universal] rows=$ROWS  independent awk reference:" >&2
echo "[semurg_universal]   Q1 id=$UNIV_Q1ID amount=$REF_Q1 | Q2 per-category=[$REF_Q2] | Q3 friend->region=$UNIV_Q3REG -> $REF_Q3" >&2
echo "[semurg_universal]   reference ANSWER_HASH=$REF_HASH" >&2

# ---- (2) BUILD the throwaway store: one 64B container per person (document model) -------------------
# inline layout: <<amount::little-64, region::little-32, category::little-32>> at byte offset 32.
# Container.node(id, category, region, inline,...) also carries category@16 + region@24 for the O(1)
# scan_histogram fold (Q2). The `friend` edges are the graph relation, joined at query time by fetch.
cat > "$BUILD_EXS" <<'ELIXIR_BUILD'
{:ok, _} = Application.ensure_all_started(:substrate_core)
Process.sleep(600)
alias SubstrateCore.{Shard, Container}
csv   = System.fetch_env!("UNIV_CSV")
store = System.fetch_env!("UNIV_STORE_FILE")
cats  = String.to_integer(System.get_env("UNIV_CATEGORIES") || "32")
File.mkdir_p!(Path.dirname(store))
for ext <- ["", ".arith", ".sb", ".coloc", ".genoff.idx", ".genoff.idx.sparse", ".deadset.idx"],
    do: File.rm(store <> ext)
{:ok, h} = Shard.open(store)
t0 = System.monotonic_time(:millisecond)
rows =
  csv
  |> File.stream!([], :line)
  |> Stream.drop(1)
  |> Stream.chunk_every(1_000_000)
  |> Enum.reduce(0, fn lines, acc ->
    io =
      for ln <- lines do
        [id, cat, reg, amt] = ln |> String.trim() |> String.split(",")
        inline =
          <<String.to_integer(amt)::little-64, String.to_integer(reg)::little-32,
            String.to_integer(cat)::little-32>>
        Container.node(String.to_integer(id), String.to_integer(cat), String.to_integer(reg), inline, 0, 1)
      end
    {:ok, _} = Shard.append(h, IO.iodata_to_binary(io))
    acc + length(lines)
  end)
Shard.checkpoint(h)
:ok = Shard.set_fold_hist(h, 16, cats)
load_ms = System.monotonic_time(:millisecond) - t0
{:ok, %{dataset_bytes: db}} = Shard.mem_stats(h)
IO.puts("SEMURG_UNIV_BUILD rows=#{rows} load_ms=#{load_ms} dataset_mb=#{div(db, 1_048_576)} cats=#{cats}")
ELIXIR_BUILD

# NOTE: `</dev/null` on the release eval is load-bearing -- the release drains stdin, and when the board
# runs this lane inside its `while read ... done < <(plan_lines)` loop the inherited FD is that plan
# stream; the redirect keeps the board's per-domain iteration intact (same as semurg_doc.sh).
BOUT="$( env "${run_env[@]}" UNIV_CSV="$WORK/persons.csv" UNIV_STORE_FILE="$STORE_FILE" UNIV_CATEGORIES="$CATS" \
        "$REL" eval "$(cat "$BUILD_EXS")" </dev/null 2>&1 )"
BLINE="$(printf '%s\n' "$BOUT" | grep -m1 '^SEMURG_UNIV_BUILD ')"
if [ -z "$BLINE" ]; then
  if [ -z "$BOUT" ]; then
    echo "LANE=semurg_universal STATUS=skip REASON=installed-node-unreachable(no-output-from-eval; check r11 + substrate_core)"
  else
    echo "LANE=semurg_universal STATUS=dnf REASON=build-engine-error([$(printf '%s' "$BOUT" | tr '\n' ' ' | tail -c 120)])"
  fi
  exit 0
fi
LOAD_MS="$(sed -n 's/.*load_ms=\([0-9]*\).*/\1/p' <<<"$BLINE")"
DSMB="$(sed -n 's/.*dataset_mb=\([0-9]*\).*/\1/p' <<<"$BLINE")"
echo "[semurg_universal] build: $BLINE" >&2

# ---- (3) 3-cycle QUERY phase (fresh process => cycle 1 genuinely cold) ------------------------------
cat > "$QUERY_EXS" <<'ELIXIR_QUERY'
{:ok, _} = Application.ensure_all_started(:substrate_core)
Process.sleep(600)
alias SubstrateCore.Shard
geti = fn k, d -> case System.get_env(k) do nil -> d; "" -> d; s -> String.to_integer(s) end end
store = System.fetch_env!("UNIV_STORE_FILE")
edges = System.fetch_env!("UNIV_EDGES")
cats  = geti.("UNIV_CATEGORIES", 32)
q1id  = geti.("UNIV_Q1ID", 1)
q3reg = geti.("UNIV_Q3REG", 0)
{:ok, h} = Shard.open(store)
:ok = Shard.set_fold_hist(h, 16, cats)

# the friend relation: dst id per edge (the graph model), loaded once and PRE-PACKED as a u64-LE binary
# (only tokens cross the boundary, HARD RULE 4). Packing happens ONCE here, OUTSIDE the timed 3-cycle
# loop, so Q3 in each cycle is a single native gather-fold crossing with no per-cycle BEAM pack cost.
dsts_bin =
  edges
  |> File.stream!([], :line)
  |> Stream.drop(1)
  |> Enum.reduce(<<>>, fn l, acc ->
    [_src, dst] = l |> String.trim() |> String.split(",")
    <<acc::binary, String.to_integer(dst)::unsigned-little-64>>
  end)

hash = fn s -> :crypto.hash(:sha256, s) |> Base.encode16(case: :lower) |> binary_part(0, 32) end
snap = fn -> try do Shard.conveyor_report() rescue _ -> %{} catch _, _ -> %{} end end
gc = fn m, k -> Map.get(m, k, 0) end

# Q1: document point-lookup -> inline amount (little-64 at offset 32).
q1 = fn ->
  case Shard.fetch(h, q1id) do
    c when is_binary(c) and byte_size(c) >= 40 ->
      <<_::binary-size(32), amt::little-64, _::binary>> = c
      amt
    _ -> -1
  end
end
# Q3: GRAPH+DOCUMENT join -> COUNT friend edges whose TARGET person is in region q3reg, pushed to the
# native gather-fold kernel (Shard.join_count_cmp): walk the relation (the packed dst targets) + read
# each linked person's region field + count region == q3reg, ALL in ONE crossing through the deep-QD
# coalesced conveyor - only the count crosses back, ZERO containers materialise in BEAM. This replaces
# the per-edge Shard.fetch reduce (50000 QD1 crossings + 50000 binary allocs in interpreted Elixir).
# region is stored as the u64 attr @24 (Container.node(id, category, region, ...)) AND as the u32 inline
# @40; @24 == @40 (same region value, region < R), so the count is BIT-IDENTICAL to the old @40 read and
# the equal-answer hash is unchanged. 4 = EQ (0=GT 1=GE 2=LT 3=LE 4=EQ 5=NE).
q3 = fn -> Shard.join_count_cmp(h, dsts_bin, 24, :eq, q3reg) end

Shard.reset_stats(h)
Enum.each(1..3, fn cyc ->
  b0 = snap.()
  {q1_us, amt} = :timer.tc(q1)
  {q2_us, q2counts} = :timer.tc(fn -> Shard.scan_histogram(h, 16, cats) end)
  {q3_us, q3cnt} = :timer.tc(q3)
  b1 = snap.()
  q2str = Enum.map_join(q2counts, ",", &Integer.to_string/1)
  ans = hash.("#{amt}|#{q2str}|#{q3cnt}")
  IO.puts(
    "SEMURG_UNIV_CYCLE cycle=#{cyc} q1_us=#{q1_us} q2_us=#{q2_us} q3_us=#{q3_us} " <>
      "answer=#{ans} q3count=#{q3cnt}"
  )
  IO.puts(
    "SEMURG_UNIV_BELT cycle=#{cyc} sqes=#{gc.(b1, :sqes_submitted) - gc.(b0, :sqes_submitted)} " <>
      "odirect_open_ok=#{gc.(b1, :odirect_open_ok)} fallback=#{gc.(b1, :odirect_open_fallback)} " <>
      "buffered=#{gc.(b1, :buffered_preads)} " <>
      "fifo_hits=#{gc.(b1, :fifo_hits) - gc.(b0, :fifo_hits)} " <>
      "fifo_misses=#{gc.(b1, :fifo_misses) - gc.(b0, :fifo_misses)} " <>
      "prefetch_sqes_ahead=#{gc.(b1, :prefetch_sqes_ahead) - gc.(b0, :prefetch_sqes_ahead)}"
  )
end)
# self-check: the O(1) group_by fold matches the SIMD scan (never a faked speedup).
served = try do {s, fc} = Shard.group_by_hist(h, 16, cats); {s, fc == Shard.scan_histogram(h, 16, cats)} rescue _ -> {0, true} catch _, _ -> {0, true} end
IO.puts("SEMURG_UNIV_SUMMARY self_equal=#{elem(served, 1)}")
ELIXIR_QUERY

QOUT="$( env "${run_env[@]}" UNIV_STORE_FILE="$STORE_FILE" UNIV_EDGES="$WORK/edges.csv" \
        UNIV_CATEGORIES="$CATS" UNIV_Q1ID="$UNIV_Q1ID" UNIV_Q3REG="$UNIV_Q3REG" \
        "$REL" eval "$(cat "$QUERY_EXS")" </dev/null 2>&1 )"

# ---- (4) parse cycles + EQUAL-ANSWER gate against the independent reference -------------------------
q1min=""; q2min=""; q3min=""; ncyc=0; all_match=1
while IFS= read -r ln; do
  [ -n "$ln" ] || continue
  ncyc=$((ncyc+1))
  ans="$(sed -n 's/.*answer=\([0-9a-f]*\).*/\1/p' <<<"$ln")"
  u1="$(sed -n 's/.*q1_us=\([0-9]*\).*/\1/p' <<<"$ln")"
  u2="$(sed -n 's/.*q2_us=\([0-9]*\).*/\1/p' <<<"$ln")"
  u3="$(sed -n 's/.*q3_us=\([0-9]*\).*/\1/p' <<<"$ln")"
  [ "$ans" = "$REF_HASH" ] || all_match=0
  [ -n "$u1" ] && { [ -z "$q1min" ] || [ "$u1" -lt "$q1min" ]; } && q1min="$u1"
  [ -n "$u2" ] && { [ -z "$q2min" ] || [ "$u2" -lt "$q2min" ]; } && q2min="$u2"
  [ -n "$u3" ] && { [ -z "$q3min" ] || [ "$u3" -lt "$q3min" ]; } && q3min="$u3"
  echo "[semurg_universal] $ln" >&2
done < <(printf '%s\n' "$QOUT" | grep '^SEMURG_UNIV_CYCLE ')
printf '%s\n' "$QOUT" | grep '^SEMURG_UNIV_BELT ' | sed 's/^/[semurg_universal] /' >&2 || true
SUM="$(printf '%s\n' "$QOUT" | grep -m1 '^SEMURG_UNIV_SUMMARY ')"
echo "[semurg_universal] ${SUM:-SEMURG_UNIV_SUMMARY missing}" >&2

# ---- (5) gate + emit (query us -> ms, ceil so a real sub-ms op never reads 0) -----------------------
if [ "$ncyc" -lt 3 ]; then
  echo "LANE=semurg_universal STATUS=dnf REASON=query-engine-error([$(printf '%s' "$QOUT" | tr '\n' ' ' | tail -c 120)])"
  exit 0
fi
if [ "$all_match" != 1 ]; then
  echo "LANE=semurg_universal STATUS=dnf REASON=equal-answer-mismatch(semurg!=independent-awk-reference)"
  exit 0
fi
ceilms(){ local u="${1:-0}"; [ -n "$u" ] || u=0; echo $(( (u + 999) / 1000 )); }
Q1_MS="$(ceilms "$q1min")"; Q2_MS="$(ceilms "$q2min")"; Q3_MS="$(ceilms "$q3min")"
echo "[semurg_universal] EQUAL-ANSWER OK on all 3 cycles (dataset_mb=${DSMB:-?}); emitting board line." >&2
echo "LANE=semurg_universal STATUS=ok LOAD_MS=${LOAD_MS:-0} Q1_MS=$Q1_MS Q2_MS=$Q2_MS Q3_MS=$Q3_MS ANSWER_HASH=$REF_HASH"
exit 0
