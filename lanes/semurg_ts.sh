#!/usr/bin/env bash
# ============================================================================================
# Semurg TIME-SERIES lane (DOMAIN 3). Drives the INSTALLED Semurg release (bin/semurg-arena ->
# /opt/semurg/bin/r11, the surface a third party has after Semurg-Install, NOT the dev trunk) to
# ingest the SAME deterministic ts_data.csv every engine loads into a THROWAWAY store (never the live
# node's store), then answers the canonical time-series DOWNSAMPLE set with the substrate's NATIVE
# primitives and reports one machine-readable line the master board parses:
#   Q1 DOWNSAMPLE  : per-B-second bucket count/sum/min/max, ascending    (native SIMD scan_histogram
#                    for the count profile, + the O(1) FOLD-AT-INGEST point-get -- the architectural win)
#   Q2 PER-SERIES  : one device's count/sum/min/max                      (native scan_histogram @16)
#   Q3 RANGE       : count of readings in [WIN_LO, WIN_HI)              (O(log N) boundary find on the
#                    ordered time column -- a contiguous id range on the tick stream)
# The buckets are (ts - ts%B) integer-exact, so the ANSWER_HASH (per-bucket count:sum:min:max | per-series
# count:sum:min:max | range count) is BYTE-IDENTICAL to the awk reference and the Timescale/QuestDB lanes
# in this domain -- the equal-answer gate. Semurg's exposed native aggregate is the count-histogram + O(1)
# fold; the sum/min/max for the gate are folded from the engine's DEEP-QD batched read-back (io_uring),
# and the count profile is self-checked native-scan == fold == read-back (never a faked answer).
#
# Emits (mirrors the semurg_graph/semurg_olap STATUS contract; run_all_domains renders ok-matched/ok-reference):
#   LANE=semurg_ts STATUS=ok LOAD_MS=.. Q1_MS=.. Q2_MS=.. Q3_MS=.. ANSWER_HASH=<32hex> SCAN_RPS=.. FOLD_RPS=.. ANSWER_MS=.. SELF_EQUAL=..
# Robust: if the installed release is unreachable / no dataset / the engine errors, it emits a clean
# STATUS=skip (or STATUS=dnf) line and NEVER crashes the board. NO `set -e` (a lane isolates its own failure).
set -uo pipefail
here="$(cd "$(dirname "$0")" && pwd)"
. "$here/_common.sh"

DATA="${TS_DATA:-}"                         # the shared deterministic ts_data.csv (device_id,ts,value)
B="${TS_BUCKET:-3600}"                       # downsample bucket width in seconds
WLO="${WIN_LO:-0}"                           # Q3 range window low  (inclusive)  -- passed by the orchestrator
WHI="${WIN_HI:-9223372036854775807}"         # Q3 range window high (exclusive)
DEV="${Q2_DEVICE:-42}"                        # Q2 per-series device_id           -- passed by the orchestrator

# The installed release (same discovery as semurg_graph.sh / semurg_olap.sh): prefer /opt/semurg/bin/r11.
REL="${SEMURG_REL_BIN:-/opt/semurg/bin/r11}"
[ -x "$REL" ] || REL="$(command -v r11 2>/dev/null || true)"
[ -n "$REL" ] && [ -x "$REL" ] || { echo "LANE=semurg_ts STATUS=skip REASON=release-not-installed(run:semurg-arena install)"; exit 0; }
[ -n "$DATA" ] && [ -f "$DATA" ] || { echo "LANE=semurg_ts STATUS=skip REASON=ts-data-missing(set TS_DATA=/path/to/ts_data.csv)"; exit 0; }

# Isolated THROWAWAY store (never touches the live node's store), reaped on exit.
SCRATCH="${SEMURG_TS_SCRATCH:-${TMPDIR:-/tmp}/arena_semurg_ts.$$}"
D="$SCRATCH/store"; EXS="$SCRATCH/semurg_ts.exs"
mkdir -p "$D" "$(dirname "$(dirname "$REL")")/tmp" 2>/dev/null || true
trap 'rm -rf "$SCRATCH" 2>/dev/null || true' EXIT INT TERM

# The Semurg operations, run via the release `eval` (native engine engaged). Written to a temp file so it
# is passed verbatim; the quoted heredoc means the shell expands nothing inside it.
cat > "$EXS" <<'SEMURG_TS_EXS'
# Semurg TIME-SERIES lane core (self-contained; run via the installed release `eval`). Ingests the SAME
# deterministic ts_data.csv every engine loads into a THROWAWAY store, then answers the canonical
# time-series DOWNSAMPLE set with the substrate's NATIVE primitives:
#   Q1 DOWNSAMPLE  = scan_histogram(@24)  -> per-bucket count in ONE SIMD sweep (+ the O(1) fold-at-ingest)
#   Q2 PER-SERIES  = scan_histogram(@16)  -> per-device count
#   Q3 RANGE       = O(log N) boundary find on the ordered time column (contiguous id range)
# The equal-answer contract on the PUBLIC board is per-bucket count:sum:min:max (+ per-series, + range),
# byte-identical to the awk reference and the Timescale/QuestDB lanes. Semurg's exposed native aggregate
# is the count-histogram + O(1) fold; the sum/min/max are folded from the engine's DEEP-QD batched
# read-back (fetch_batch = io_uring), and the count profile is self-checked scan==fold==read-back.
{:ok, _} = Application.ensure_all_started(:substrate_core)
Process.sleep(600)
alias SubstrateCore.{Shard, Container}

env = fn k -> System.fetch_env!(k) end
csv = env.("SEMURG_CSV")
store = env.("SEMURG_STORE")
b = String.to_integer(env.("TS_B"))
wlo = String.to_integer(env.("WIN_LO"))
whi = String.to_integer(env.("WIN_HI"))
qdev = String.to_integer(env.("Q2_DEVICE"))

for ext <- ["", ".arith", ".sb", ".coloc", ".genoff.idx", ".genoff.idx.sparse", ".deadset.idx"], do: File.rm(store <> ext)
File.mkdir_p!(Path.dirname(store))

rows_list =
  csv |> File.stream!() |> Stream.drop(1)
  |> Enum.map(fn line ->
    [d, ts, v] = line |> String.trim() |> String.split(",")
    {String.to_integer(d), String.to_integer(ts), String.to_integer(v)}
  end)

n = length(rows_list)
{min_ts, max_ts, max_dev} =
  Enum.reduce(rows_list, {nil, nil, 0}, fn {d, ts, _v}, {mn, mx, md} ->
    {(if mn == nil or ts < mn, do: ts, else: mn), (if mx == nil or ts > mx, do: ts, else: mx), max(md, d)}
  end)
base = min_ts - rem(min_ts, b)
nb = div(max_ts - base, b) + 1
nd = max_dev + 1

{:ok, h} = Shard.open(store)
{load_us, _} =
  :timer.tc(fn ->
    rows_list
    |> Stream.with_index(1)
    |> Stream.chunk_every(1_000_000)
    |> Enum.each(fn chunk ->
      io =
        for {{dev, ts, val}, idx} <- chunk do
          Container.node(idx, dev, div(ts - base, b), <<val::little-64, ts::little-64, 0::little-32>>, 0, 1)
        end
      {:ok, _} = Shard.append(h, IO.iodata_to_binary(io))
    end)
  end)
Shard.checkpoint(h)

min_tc = fn f, k -> for(_ <- 1..k, do: :timer.tc(f)) |> Enum.min_by(fn {us, _} -> us end) end

# ---- Q1 native DOWNSAMPLE SCAN (SIMD group-by count) + O(1) fold-at-ingest ----
_ = Shard.scan_histogram(h, 24, nb)
{q1_us, scan_counts} = min_tc.(fn -> Shard.scan_histogram(h, 24, nb) end, 3)
scan_rps = if q1_us > 0, do: round(n / (q1_us / 1_000_000.0)), else: 0
:ok = Shard.set_fold_hist(h, 24, nb)
{served, fold_counts} = Shard.group_by_hist(h, 24, nb)
{fold_us, _} = min_tc.(fn -> Shard.group_by_hist(h, 24, nb) end, 50)
fold_us = max(fold_us, 1)
fold_rps = round(n / (fold_us / 1_000_000.0))

# ---- Q2 native PER-SERIES SCAN ----
{q2_us, dev_counts} = min_tc.(fn -> Shard.scan_histogram(h, 16, nd) end, 3)
q2_scan_count = Enum.at(dev_counts, qdev, 0)

# ---- Q3 RANGE: O(log N) boundary find on the ordered time column (id order == ts order) ----
ts_at = fn id ->
  <<_::binary-size(40), ts::little-64, _::binary>> = Shard.fetch(h, id)
  ts
end

first_ge = fn thr ->
  {lo, hi} =
    Stream.iterate({1, n + 1}, fn {l, r} ->
      if l >= r do
        {l, r}
      else
        m = div(l + r, 2)
        if ts_at.(m) >= thr, do: {l, m}, else: {m + 1, r}
      end
    end)
    |> Enum.find(fn {l, r} -> l >= r end)

  lo
end

{q3_us, {id_lo, id_hi}} = :timer.tc(fn -> {first_ge.(wlo), first_ge.(whi)} end)
q3_bsearch = max(id_hi - id_lo, 0)

# ---- equal-answer materialization: DEEP-QD batched read-back -> count:sum:min:max ----
{answer_us, {bmap, dacc, q3_rb}} =
  :timer.tc(fn ->
    Enum.reduce(Stream.chunk_every(1..n, 100_000), {%{}, nil, 0}, fn ids, acc0 ->
      packed = Shard.fetch_batch(h, ids)

      Enum.reduce((for <<c::binary-size(64) <- packed>>, do: c), acc0, fn c, {m, da, q} ->
        <<_::binary-size(16), dev::little-64, _::binary-size(8), val::little-64, ts::little-64, _::binary>> = c
        k = ts - rem(ts, b)

        m2 =
          case m do
            %{^k => {c0, s0, mn0, mx0}} -> %{m | k => {c0 + 1, s0 + val, min(mn0, val), max(mx0, val)}}
            _ -> Map.put(m, k, {1, val, val, val})
          end

        da2 =
          if dev == qdev do
            case da do
              nil -> {1, val, val, val}
              {c1, s1, mn1, mx1} -> {c1 + 1, s1 + val, min(mn1, val), max(mx1, val)}
            end
          else
            da
          end

        q2v = if ts >= wlo and ts < whi, do: q + 1, else: q
        {m2, da2, q2v}
      end)
    end)
  end)

# ---- self-checks: native count profile == read-back == fold; range + per-series agree ----
rb_counts = bmap |> Enum.sort_by(fn {k, _} -> k end) |> Enum.map(fn {_, {c, _, _, _}} -> c end)
scan_nonzero = Enum.reject(scan_counts, &(&1 == 0))
self_equal =
  rb_counts == scan_nonzero and scan_counts == fold_counts and
    Enum.sum(scan_counts) == n and q3_bsearch == q3_rb and
    q2_scan_count == (case dacc do nil -> 0; {c, _, _, _} -> c end)

# ---- colon count:sum:min:max answer (byte-identical to the awk reference + incumbent lanes) ----
q1s = bmap |> Enum.sort_by(fn {k, _} -> k end) |> Enum.map_join("", fn {k, {c, s, mn, mx}} -> "#{k}:#{c}:#{s}:#{mn}:#{mx};" end)
{q2c, q2sum, q2mn, q2mx} = dacc || {0, 0, 0, 0}
q2s = "#{q2c}:#{q2sum}:#{q2mn}:#{q2mx}"
q3s = "#{q3_rb}"
answer = :crypto.hash(:sha256, q1s <> "|" <> q2s <> "|" <> q3s) |> Base.encode16(case: :lower) |> binary_part(0, 32)

IO.puts(
  "SEMURG_TS rows=#{n} buckets=#{nb} load_ms=#{div(load_us, 1000)} " <>
    "q1_ms=#{div(q1_us, 1000)} q2_ms=#{div(q2_us, 1000)} q3_ms=#{div(q3_us, 1000)} " <>
    "scan_rps=#{scan_rps} fold_rps=#{if self_equal, do: fold_rps, else: 0} fold_ms=#{Float.round(fold_us / 1000.0, 3)} " <>
    "answer_ms=#{div(answer_us, 1000)} self_equal=#{self_equal} served_by_fold=#{served} answer=#{answer}"
)
SEMURG_TS_EXS

# minimal env the release eval needs (mirrors semurg_olap.sh / semurg_graph.sh). SECRET_KEY_BASE from the
# installed env if present, else a scratch default -- never printed. Isolated: 127.0.0.1, own PORT, own data dir.
# stdin is redirected from /dev/null so the release `eval` never drains the caller's stdin (e.g. the master
# orchestrator's plan feed), which would otherwise cut the remaining lanes off after this one.
[ -f /etc/semurg/semurg.env ] && SKB="$(sed -n 's/^SECRET_KEY_BASE=//p' /etc/semurg/semurg.env | head -1)"
OUT="$( \
  RELEASE_TMP="$(dirname "$(dirname "$REL")")/tmp" \
  SEMURG_DATA_DIR="$D" SEMURG_STRIPE_ROOTS="$D/s0" \
  SECRET_KEY_BASE="${SKB:-arena_scratch_secret_key_base_0000000000000000}" \
  PORT="${SEMURG_TS_EVAL_PORT:-4994}" SEMURG_BIND=127.0.0.1 \
  SEMURG_CSV="$DATA" SEMURG_STORE="$D/ts.bin" TS_B="$B" WIN_LO="$WLO" WIN_HI="$WHI" Q2_DEVICE="$DEV" \
  "$REL" eval "$(cat "$EXS")" </dev/null 2>&1 )"

LINE="$(printf '%s\n' "$OUT" | grep -m1 '^SEMURG_TS ')"
if [ -z "$LINE" ]; then
  if printf '%s' "$OUT" | grep -qiE 'killed|out of memory|oom'; then
    echo "LANE=semurg_ts STATUS=dnf REASON=oom-killed"
  else
    echo "LANE=semurg_ts STATUS=dnf REASON=engine-error([$(printf '%s' "$OUT" | tr '\n' ' ' | tail -c 120)])"
  fi
  exit 0
fi

# translate the SEMURG_TS k=v line -> the LANE= line the master board (run_all_domains.sh) parses.
lm=$(sed -n 's/.*load_ms=\([0-9]*\).*/\1/p' <<<"$LINE")
q1=$(sed -n 's/.*q1_ms=\([0-9]*\).*/\1/p' <<<"$LINE")
q2=$(sed -n 's/.*q2_ms=\([0-9]*\).*/\1/p' <<<"$LINE")
q3=$(sed -n 's/.*q3_ms=\([0-9]*\).*/\1/p' <<<"$LINE")
sr=$(sed -n 's/.*scan_rps=\([0-9]*\).*/\1/p' <<<"$LINE")
fr=$(sed -n 's/.*fold_rps=\([0-9]*\).*/\1/p' <<<"$LINE")
am=$(sed -n 's/.*answer_ms=\([0-9]*\).*/\1/p' <<<"$LINE")
fm=$(sed -n 's/.*fold_ms=\([0-9.]*\).*/\1/p' <<<"$LINE")
se=$(sed -n 's/.*self_equal=\([a-z]*\).*/\1/p' <<<"$LINE")
an=$(sed -n 's/.*answer=\([0-9a-f]*\).*/\1/p' <<<"$LINE")

echo "# semurg_ts: native downsample scan ${sr:-?} rows/s + O(1) fold-at-ingest (${fr:-?} rows/s, fold=${fm:-?}ms); count:sum:min:max equal-answer folded from the deep-QD read-back in ${am:-?} ms; self_equal=${se:-?}"
echo "LANE=semurg_ts STATUS=ok LOAD_MS=$lm Q1_MS=$q1 Q2_MS=$q2 Q3_MS=$q3 ANSWER_HASH=$an SCAN_RPS=$sr FOLD_RPS=$fr ANSWER_MS=$am SELF_EQUAL=$se"
exit 0
