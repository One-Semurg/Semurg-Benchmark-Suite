#!/usr/bin/env bash
# Semurg SEARCH lane (board domain #10). Stands up a deterministic full-text document corpus, ingests it
# into a THROWAWAY store STRIPED across sub-shards via the INSTALLED release's `eval` (never touches the
# live node's store), then runs the domain's four SEARCH operations three cycles (cold -> warm -> warm)
# through the UNIVERSAL substrate primitives only, and GATES the engine's answer against an INDEPENDENT
# awk reference before it reports a single number. Same corpus knobs => byte-identical corpus on any box,
# so a future public incumbent lane (opensearch) over the same corpus reproduces the SAME equal-answer.
#
# The four canonical operations (identical to every lane in this domain):
#   S1  full-text term match  -> COUNT of docs whose topic field contains the term (the boolean full-text
#                                result SET; the vocabulary is chosen so token-match == byte-substring-match,
#                                so this COUNT identifies the SAME doc-id set on every engine over this
#                                deterministic corpus). Semurg: Shard/StripedShard.scan_pattern (SIMD binary
#                                pattern over the raw 64B container bytes off the io_uring conveyor, no index).
#   S2  exact term filter     -> COUNT of docs with category == CAT   (one category slice of the histogram).
#   S3  terms aggregation     -> per-category doc COUNTS [c0,c1,c2]   (COUNT(*) GROUP BY category).
#   S4  numeric range filter  -> COUNT of docs with priority >= PRI   (priority slice of the same fold).
# S2/S3/S4 come from ONE scan_histogram(field 24, 24 buckets) fold: bucket b => category=div(b,8),
# priority=rem(b,8) (each doc container stores category*8+priority at offset 24). BM25 relevance RANKING
# is the incumbent's home turf and is a SEPARATE, non-gated metric, never claimed here.
#
# EQUAL-ANSWER (the gate). The canonical answer string "S1=..;S2=..;S3=c0,c1,c2;S4=.." is hashed with the
# domain rule (hash_answer = first 32 hex of sha256, no trailing newline). The lane computes this reference
# INDEPENDENTLY with an engine-agnostic awk pass over the corpus, then requires the engine to reproduce it
# EXACTLY -- a disagreement emits STATUS=dnf, never a green number. run_all_domains.sh gates search as
# first-ok-lane, so this establishes the cross-engine reference (ok-reference); the same hash from a
# later incumbent lane renders ok-matched.
#
# Emits ONE line run_all_domains.sh parses (LOAD_MS + QUERY_MS + ANSWER are the parsed fields; the rest is
# honest evidence a third party can read):
#   LANE=semurg_search STATUS=ok LOAD_MS=<int> QUERY_MS=<int> COLD_MS=<f> WARM_MS=<f> WARM2_MS=<f> \
#     QUERY_US=<int> S1=.. S2=.. S3=c0,c1,c2 S4=.. SELF_EQUAL=.. ANSWER=<32hex> BELT=<conveyor counters>
# QUERY_MS is the WARM2 (steady-state, cache-served) query-set wall; COLD/WARM/WARM2 show the 3-cycle belt
# warming. If the installed node is unreachable the lane emits a clean SKIP with the exact fix (never crash).
set -uo pipefail
here="$(cd "$(dirname "$0")" && pwd)"; . "$here/_common.sh"

# ---- scratch (run_all_domains.sh passes ARENA_DATA; else self-contained temp) ----
SCRATCH="${ARENA_DATA:-${SEARCH_SCRATCH:-${TMPDIR:-/tmp}/semurg_search_$$}}"
mkdir -p "$SCRATCH" 2>/dev/null || { echo "LANE=semurg_search STATUS=skip REASON=scratch-unwritable($SCRATCH)"; exit 0; }

# ---- the INSTALLED release a third party has after Semurg-Install (NOT the dev trunk) ----
REL="${SEMURG_REL_BIN:-/opt/semurg/bin/r11}"
[ -x "$REL" ] || REL="$(command -v r11 2>/dev/null || true)"
[ -n "$REL" ] && [ -x "$REL" ] || { echo "LANE=semurg_search STATUS=skip REASON=release-not-installed(run:semurg-arena install)"; exit 0; }

# ---- deterministic corpus knobs (identical knobs => byte-identical corpus on any box) ----
DOCS="${SEARCH_DOCS:-200000}"       # corpus size (docs)
SEED="${SEARCH_SEED:-1234567}"      # LCG seed (determinism)
NCAT="${SEARCH_NCAT:-3}"            # category cardinality 0..NCAT-1
S1_TERM="${SEARCH_S1_TERM:-foxtrot}"
S2_CAT="${SEARCH_S2_CAT:-1}"
S4_PRI="${SEARCH_S4_PRI:-4}"        # priority range: >= this (priority is 1..5)
# 12 distinct words, NONE a substring of another, so a token match and a raw byte-substring match count the
# SAME docs -> the boolean full-text answer is bit-exact across engines.
VOCAB="alpha bravo charlie delta echo foxtrot golf hotel india juliet kilo lima"

corpus="$SCRATCH/search.corpus.tsv"
meta="$SCRATCH/search.meta"
refans="$SCRATCH/search.answer.txt"

# ---- 1. corpus: one LCG stream; each draw uses HIGH bits (drops the 17 poor low bits of a power-of-2 LCG)
#         so topic/body/category/priority are uniform + well-spread. body's primary token is `topic`. ----
awk -v n="$DOCS" -v seed="$SEED" -v ncat="$NCAT" -v vocab="$VOCAB" 'BEGIN{
  m=split(vocab, W, " "); s=seed; OFS="\t";
  for(i=1;i<=n;i++){
    s=(s*1103515245+12345)%2147483648; ti=1+int(s/131072)%m;
    s=(s*1103515245+12345)%2147483648; a=1+int(s/131072)%m;
    s=(s*1103515245+12345)%2147483648; b=1+int(s/131072)%m;
    s=(s*1103515245+12345)%2147483648; cat=int(s/131072)%ncat;
    s=(s*1103515245+12345)%2147483648; pri=1+int(s/131072)%5;
    body="the " W[ti] " subsystem streams through the " W[a] " " W[b] " tier";
    print i, W[ti], body, cat, pri;
  }
}' > "$corpus" 2>/dev/null || { echo "LANE=semurg_search STATUS=skip REASON=corpus-gen-failed"; rm -rf "$SCRATCH" 2>/dev/null; exit 0; }

printf "N=%s NCAT=%s S1_TERM=%s S2_CAT=%s S4_PRI=%s VOCAB=%s\n" \
  "$DOCS" "$NCAT" "$S1_TERM" "$S2_CAT" "$S4_PRI" "$(echo "$VOCAB" | tr ' ' ',')" > "$meta"

# ---- 2. INDEPENDENT reference answers -- a single O(N) awk pass over the corpus text (no engine). ----
read -r R_S1 R_S2 R_S3 R_S4 < <(awk -v term="$S1_TERM" -v c="$S2_CAT" -v ncat="$NCAT" -v p="$S4_PRI" '
  BEGIN{ FS="\t"; s1=0; s4=0; for(k=0;k<ncat;k++) g[k]=0 }
  { topic=$2; cat=$4+0; pri=$5+0;
    if(topic==term) s1++;
    g[cat]++;
    if(pri>=p) s4++;
  }
  END{
    s2=g[c];
    grp=""; for(k=0;k<ncat;k++){ grp=(k==0?g[k]:grp","g[k]) }
    print s1, s2, grp, s4;
  }' "$corpus")
REF_ANS="S1=${R_S1};S2=${R_S2};S3=${R_S3};S4=${R_S4}"
printf '%s' "$REF_ANS" > "$refans"
REF_HASH="$(hash_answer "$REF_ANS")"

# ---- 3. write the Semurg native driver (uses ONLY the universal public engine API) to scratch ----
EXS="$SCRATCH/semurg_search.exs"
cat > "$EXS" <<'SEMURG_SEARCH_EXS_EOF'
# Semurg native SEARCH driver (run via `<release> eval`, exactly as the graph/olap lanes are driven).
# Ingests the shared corpus into a THROWAWAY StripedShard (write-fan across all mounts), then runs the same
# four SEARCH ops three cycles (cold -> warm -> warm) through the UNIVERSAL substrate primitives only.
defmodule SemurgSearchLane do
  alias SubstrateCore.{Shard, Container, StripedShard}

  def env(k, d) do
    case System.get_env(k) do
      nil -> d
      "" -> d
      v -> v
    end
  end

  def env_int(k, d), do: String.to_integer(env(k, to_string(d)))

  def meta(path) do
    File.read!(path)
    |> String.split()
    |> Enum.reduce(%{}, fn tok, acc ->
      case String.split(tok, "=", parts: 2) do
        [k, v] -> Map.put(acc, k, v)
        _ -> acc
      end
    end)
  end

  # process-global conveyor / janitor / prefetch counters (un-fakeable). Guarded: an older release that
  # lacks the report yields an empty map, and the belt evidence is reported as n/a (honest, never faked).
  def belt do
    Shard.conveyor_report()
  rescue
    _ -> %{}
  catch
    _, _ -> %{}
  end

  def g(m, k), do: Map.get(m, k, 0) || 0

  def run do
    corpus = env("SEARCH_CORPUS_TSV", "")
    metap  = env("SEARCH_META", "")
    disks  = env("SEARCH_DISKS", "/data0,/data1") |> String.split(",", trim: true)

    unless corpus != "" and File.exists?(corpus) and metap != "" and File.exists?(metap) do
      IO.puts("SEMURG_SEARCH status=skip reason=corpus-or-meta-missing")
      System.halt(0)
    end

    m = meta(metap)
    term = Map.get(m, "S1_TERM", "foxtrot")
    s2cat = String.to_integer(Map.get(m, "S2_CAT", "1"))
    s4pri = String.to_integer(Map.get(m, "S4_PRI", "4"))
    ncat  = String.to_integer(Map.get(m, "NCAT", "3"))

    ts = System.system_time(:millisecond)
    mounts = Enum.with_index(disks) |> Enum.map(fn {d, i} ->
      dir = Path.join([d, "bench_search_#{ts}"]); File.mkdir_p!(dir)
      Path.join(dir, "search_s#{i}.bin")
    end)

    st = StripedShard.open(mounts)

    # 64B node container per doc: entity_id=doc_id, type_tag@16=priority, attr_bitmap@24=(category*8+priority)
    # [the ONE histogram-fold field], inline@32=topic word bytes (what scan_pattern matches).
    t0 = System.monotonic_time(:millisecond)
    corpus
    |> File.stream!()
    |> Stream.map(fn line ->
      [id, topic, _body, cat, pri] = String.split(String.trim_trailing(line, "\n"), "\t")
      idi = String.to_integer(id); cati = String.to_integer(cat); prii = String.to_integer(pri)
      Container.node(idi, prii, cati * 8 + prii, topic)
    end)
    |> Stream.chunk_every(1_000_000)
    |> Enum.each(fn chunk -> :ok = StripedShard.append(st, IO.iodata_to_binary(chunk)) end)
    Enum.each(st.subs, &Shard.checkpoint/1)
    load_ms = System.monotonic_time(:millisecond) - t0

    matches = fn tuple -> tuple |> Tuple.to_list() |> hd() end
    # one query cycle: S1 pattern-match count + the histogram fold (S2/S3/S4 derived). Return {us, s1, hist}.
    cycle = fn ->
      c0 = System.monotonic_time(:microsecond)
      s1 = StripedShard.scan_pattern(st, term) |> matches.()
      hist = StripedShard.scan_histogram(st, 24, 24)
      us = System.monotonic_time(:microsecond) - c0
      {us, s1, hist}
    end

    b_pre = belt()
    {c1_us, s1, hist} = cycle.()
    b_cold = belt()
    {c2_us, _, _} = cycle.()
    {c3_us, _, hist3} = cycle.()
    b_warm = belt()

    # derive S2/S3/S4 from the histogram: bucket b => category = div(b,8), priority = rem(b,8).
    cat_count = fn k -> Enum.slice(hist, 8 * k, 8) |> Enum.sum() end
    s3 = for k <- 0..(ncat - 1), do: cat_count.(k)
    s2 = Enum.at(s3, s2cat)
    s4 = hist |> Enum.with_index() |> Enum.reduce(0, fn {c, b}, acc ->
      if rem(b, 8) >= s4pri, do: acc + c, else: acc
    end)

    total = Enum.sum(hist)
    n = env_int("SEARCH_DOCS_EXPECT", total)
    self_equal = hist == hist3 and total == n

    answer = "S1=#{s1};S2=#{s2};S3=#{Enum.join(s3, ",")};S4=#{s4}"

    Enum.each(mounts, fn p -> Enum.each(["", ".arith", ".sb", ".coloc"], fn e -> File.rm(p <> e) end) end)
    Enum.each(disks, fn d -> File.rm_rf(Path.join(d, "bench_search_#{ts}")) end)

    belt_str =
      "odok:#{g(b_warm, :odirect_open_ok)}," <>
      "fallback:#{g(b_warm, :odirect_open_fallback)}," <>
      "buffered:#{g(b_warm, :buffered_preads)}," <>
      "fifo_hits_cold:#{g(b_cold, :fifo_hits) - g(b_pre, :fifo_hits)}," <>
      "fifo_hits_warm:#{g(b_warm, :fifo_hits) - g(b_cold, :fifo_hits)}," <>
      "prefetch_ahead:#{g(b_warm, :prefetch_sqes_ahead)}," <>
      "relocated:#{g(b_warm, :relocated_containers)}," <>
      "pages_before:#{g(b_warm, :pages_before)}," <>
      "pages_after:#{g(b_warm, :pages_after)}," <>
      "bytes_consumed:#{g(b_warm, :bytes_consumed)}"

    IO.puts(
      "SEMURG_SEARCH status=ok load_ms=#{load_ms} " <>
      "c1_ms=#{Float.round(c1_us / 1000, 3)} c2_ms=#{Float.round(c2_us / 1000, 3)} c3_ms=#{Float.round(c3_us / 1000, 3)} " <>
      "c3_us=#{c3_us} s1=#{s1} s2=#{s2} s3=#{Enum.join(s3, ",")} s4=#{s4} self_equal=#{self_equal} " <>
      "answer=#{answer} belt=#{belt_str}"
    )
  rescue
    e -> IO.puts("SEMURG_SEARCH status=dnf reason=#{Exception.message(e) |> String.replace("\n", " ") |> String.slice(0, 160)}")
  end
end

case Application.ensure_all_started(:substrate_core) do
  {:ok, _} ->
    Process.sleep(600)
    SemurgSearchLane.run()
  other ->
    IO.puts("SEMURG_SEARCH status=skip reason=substrate_core-not-started(#{inspect(other) |> String.slice(0, 120)})")
end
SEMURG_SEARCH_EXS_EOF

# ---- 4. drive the installed release via `eval` into a THROWAWAY store (never the live node's store) ----
# store dirs to stripe the throwaway shard across: default = two sub-shards under scratch (robust on any
# box, never assumes cluster disks). Set SEARCH_DISKS=/data0,/data1 to exercise both NVMe pipes on-cluster.
DISKS="${SEARCH_DISKS:-$SCRATCH/s0,$SCRATCH/s1}"
STORE="$SCRATCH/relstore_$$"
RELDIR="$(dirname "$(dirname "$REL")")"; mkdir -p "$RELDIR/tmp" 2>/dev/null || true
[ -f /etc/semurg/semurg.env ] && SKB="$(sed -n 's/^SECRET_KEY_BASE=//p' /etc/semurg/semurg.env | head -1)"

OUT="$( \
  RELEASE_TMP="$RELDIR/tmp" \
  SEMURG_DATA_DIR="$STORE" SEMURG_STRIPE_ROOTS="$STORE/s0" \
  SECRET_KEY_BASE="${SKB:-arena_scratch_secret_key_base_0000000000000000}" \
  PORT="${SEMURG_EVAL_PORT:-4994}" SEMURG_BIND=127.0.0.1 \
  SEARCH_CORPUS_TSV="$corpus" SEARCH_META="$meta" SEARCH_DISKS="$DISKS" SEARCH_DOCS_EXPECT="$DOCS" \
  "$REL" eval "$(cat "$EXS")" </dev/null 2>&1 )"

# tidy every throwaway artefact (store + striped mounts + corpus); never touch the live node's store.
rm -rf "$STORE" 2>/dev/null || true
[ -z "${ARENA_DATA:-}" ] && rm -rf "$SCRATCH" 2>/dev/null || true

# ---- 5. parse the engine's canonical line ----
LINE="$(printf '%s\n' "$OUT" | grep -m1 '^SEMURG_SEARCH ')"
if [ -z "$LINE" ]; then
  if printf '%s' "$OUT" | grep -qiE 'killed|out of memory|oom'; then
    echo "LANE=semurg_search STATUS=dnf REASON=oom-killed"
  else
    echo "LANE=semurg_search STATUS=dnf REASON=engine-error([$(printf '%s' "$OUT" | tr '\n' ' ' | tail -c 140)])"
  fi
  exit 0
fi
status="$(sed -n 's/.*status=\([a-z]*\).*/\1/p' <<<"$LINE")"
if [ "$status" != ok ]; then
  reason="$(sed -n 's/.*reason=\(.*\)/\1/p' <<<"$LINE")"
  echo "LANE=semurg_search STATUS=${status:-dnf} REASON=${reason:-native-lane-not-ok}"
  exit 0
fi

lm="$(sed -n 's/.*\bload_ms=\([0-9]*\).*/\1/p' <<<"$LINE")"
c1="$(sed -n 's/.*\bc1_ms=\([0-9.]*\).*/\1/p' <<<"$LINE")"
c2="$(sed -n 's/.*\bc2_ms=\([0-9.]*\).*/\1/p' <<<"$LINE")"
c3="$(sed -n 's/.*\bc3_ms=\([0-9.]*\).*/\1/p' <<<"$LINE")"
c3us="$(sed -n 's/.*\bc3_us=\([0-9]*\).*/\1/p' <<<"$LINE")"
s1="$(sed -n 's/.*\bs1=\([0-9]*\).*/\1/p' <<<"$LINE")"
s2="$(sed -n 's/.*\bs2=\([0-9]*\).*/\1/p' <<<"$LINE")"
s3="$(sed -n 's/.*\bs3=\([0-9,]*\).*/\1/p' <<<"$LINE")"
s4="$(sed -n 's/.*\bs4=\([0-9]*\).*/\1/p' <<<"$LINE")"
se="$(sed -n 's/.*\bself_equal=\([a-z]*\).*/\1/p' <<<"$LINE")"
belt="$(sed -n 's/.*\bbelt=\([^ ]*\).*/\1/p' <<<"$LINE")"

# ---- 6. EQUAL-ANSWER GATE: the engine must reproduce the INDEPENDENT awk reference exactly ----
ENG_ANS="S1=${s1};S2=${s2};S3=${s3};S4=${s4}"
ENG_HASH="$(hash_answer "$ENG_ANS")"
if [ "$ENG_HASH" != "$REF_HASH" ]; then
  echo "LANE=semurg_search STATUS=dnf REASON=equal-answer-mismatch(engine[$ENG_ANS]!=ref[$REF_ANS])"
  exit 0
fi

# QUERY_MS = the warm2 (steady-state, cache-served) query-set wall, integer ms; COLD/WARM/WARM2 show the belt.
qms="$(awk -v v="${c3:-0}" 'BEGIN{printf "%.0f", v}')"
echo "LANE=semurg_search STATUS=ok LOAD_MS=${lm:-0} QUERY_MS=${qms:-0} COLD_MS=${c1:-0} WARM_MS=${c2:-0} WARM2_MS=${c3:-0} QUERY_US=${c3us:-0} S1=${s1} S2=${s2} S3=${s3} S4=${s4} SELF_EQUAL=${se:-false} ANSWER=${ENG_HASH} BELT=${belt:-none}"
