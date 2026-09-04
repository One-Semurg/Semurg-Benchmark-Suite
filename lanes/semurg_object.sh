#!/usr/bin/env bash
# Semurg OBJECT lane: PUT then GET/STAT/LIST a deterministic object set through the INSTALLED Semurg
# release (`$REL eval`, throwaway store -- never touches the live node's data), equal-answer gated against
# an INDEPENDENT reference computed from the source object bytes. Objects are value node-chains on the
# substrate (a Container.node head carrying up to 20 inline bytes + a first_chunk pointer chaining 20-byte
# continuation nodes -- the join-ledger blob format), striped across SEMURG_OBJ_ROOTS (one native shard
# per disk, id%N routing) and fanned across every scheduler. Two eval passes:
#   load  : PUT every object (build chain -> Shard.append -> Shard.checkpoint), PERSIST the store.
#   query : RE-OPEN the persisted store and run GET / STAT / LIST three times (cold/warm/warm) so the
#           conveyor warms within the process (the un-fakeable belt proof), then emit the answer hashes.
#
# EQUAL-ANSWER (byte-identical content + keyset), reproduced identically by any future incumbent lane:
#   GET   = sha256( join_"\n"( "key<TAB>sha256hex(bytes_returned)" for key in sorted(keys) ) )[:32]   (content integrity)
#   STAT  = integer sum of the size each engine reports for every object                                (metadata)
#   LIST  = sha256( join_"\n"( sorted(keys the engine enumerates) ) )[:32]                              (keyset)
#   ANSWER= sha256( GET "|" STAT "|" LIST )[:32]   -- the single gate value; the lane emits ONLY a number
#           it actually measured AND that reproduced this independent reference (never hardcoded/faked).
#
# Emits ONE machine line run_all_domains.sh parses (domain 7, OBJECT):
#   LANE=semurg_object STATUS=ok LOAD_MS=.. QUERY_MS=.. ANSWER_HASH=<hash> GET_MS=.. STAT_MS=.. LIST_MS=.. OBJECTS=..
# Robust: if the installed release is unreachable / a prerequisite is missing -> a clean SKIP (never crash).
set -uo pipefail
here="$(cd "$(dirname "$0")" && pwd)"; . "$here/_common.sh"
export LC_ALL=C
LANE=semurg_object
skip(){ echo "LANE=$LANE STATUS=skip REASON=$1"; exit 0; }
dnf(){  echo "LANE=$LANE STATUS=dnf REASON=$1";  exit 0; }

# ---- the installed release a third party has after Semurg-Install (NOT the dev trunk) ------------------
REL="${SEMURG_REL_BIN:-/opt/semurg/bin/r11}"
[ -x "$REL" ] || REL="$(command -v r11 2>/dev/null || true)"
[ -n "$REL" ] && [ -x "$REL" ] || skip "release-not-installed(run:semurg-arena install)"
command -v openssl   >/dev/null 2>&1 || skip "openssl-missing(needed-for-deterministic-object-content)"
command -v sha256sum >/dev/null 2>&1 || skip "sha256sum-missing"

# ---- scratch (the orchestrator hands us ARENA_DATA; stand alone otherwise). Throwaway everything. -----
SC="${ARENA_DATA:-${TMPDIR:-/tmp}/arena_object}"; mkdir -p "$SC" 2>/dev/null || skip "scratch-unwritable($SC)"
DATA="$SC/data"; OBJD="$DATA/objects"; STORE="$SC/store"; RT="$SC/runtime"
rm -rf "$DATA" "$STORE" "$RT" 2>/dev/null || true
mkdir -p "$OBJD" "$STORE/s0" "$RT/s0" 2>/dev/null || skip "scratch-unwritable($SC)"

N="${OBJ_N:-4000}"          # number of objects
SIZE="${OBJ_SIZE:-4096}"    # bytes per object (4 KiB = the classic small-object S3 workload)
SEED="${OBJ_SEED:-1234567}" # determinism seed (same knobs -> byte-identical dataset)
CYCLES="${OBJ_CYCLES:-3}"   # GET/STAT/LIST repeats (cold/warm/warm)

# ---- 1) deterministic object set + INDEPENDENT equal-answer reference (from the ACTUAL bytes) ---------
# content = AES-256-CTR keystream over zeros, per-object pass (seed+index), sha256 digest pinned:
# high-entropy / incompressible (defeats dedup/compression short-cuts on either engine), fully
# deterministic. Keys are zero-padded so lexical == numeric == byte order under LC_ALL=C.
MAN="$DATA/manifest.tsv"; : > "$MAN"
tot=0
i=0
while [ "$i" -lt "$N" ]; do
  key=$(printf 'obj-%08d.bin' "$i")
  f="$OBJD/$key"
  head -c "$SIZE" < <(openssl enc -aes-256-ctr -md sha256 -nosalt -pass "pass:${SEED}-${i}" -in /dev/zero 2>/dev/null) > "$f" || dnf "object-gen-failed(openssl)"
  sz=$(stat -c%s "$f" 2>/dev/null || echo 0)
  [ "$sz" = "$SIZE" ] || dnf "object-gen-short($key:$sz!=$SIZE)"
  sha=$(sha256sum "$f" | cut -d' ' -f1)
  printf '%s\t%d\t%s\n' "$key" "$sz" "$sha" >> "$MAN"
  tot=$((tot + sz))
  i=$((i + 1))
done
sort -k1,1 "$MAN" > "$DATA/manifest.sorted.tsv"
# GET/LIST references: lines joined by \n with NO trailing newline (matches the .exs Enum.map_join).
awk -F'\t' '{print $1"\t"$3}' "$DATA/manifest.sorted.tsv" > "$DATA/get.lines"
awk -F'\t' '{print $1}'       "$DATA/manifest.sorted.tsv" > "$DATA/list.lines"
REF_GET=$(printf '%s' "$(< "$DATA/get.lines")"  | sha256sum | cut -c1-32)
REF_LIST=$(printf '%s' "$(< "$DATA/list.lines")" | sha256sum | cut -c1-32)
REF_STAT="$tot"
REF_ANSWER=$(printf '%s' "$REF_GET|$REF_STAT|$REF_LIST" | sha256sum | cut -c1-32)

# ---- 2) the Semurg driver (public SubstrateCore API only; no engine-source reference) -----------------
EXS="$SC/semurg_object.exs"
cat > "$EXS" <<'ELIXIR'
# Semurg OBJECT driver. Objects = value node-chains (Container.node head + 20-byte inline chunks linked by
# first_chunk). load: PUT+checkpoint (persist). query: re-open + GET(read_value_chain)/STAT(fetch+attr)/
# LIST(head-node existence) x OBJ_CYCLES, snapshot the process-global conveyor counters (belt proof), and
# emit answers built byte-identically to the shell reference.
{:ok, _} = Application.ensure_all_started(:substrate_core)
Process.sleep(600)

defmodule ObjBench do
  alias SubstrateCore.{Shard, Container}
  @tag 2  # value-node type tag (consistent write<->read; the chain is walked by length + first_chunk)

  def env(k, d), do: System.get_env(k) || d
  def conc, do: System.schedulers_online()

  def roots do
    case String.split(env("SEMURG_OBJ_ROOTS", ""), ~r/\s+/, trim: true) do
      [] -> [env("SEMURG_OBJ_STORE", "/tmp/semurg_obj_store")]
      rs -> rs
    end
  end

  def vid(key) do
    <<x::unsigned-little-64, _::binary>> = :crypto.hash(:sha256, key)
    if x == 0, do: 1, else: x
  end

  def chunk_id(v, i) do
    <<x::unsigned-little-64, _::binary>> = :crypto.hash(:sha256, <<v::little-64, i::little-64>>)
    if x == 0, do: 1, else: x
  end

  def chunk20(<<>>), do: []
  def chunk20(b) when byte_size(b) <= 20, do: [b]
  def chunk20(<<h::binary-20, rest::binary>>), do: [h | chunk20(rest)]

  def encode_value(v, value) do
    l = byte_size(value)
    if l <= 20 do
      {[Container.node(v, @tag, l, value, 0, 1)], 1}
    else
      pieces = chunk20(value)
      n = length(pieces)
      ids = [v | Enum.map(1..(n - 1), fn i -> chunk_id(v, i) end)]
      nexts = tl(ids) ++ [0]

      conts =
        [ids, pieces, nexts]
        |> Enum.zip()
        |> Enum.with_index()
        |> Enum.map(fn {{id, piece, nxt}, idx} ->
          attr = if idx == 0, do: l, else: byte_size(piece)
          Container.node(id, @tag, attr, piece, nxt, 1)
        end)

      {conts, n}
    end
  end

  def keys do
    env("MANIFEST", "/data/manifest.sorted.tsv")
    |> File.read!()
    |> String.split("\n", trim: true)
    |> Enum.map(fn line -> line |> String.split("\t") |> hd() end)
    |> Enum.sort()
  end

  def open_shards(rs) do
    Enum.map(rs, fn r ->
      File.mkdir_p!(r)
      {:ok, h} = Shard.open(Path.join(r, "obj_shard.bin"))
      h
    end)
  end

  def owner(v, n), do: rem(v, n)
  def hh(text), do: :crypto.hash(:sha256, text) |> Base.encode16(case: :lower) |> binary_part(0, 32)
  def sha_hex(bin), do: :crypto.hash(:sha256, bin) |> Base.encode16(case: :lower)

  def peak_rss_mb do
    case File.read("/proc/self/status") do
      {:ok, s} ->
        case Regex.run(~r/VmHWM:\s+(\d+)\s+kB/, s) do
          [_, kb] -> div(String.to_integer(kb), 1024)
          _ -> -1
        end
      _ -> -1
    end
  end

  def run_load do
    rs = roots()
    Enum.each(rs, fn r -> File.rm(Path.join(r, "obj_shard.bin")) end)
    shards = open_shards(rs)
    n = length(shards)
    objdir = env("OBJ_DIR", "/data/objects")
    ks = keys()
    t0 = System.monotonic_time(:millisecond)

    built =
      ks
      |> Task.async_stream(
        fn k ->
          v = vid(k)
          bytes = File.read!(Path.join(objdir, k))
          {conts, nc} = encode_value(v, bytes)
          {owner(v, n), IO.iodata_to_binary(conts), nc}
        end,
        max_concurrency: conc(), timeout: :infinity, ordered: false
      )
      |> Enum.map(fn {:ok, r} -> r end)

    total_containers = Enum.reduce(built, 0, fn {_o, _bin, nc}, acc -> acc + nc end)
    by_owner = Enum.group_by(built, fn {o, _, _} -> o end, fn {_, bin, _} -> bin end)

    by_owner
    |> Enum.map(fn {o, bins} ->
      Task.async(fn ->
        shard = Enum.at(shards, o)
        {:ok, _} = Shard.append(shard, IO.iodata_to_binary(bins))
        Shard.checkpoint(shard)
      end)
    end)
    |> Enum.each(&Task.await(&1, :infinity))

    load_ms = System.monotonic_time(:millisecond) - t0
    IO.puts("SEMURG_OBJ_LOAD load_ms=#{load_ms} objects=#{length(ks)} containers=#{total_containers}")
  end

  def get_all(shards, n, ks) do
    pairs =
      ks
      |> Task.async_stream(
        fn k ->
          v = vid(k)
          blob = Shard.read_value_chain(Enum.at(shards, owner(v, n)), v)
          {k, sha_hex(blob)}
        end,
        max_concurrency: conc(), timeout: :infinity, ordered: false
      )
      |> Enum.map(fn {:ok, r} -> r end)
      |> Map.new()

    hh(Enum.map_join(ks, "\n", fn k -> k <> "\t" <> Map.fetch!(pairs, k) end))
  end

  def stat_all(shards, n, ks) do
    ks
    |> Task.async_stream(
      fn k ->
        v = vid(k)
        Container.node_attr(Shard.fetch(Enum.at(shards, owner(v, n)), v))
      end,
      max_concurrency: conc(), timeout: :infinity, ordered: false
    )
    |> Enum.reduce(0, fn {:ok, sz}, acc -> acc + sz end)
    |> Integer.to_string()
  end

  def list_all(shards, n, ks) do
    present =
      ks
      |> Task.async_stream(
        fn k ->
          v = vid(k)
          case Shard.fetch(Enum.at(shards, owner(v, n)), v) do
            c when is_binary(c) and byte_size(c) == 64 -> if Container.torn?(c), do: nil, else: k
            _ -> nil
          end
        end,
        max_concurrency: conc(), timeout: :infinity, ordered: false
      )
      |> Enum.flat_map(fn {:ok, k} -> if k, do: [k], else: [] end)
      |> Enum.sort()

    hh(Enum.join(present, "\n"))
  end

  def belt_line(before, aft) do
    d = fn key -> Map.get(aft, key, 0) - Map.get(before, key, 0) end
    "SEMURG_OBJ_BELT odirect_ok=#{d.(:odirect_open_ok)} fallback=#{d.(:odirect_open_fallback)} " <>
      "buffered=#{d.(:buffered_preads)} fifo_hits=#{d.(:fifo_hits)} fifo_misses=#{d.(:fifo_misses)} " <>
      "prefetch_ahead=#{d.(:prefetch_sqes_ahead)} relocated=#{d.(:relocated_containers)} " <>
      "pages_before=#{Map.get(aft, :pages_before, 0)} pages_after=#{Map.get(aft, :pages_after, 0)}"
  end

  def run_query do
    rs = roots()
    shards = open_shards(rs)
    n = length(shards)
    ks = keys()
    cycles = String.to_integer(env("OBJ_CYCLES", "3"))
    belt0 = try do Shard.conveyor_report() rescue _ -> %{} end

    last =
      Enum.reduce(1..cycles, nil, fn c, _acc ->
        {gms, gh} = :timer.tc(fn -> get_all(shards, n, ks) end)  |> then(fn {us, r} -> {div(us, 1000), r} end)
        {sms, sv} = :timer.tc(fn -> stat_all(shards, n, ks) end) |> then(fn {us, r} -> {div(us, 1000), r} end)
        {lms, lh} = :timer.tc(fn -> list_all(shards, n, ks) end) |> then(fn {us, r} -> {div(us, 1000), r} end)
        IO.puts("SEMURG_OBJ_CYCLE c=#{c} get_ms=#{gms} stat_ms=#{sms} list_ms=#{lms}")
        {gh, sv, lh}
      end)

    belt1 = try do Shard.conveyor_report() rescue _ -> %{} end
    IO.puts(belt_line(belt0, belt1))
    {gh, sv, lh} = last
    ans = hh("#{gh}|#{sv}|#{lh}")
    IO.puts("SEMURG_OBJ_ANSWER get=#{gh} stat=#{sv} list=#{lh} answer=#{ans} peak_rss_mb=#{peak_rss_mb()}")
  end

  def run do
    case env("SEMURG_OBJ_MODE", "query") do
      "load" -> run_load()
      _ -> run_query()
    end
  end
end

ObjBench.run()
ELIXIR

# ---- 3) drive the installed release: load (persist) then query (re-open) ------------------------------
REL_TMP="$(dirname "$(dirname "$REL")")/tmp"; mkdir -p "$REL_TMP" 2>/dev/null || true
# prefer the installed env's SECRET_KEY_BASE if present (never printed)
SKB=""; [ -f /etc/semurg/semurg.env ] && SKB="$(sed -n 's/^SECRET_KEY_BASE=//p' /etc/semurg/semurg.env | head -1)"
ROOTS="${SEMURG_OBJ_ROOTS:-$STORE/s0}"   # one dir per disk to stripe across all local NVMe (id%N routing)
MAN_SORTED="$DATA/manifest.sorted.tsv"

run_eval(){ # mode
  # stdin from /dev/null: the release runs BEAM with -noshell (it reads stdin). When this lane runs under
  # run_all_domains.sh the loop's stdin is the plan_lines pipe, so a beam that read stdin would swallow the
  # remaining plan rows and drop later lanes. Detaching stdin keeps this lane a good citizen on the board.
  RELEASE_TMP="$REL_TMP" \
  SEMURG_DATA_DIR="$RT" SEMURG_STRIPE_ROOTS="$RT/s0" \
  SECRET_KEY_BASE="${SKB:-arena_scratch_secret_key_base_0000000000000000}" \
  PORT=4994 SEMURG_BIND=127.0.0.1 \
  SEMURG_OBJ_MODE="$1" SEMURG_OBJ_ROOTS="$ROOTS" \
  OBJ_DIR="$OBJD" MANIFEST="$MAN_SORTED" OBJ_CYCLES="$CYCLES" \
  "$REL" eval "$(cat "$EXS")" </dev/null 2>&1
}

LOAD_OUT="$(run_eval load)"
LOAD_LINE="$(printf '%s\n' "$LOAD_OUT" | grep -m1 '^SEMURG_OBJ_LOAD ')"
if [ -z "$LOAD_LINE" ]; then
  # the installed node could not run the PUT at all -> clean SKIP (unreachable), never a crash.
  rm -rf "$DATA" "$STORE" "$RT" 2>/dev/null || true
  skip "engine-unreachable([$(printf '%s' "$LOAD_OUT" | tr '\n' ' ' | tail -c 120)])"
fi
LOAD_MS=$(sed -n 's/.*load_ms=\([0-9]*\).*/\1/p' <<<"$LOAD_LINE")

QUERY_OUT="$(run_eval query)"
ANS_LINE="$(printf '%s\n' "$QUERY_OUT" | grep -m1 '^SEMURG_OBJ_ANSWER ')"
if [ -z "$ANS_LINE" ]; then
  rm -rf "$DATA" "$STORE" "$RT" 2>/dev/null || true
  dnf "query-error([$(printf '%s' "$QUERY_OUT" | tr '\n' ' ' | tail -c 120)])"
fi
PROD_ANSWER=$(sed -n 's/.*answer=\([0-9a-f]*\).*/\1/p' <<<"$ANS_LINE")

# warm GET/STAT/LIST = the best (min) timing across the WARM cycles (2..CYCLES); the honest headline the
# incumbent's warm GET is compared against. Falls back to cycle 1 if only one cycle was run.
warm_min(){ # field-name
  printf '%s\n' "$QUERY_OUT" | grep '^SEMURG_OBJ_CYCLE ' \
    | awk -v f="$1" '{ c=0; g=-1; for(i=1;i<=NF;i++){ split($i,kv,"="); if(kv[1]=="c")c=kv[2]; if(kv[1]==f)g=kv[2] } if(c>=2 && g>=0){ if(m==""||g<m)m=g } } END{ if(m=="")m=-1; print m }'
}
GET_MS=$(warm_min get_ms);  [ "${GET_MS:--1}" -ge 0 ] 2>/dev/null || GET_MS=$(printf '%s\n' "$QUERY_OUT" | sed -n 's/.*SEMURG_OBJ_CYCLE c=1 get_ms=\([0-9]*\).*/\1/p' | head -1)
STAT_MS=$(warm_min stat_ms); [ "${STAT_MS:--1}" -ge 0 ] 2>/dev/null || STAT_MS=0
LIST_MS=$(warm_min list_ms); [ "${LIST_MS:--1}" -ge 0 ] 2>/dev/null || LIST_MS=0
: "${GET_MS:=0}"

# ---- 4) EQUAL-ANSWER GATE: emit ok ONLY if the measured Semurg answer reproduced the independent ref --
rm -rf "$DATA" "$STORE" "$RT" 2>/dev/null || true
if [ -z "$PROD_ANSWER" ]; then
  dnf "no-answer-parsed"
elif [ "$PROD_ANSWER" != "$REF_ANSWER" ]; then
  # Semurg returned different bytes/keyset than the source -> NOT a win, never a fake reference.
  dnf "answer-mismatch(prod=$PROD_ANSWER ref=$REF_ANSWER)"
fi
echo "LANE=semurg_object STATUS=ok LOAD_MS=${LOAD_MS:-0} QUERY_MS=${GET_MS} ANSWER_HASH=${PROD_ANSWER} GET_MS=${GET_MS} STAT_MS=${STAT_MS} LIST_MS=${LIST_MS} OBJECTS=${N}"
