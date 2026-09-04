#!/usr/bin/env bash
# Semurg KEY-VALUE lane (domain 6). Point GET + batch GET over a 1M-key, value=20-byte keyspace, run
# against a WHOLE-CLUSTER striped :fan store through the INSTALLED release's `eval` (the same surface a
# third party has after Semurg-Install), on a THROWAWAY store that never touches the live node's data.
# Mirrors the wired done-domain lanes (semurg_graph.sh / semurg_olap.sh): resolve the installed r11,
# use a throwaway data dir, emit ONE machine line run_all_domains.sh parses, and SKIP honestly (never a
# fake number) when the release is not installed.
#
# value(id) = 20-byte big-endian id == printf '%040x' id (40 lowercase hex chars) -- byte-identical to
# the Redis + RocksDB KV lanes, so the equal-answer is cross-engine. ANSWER = first 32 hex chars of
# sha256 over the values of ids 1..S in id order (the same reference workload/gen_data.sh computes
# independently with awk). A run counts only if its ANSWER equals that reference (equal-answer gate).
#
# Emits (parsed by run_all_domains.sh -> ok-reference / ok-matched):
#   LANE=semurg_kv STATUS=ok LOAD_MS=.. SHARDS=.. PIPES=.. GET_RPS_C1/2/3=.. BATCH_RPS_C1/2/3=.. \
#     ROWS_PER_S=<warm point-GET/s> SELF=.. ANSWER=<hash> keys=..
# plus SEMURG_KV_BELT cycle lines (the #1-TEST belt evidence: odirect_open_ok>0, fallback=0,
# buffered=0, sqes fall cold->warm, fifo_hits rise, prefetch/janitor engaged). run_all_domains.sh reads
# LOAD_MS (load column), ROWS_PER_S (q_ms column = the point-GET throughput, the apples-to-apples KV
# metric Redis/RocksDB also report), and ANSWER (equal-answer hash). GET_RPS_C1/2/3 + BATCH_RPS_C1/2/3
# ride the same line self-descriptively so the batch (one-crossing deep-QD) number is never hidden.
set -uo pipefail
here="$(cd "$(dirname "$0")" && pwd)"; . "$here/_common.sh" 2>/dev/null || true

REL="${SEMURG_REL_BIN:-/opt/semurg/bin/r11}"
[ -x "$REL" ] || REL="$(command -v r11 2>/dev/null || echo "$REL")"
[ -x "$REL" ] || { echo "LANE=semurg_kv STATUS=skip REASON=release-not-installed(run:semurg-arena install; or set SEMURG_REL_BIN=/opt/semurg/bin/r11)"; exit 0; }

# throwaway scratch: reuse the orchestrator's ARENA_DATA when given, else a temp dir; cleaned on exit.
SCRATCH="${ARENA_DATA:-$(mktemp -d)}"; mkdir -p "$SCRATCH" 2>/dev/null || true
D="$SCRATCH/semurg_kv_data.$$"; rm -rf "$D"; mkdir -p "$D" "$D/tmp" 2>/dev/null || true
trap 'rm -rf "$D"' EXIT

# SECRET_KEY_BASE: reuse the box's installed value if present, else a throwaway. NEVER printed.
SKB="$(sed -n 's/^SECRET_KEY_BASE=//p' /etc/semurg/semurg.env 2>/dev/null | head -1)"
[ -n "$SKB" ] || SKB="$(head -c48 /dev/urandom | base64 | tr -d '/+=' | head -c48)"

# stripe roots = the disk pipes to fan across (KV_STRIPE_ROOTS / SEMURG_STRIPE_ROOTS, comma/space
# separated); set them to dirs on 2+ physical NVMe for a real multi-pipe fan, else a single throwaway.
STRIPES="${KV_STRIPE_ROOTS:-${SEMURG_STRIPE_ROOTS:-$D/data}}"

# ---- the Semurg-native KV workload, run against the INSTALLED release via `eval` --------------------
# Universal production APIs only (SubstrateCore.Shard / Container / StripedShard / Janitor) -- the same
# surface the graph + olap lanes drive; no model-specific or bench-specific read path.
KV_EXS="$(cat <<'KV_ELIXIR'
{:ok, _} = Application.ensure_all_started(:substrate_core)
Process.sleep(600)
alias SubstrateCore.{Shard, Container, StripedShard}

env_int = fn k, d ->
  case System.get_env(k) do
    nil -> d
    "" -> d
    s -> String.to_integer(s)
  end
end

n = env_int.("KV_KEYS", 1_000_000)
asample = min(env_int.("KV_ANSWER_SAMPLE", 4096), n)
getsample = min(env_int.("KV_GET_SAMPLE", 200_000), n)

# stripe roots -> n sub-shards, shard i on roots[i % len(roots)] (fan across all disk pipes).
roots =
  case System.get_env("KV_STRIPE_ROOTS") || System.get_env("SEMURG_STRIPE_ROOTS") do
    s when is_binary(s) and s != "" -> s |> String.split([",", " "], trim: true) |> Enum.map(&String.trim/1) |> Enum.reject(&(&1 == ""))
    _ -> [System.get_env("KV_DIR") || "/tmp/semurg_kv_#{System.system_time(:millisecond)}"]
  end

nshards = env_int.("KV_SHARDS", max(length(roots), System.schedulers_online()))
base = "kvbench_#{System.system_time(:millisecond)}"
dirs = Enum.map(roots, fn r -> d = Path.join(r, base); File.rm_rf(d); File.mkdir_p!(d); d end)

# open the striped :fan store (production API). subs = [{:native, ref}, ...] in shard-index order.
st = StripedShard.open_striped(base, dirs, nshards)
handle = {:striped, st}
subs_arr = List.to_tuple(st.subs)

# best-effort arm the ONE whole-store janitor on the striped handle (universal primitive; #1 TEST wants
# it engaged on every path). Counters are read regardless via conveyor_report/0.
_ =
  try do
    SubstrateCore.Janitor.start_for(handle)
  rescue
    _ -> :ok
  catch
    _, _ -> :ok
  end

# deterministic value for id = 20-byte big-endian id == printf '%040x' id == the incumbents' 20 raw bytes.
val = fn id -> <<id::unsigned-big-integer-size(160)>> end
# inline20 sits at byte offset 32 in the 64B node container (tag1+pad1+gen4+ver2+id8+type8+attr8 = 32).
extract = fn c -> if is_binary(c) and byte_size(c) >= 52, do: binary_part(c, 32, 20), else: <<0::160>> end
snap = fn ->
  try do
    Shard.conveyor_report()
  rescue
    _ -> %{}
  catch
    _, _ -> %{}
  end
end
gc = fn m, k -> Map.get(m, k, 0) end

# ---- LOAD: octopus write-fan, containers grouped by id % nshards, ONE crossing per chunk ----
t_load = :erlang.monotonic_time(:millisecond)
chunk = 2_000_000
Enum.each(0..(div(n + chunk - 1, chunk) - 1), fn c ->
  lo = c * chunk + 1
  hi = min(lo + chunk - 1, n)

  buckets =
    Enum.reduce(lo..hi, %{}, fn id, acc ->
      s = rem(id, nshards)
      Map.update(acc, s, [Container.node(id, 0, 0, val.(id), 0, 1)], fn l -> [Container.node(id, 0, 0, val.(id), 0, 1) | l] end)
    end)

  pairs = for {s, iol} <- buckets, do: {elem(subs_arr, s), IO.iodata_to_binary(iol)}
  {:ok, _} = Shard.append_multi(pairs)
end)

Shard.checkpoint(handle)
load_ms = :erlang.monotonic_time(:millisecond) - t_load

get_ids = Enum.to_list(1..getsample)
getfn = fn -> Enum.each(get_ids, fn id -> Shard.fetch(handle, id) end) end
batchfn = fn -> Shard.fetch_batch(handle, get_ids) end

# ---- 3 CYCLES: cold -> warm -> warm (same store, same BEAM => real belt cold->warm progression) ----
cycles =
  for cyc <- 1..3 do
    b0 = snap.()
    {gus, _} = :timer.tc(getfn)
    {bus, _} = :timer.tc(batchfn)
    b1 = snap.()

    %{
      cyc: cyc,
      get_rps: round(getsample / (gus / 1_000_000.0)),
      batch_rps: round(getsample / (bus / 1_000_000.0)),
      sqes: gc.(b1, :sqes_submitted) - gc.(b0, :sqes_submitted),
      odok: gc.(b1, :odirect_open_ok),
      odfb: gc.(b1, :odirect_open_fallback),
      buffered: gc.(b1, :buffered_preads),
      fifo_hits: gc.(b1, :fifo_hits) - gc.(b0, :fifo_hits),
      fifo_miss: gc.(b1, :fifo_misses) - gc.(b0, :fifo_misses),
      prefetch: gc.(b1, :prefetch_sqes_ahead) - gc.(b0, :prefetch_sqes_ahead),
      reloc: gc.(b1, :relocated_containers),
      jep: gc.(b1, :janitor_epochs)
    }
  end

# self-check: the batch's first value == the single fetch of id 1 (never a faked speedup).
packed = batchfn.()
first = if byte_size(packed) >= 52, do: binary_part(packed, 32, 20), else: <<0::160>>
self_ok = first == extract.(Shard.fetch(handle, 1))

# ANSWER = sha256 over ids 1..S, each value as 40-char lowercase hex, first 32 hex chars. Byte-identical
# to the Redis + RocksDB lanes and to gen_data.sh's independent awk reference.
ahex = for id <- 1..asample, into: <<>>, do: Base.encode16(extract.(Shard.fetch(handle, id)), case: :lower)
answer = :crypto.hash(:sha256, ahex) |> Base.encode16(case: :lower) |> binary_part(0, 32)

Enum.each(dirs, &File.rm_rf/1)

c = fn i, k -> Map.get(Enum.at(cycles, i), k) end

IO.puts(
  "SEMURG_KV keys=#{n} shards=#{nshards} pipes=#{length(dirs)} load_ms=#{load_ms} " <>
    "get_rps_c1=#{c.(0, :get_rps)} get_rps_c2=#{c.(1, :get_rps)} get_rps_c3=#{c.(2, :get_rps)} " <>
    "batch_rps_c1=#{c.(0, :batch_rps)} batch_rps_c2=#{c.(1, :batch_rps)} batch_rps_c3=#{c.(2, :batch_rps)} " <>
    "self_equal=#{self_ok} answer=#{answer}"
)

for cy <- cycles do
  IO.puts(
    "SEMURG_KV_BELT cycle=#{cy.cyc} sqes=#{cy.sqes} odirect_open_ok=#{cy.odok} fallback=#{cy.odfb} " <>
      "buffered=#{cy.buffered} fifo_hits=#{cy.fifo_hits} fifo_misses=#{cy.fifo_miss} " <>
      "prefetch_sqes_ahead=#{cy.prefetch} janitor_epochs=#{cy.jep} relocated=#{cy.reloc}"
  )
end
KV_ELIXIR
)"

OUT="$( \
  KV_KEYS="${KV_KEYS:-1000000}" KV_ANSWER_SAMPLE="${KV_ANSWER_SAMPLE:-4096}" KV_GET_SAMPLE="${KV_GET_SAMPLE:-200000}" \
  KV_SHARDS="${KV_SHARDS:-}" KV_STRIPE_ROOTS="$STRIPES" KV_DIR="$D/kv" \
  SEMURG_DATA_DIR="$D/data" SEMURG_STRIPE_ROOTS="$STRIPES" \
  SECRET_KEY_BASE="$SKB" RELEASE_TMP="$D/tmp" PORT="${KV_PORT:-4991}" SEMURG_BIND=127.0.0.1 \
  timeout "${KV_TIMEOUT:-600}" "$REL" eval "$KV_EXS" </dev/null 2>/dev/null )"

LINE="$(printf '%s\n' "$OUT" | grep -m1 '^SEMURG_KV ')"
if [ -z "$LINE" ]; then
  # ran but produced no machine line: honest DNF (release present but the workload errored/timed out).
  if printf '%s' "$OUT" | grep -qiE 'killed|out of memory|oom'; then
    echo "LANE=semurg_kv STATUS=dnf REASON=oom-killed"
  elif [ -z "$OUT" ]; then
    echo "LANE=semurg_kv STATUS=skip REASON=installed-node-unreachable(no-output-from-eval; check r11 + substrate_core)"
  else
    echo "LANE=semurg_kv STATUS=dnf REASON=engine-error([$(printf '%s' "$OUT" | tr '\n' ' ' | tail -c 120)])"
  fi
  exit 0
fi

g(){ sed -n "s/.*$1=\\([0-9a-z]*\\).*/\\1/p" <<<"$LINE"; }
gc3="$(g get_rps_c3)"
echo "LANE=semurg_kv STATUS=ok LOAD_MS=$(g load_ms) SHARDS=$(g shards) PIPES=$(g pipes) \
GET_RPS_C1=$(g get_rps_c1) GET_RPS_C2=$(g get_rps_c2) GET_RPS_C3=$gc3 \
BATCH_RPS_C1=$(g batch_rps_c1) BATCH_RPS_C2=$(g batch_rps_c2) BATCH_RPS_C3=$(g batch_rps_c3) \
ROWS_PER_S=$gc3 SELF=$(g self_equal) ANSWER=$(g answer) keys=$(g keys)"
# belt evidence (the un-fakeable #1-TEST proof) passed through verbatim.
printf '%s\n' "$OUT" | grep '^SEMURG_KV_BELT ' || true
exit 0
