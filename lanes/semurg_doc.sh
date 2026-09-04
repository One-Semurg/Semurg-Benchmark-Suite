#!/usr/bin/env bash
# Semurg DOCUMENT lane (domain 8): stand up a deterministic document collection, build a THROWAWAY
# Semurg store via the INSTALLED release's `eval` (native engine engaged, one 64B content-addressed
# container per document, never touches the live node's store), then run the three canonical JSON
# document operations 3 cycles cold/warm/warm:
#   Q1  point-lookup by _id (projected to amount_cents)          -> Shard.fetch, inline @32
#   Q2  group-by aggregation (count per category)                -> Shard.scan_histogram @16
#   Q3  nested-field filter count (shipping.region_id == R)      -> Shard.scan_histogram @24
# The answer is gated EQUAL-ANSWER against an INDEPENDENT awk reference over the same CSV (NOT the
# engine): a number is emitted ONLY when Semurg's Q1|Q2|Q3 hash matches the reference on ALL 3 cycles
# AND the O(1) fold self-checks bit-exact against the SIMD scan. No match => no number (honest DNF).
#
# Emits ONE machine-readable line the board parses (same schema as the SQL/TS lanes):
#   LANE=semurg_doc STATUS=ok LOAD_MS=.. Q1_MS=.. Q2_MS=.. Q3_MS=.. ANSWER_HASH=<32hex>
# Robust: if the installed release is not present it emits a clean STATUS=skip (never crashes).
set -uo pipefail
here="$(cd "$(dirname "$0")" && pwd)"; . "$here/_common.sh"

# ---- installed release discovery (the surface a third party has after Semurg-Install) --------------
REL="${SEMURG_REL_BIN:-/opt/semurg/bin/r11}"
[ -x "$REL" ] || REL="$(command -v r11 2>/dev/null || true)"
[ -n "$REL" ] && [ -x "$REL" ] || { echo "LANE=semurg_doc STATUS=skip REASON=release-not-installed(run:semurg-arena install)"; exit 0; }

# ---- knobs (deterministic dataset; all integer-exact so the answer hash is bit-identical) ----------
ROWS="${DOC_ROWS:-2000000}"
CATS="${DOC_CATEGORIES:-32}"
REGS="${DOC_REGIONS:-16}"
STATS="${DOC_STATUSES:-8}"
TSTART="${DOC_TS_START:-1700000000}"
TSSPAN="${DOC_TS_SPAN:-31536000}"
AMTMOD="${DOC_AMOUNT_MOD:-1000000}"
CUSTMOD="${DOC_CUSTOMERS:-50000}"
NTAGS="${DOC_TAG_UNIVERSE:-48}"
PORT="${DOC_SEMURG_PORT:-4993}"

# ---- scratch (throwaway; harness passes ARENA_DATA) ------------------------------------------------
BASE="${ARENA_DATA:-${TMPDIR:-/tmp}/arena_doc_$$}"
WORK="$BASE/semurg_doc_work"
CSV="$WORK/docs.csv"
STORE_DIR="$WORK/store"; STORE_FILE="$STORE_DIR/doc.bin"
BUILD_EXS="$WORK/build.exs"; QUERY_EXS="$WORK/query.exs"
mkdir -p "$STORE_DIR/s0" "$(dirname "$(dirname "$REL")")/tmp" 2>/dev/null || true
trap 'rm -rf "$WORK" 2>/dev/null || true' EXIT INT TERM

# minimal env the release eval needs (runtime.exs). Prefer the installed env's SECRET_KEY_BASE if present.
SKB=""; [ -f /etc/semurg/semurg.env ] && SKB="$(sed -n 's/^SECRET_KEY_BASE=//p' /etc/semurg/semurg.env | head -1)"
run_env=(
  RELEASE_TMP="$(dirname "$(dirname "$REL")")/tmp"
  SEMURG_DATA_DIR="$STORE_DIR" SEMURG_STRIPE_ROOTS="$STORE_DIR/s0"
  SECRET_KEY_BASE="${SKB:-arena_scratch_secret_key_base_0000000000000000}"
  PORT="$PORT" SEMURG_BIND=127.0.0.1
)

# ---- (1) deterministic document collection (same LCG shape as the suite generators) ----------------
# Cols: id,customer_id,category_id,region_id,status_id,amount_cents,ts,tags_bitmap  (all integers).
awk -v n="$ROWS" -v C="$CATS" -v R="$REGS" -v S="$STATS" -v tstart="$TSTART" -v tsspan="$TSSPAN" \
    -v amtmod="$AMTMOD" -v custmod="$CUSTMOD" -v ntags="$NTAGS" 'BEGIN{
  s=1234567;
  print "id,customer_id,category_id,region_id,status_id,amount_cents,ts,tags_bitmap";
  for(i=1;i<=n;i++){
    s=(s*1103515245+12345)%2147483648; cust = 1 + s%custmod;
    s=(s*1103515245+12345)%2147483648; cat  = int(s/256)%C;
    s=(s*1103515245+12345)%2147483648; reg  = int(s/256)%R;
    s=(s*1103515245+12345)%2147483648; st   = int(s/256)%S;
    s=(s*1103515245+12345)%2147483648; amt  = 1 + s%amtmod;
    s=(s*1103515245+12345)%2147483648; ts   = tstart + s%tsspan;
    s=(s*1103515245+12345)%2147483648; t1   = int(s/256)%ntags;
    s=(s*1103515245+12345)%2147483648; t2   = int(s/256)%ntags;
    s=(s*1103515245+12345)%2147483648; prio = int(s/256)%5;
    bm = lshift_or(t1, t2);
    printf "%d,%d,%d,%d,%d,%d,%d,%d\n", i,cust,cat,reg,st,amt,ts,bm;
  }
}
function pow2(b,  r){ r=1; while(b-->0) r*=2; return r; }
function lshift_or(a,b,  x,y){ x=pow2(a); y=pow2(b); if(a==b) return x; return x+y; }
' > "$CSV" 2>/dev/null
[ -s "$CSV" ] || { echo "LANE=semurg_doc STATUS=dnf REASON=dataset-generation-failed"; exit 0; }

# ---- (2) INDEPENDENT awk reference (ground truth; NOT the engine) -----------------------------------
Q1ID="${DOC_Q1_ID:-$(( 1 + ROWS/3 ))}"; [ "$Q1ID" -gt "$ROWS" ] && Q1ID="$ROWS"
Q3REG="${DOC_Q3_REGION:-$(awk -F, 'NR>1{c[$4]++} END{best=-1;b=0; for(k in c){ if(c[k]>best||(c[k]==best&&k<b)){best=c[k];b=k} } print b}' "$CSV")}"
REF_Q1="$(awk -F, -v T="$Q1ID" 'NR>1 && $1==T{print $6; exit}' "$CSV")"
REF_Q2="$(awk -F, -v C="$CATS" 'NR>1{c[$3]++} END{for(i=0;i<C;i++) printf "%s%d",(i?",":""),c[i]+0}' "$CSV")"
REF_Q3="$(awk -F, -v R="$Q3REG" 'NR>1 && $4==R{n++} END{print n+0}' "$CSV")"
REF="$(hash_answer "$REF_Q1|$REF_Q2|$REF_Q3")"
echo "[semurg_doc] rows=$ROWS cats=$CATS regs=$REGS  independent awk reference:" >&2
echo "[semurg_doc]   Q1 _id=$Q1ID -> amount_cents=$REF_Q1 | Q2 per-category=[$REF_Q2] | Q3 region=$Q3REG -> $REF_Q3" >&2
echo "[semurg_doc]   reference ANSWER_HASH=$REF" >&2

# ---- (3) BUILD the throwaway Semurg store (one 64B container per document) --------------------------
cat > "$BUILD_EXS" <<'ELIXIR_BUILD'
{:ok, _} = Application.ensure_all_started(:substrate_core)
Process.sleep(600)
alias SubstrateCore.{Shard, Container}
csv   = System.fetch_env!("DOC_CSV")
store = System.fetch_env!("DOC_STORE_FILE")
cats  = String.to_integer(System.get_env("DOC_CATEGORIES") || "32")
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
        [id, _cust, cat, reg, st, amt, _ts, tags] = ln |> String.trim() |> String.split(",")
        inline =
          <<String.to_integer(amt)::little-64, String.to_integer(st)::little-32,
            String.to_integer(tags)::little-64>>
        Container.node(String.to_integer(id), String.to_integer(cat), String.to_integer(reg), inline, 0, 1)
      end
    {:ok, _} = Shard.append(h, IO.iodata_to_binary(io))
    acc + length(lines)
  end)
Shard.checkpoint(h)
:ok = Shard.set_fold_hist(h, 16, cats)
load_ms = System.monotonic_time(:millisecond) - t0
{:ok, %{dataset_bytes: db}} = Shard.mem_stats(h)
IO.puts("SEMURG_DOC_BUILD rows=#{rows} load_ms=#{load_ms} dataset_mb=#{div(db, 1_048_576)} cats=#{cats}")
ELIXIR_BUILD

# NOTE: `</dev/null` on the release `eval` is load-bearing -- the release drains stdin, and when the
# board runs this lane inside its `while read ... done < <(plan_lines)` loop the inherited FD is that
# plan stream; without the redirect the eval would consume the next plan line (the incumbent row) and
# the loop would end early. Reading EOF from /dev/null keeps the board's per-domain iteration intact.
BOUT="$( env "${run_env[@]}" DOC_CSV="$CSV" DOC_STORE_FILE="$STORE_FILE" DOC_CATEGORIES="$CATS" \
        "$REL" eval "$(cat "$BUILD_EXS")" </dev/null 2>&1 )"
BLINE="$(printf '%s\n' "$BOUT" | grep -m1 '^SEMURG_DOC_BUILD ')"
if [ -z "$BLINE" ]; then
  echo "LANE=semurg_doc STATUS=dnf REASON=build-engine-error([$(printf '%s' "$BOUT" | tr '\n' ' ' | tail -c 120)])"
  exit 0
fi
LOAD_MS="$(sed -n 's/.*load_ms=\([0-9]*\).*/\1/p' <<<"$BLINE")"
DSMB="$(sed -n 's/.*dataset_mb=\([0-9]*\).*/\1/p' <<<"$BLINE")"
echo "[semurg_doc] build: $BLINE" >&2

# ---- (4) 3-cycle QUERY phase (fresh process -> cycle 1 is genuinely cold) ---------------------------
cat > "$QUERY_EXS" <<'ELIXIR_QUERY'
{:ok, _} = Application.ensure_all_started(:substrate_core)
Process.sleep(600)
alias SubstrateCore.Shard
geti = fn k, d -> case System.get_env(k) do nil -> d; "" -> d; s -> String.to_integer(s) end end
store = System.fetch_env!("DOC_STORE_FILE")
cats  = geti.("DOC_CATEGORIES", 32)
regs  = geti.("DOC_REGIONS", 16)
q1id  = geti.("DOC_Q1_ID", 1)
q3reg = geti.("DOC_Q3_REGION", 0)
{:ok, h} = Shard.open(store)
:ok = Shard.set_fold_hist(h, 16, cats)
Shard.reset_stats(h)
hash = fn s -> :crypto.hash(:sha256, s) |> Base.encode16(case: :lower) |> binary_part(0, 32) end
q1 = fn ->
  case Shard.fetch(h, q1id) do
    c when is_binary(c) and byte_size(c) == 64 ->
      <<_::binary-size(32), amt::little-64, _::binary>> = c
      amt
    _ -> -1
  end
end
Enum.each(1..3, fn cyc ->
  {q1_us, amt} = :timer.tc(q1)
  {q2_us, q2counts} = :timer.tc(fn -> Shard.scan_histogram(h, 16, cats) end)
  {q3_us, q3counts} = :timer.tc(fn -> Shard.scan_histogram(h, 24, regs) end)
  q3 = Enum.at(q3counts, q3reg, 0)
  q2str = Enum.map_join(q2counts, ",", &Integer.to_string/1)
  ans = hash.("#{amt}|#{q2str}|#{q3}")
  {cold, warm, hot, acc, dr} = Shard.tier_counts(h)
  IO.puts(
    "SEMURG_DOC_CYCLE cycle=#{cyc} q1_us=#{q1_us} q2_us=#{q2_us} q3_us=#{q3_us} " <>
      "answer=#{ans} cold=#{cold} warm=#{warm} hot=#{hot} accesses=#{acc} disk_reads=#{dr}"
  )
end)
{served, foldcounts} = Shard.group_by_hist(h, 16, cats)
scancounts = Shard.scan_histogram(h, 16, cats)
IO.puts("SEMURG_DOC_SUMMARY served_by_fold=#{served} self_equal=#{foldcounts == scancounts}")
ELIXIR_QUERY

QOUT="$( env "${run_env[@]}" DOC_STORE_FILE="$STORE_FILE" DOC_CATEGORIES="$CATS" DOC_REGIONS="$REGS" \
        DOC_Q1_ID="$Q1ID" DOC_Q3_REGION="$Q3REG" \
        "$REL" eval "$(cat "$QUERY_EXS")" </dev/null 2>&1 )"

# ---- (5) parse cycles + EQUAL-ANSWER gate against the independent reference -------------------------
q1min=""; q2min=""; q3min=""; ncyc=0; all_match=1
while IFS= read -r ln; do
  [ -n "$ln" ] || continue
  ncyc=$((ncyc+1))
  ans="$(sed -n 's/.*answer=\([0-9a-f]*\).*/\1/p' <<<"$ln")"
  u1="$(sed -n 's/.*q1_us=\([0-9]*\).*/\1/p' <<<"$ln")"
  u2="$(sed -n 's/.*q2_us=\([0-9]*\).*/\1/p' <<<"$ln")"
  u3="$(sed -n 's/.*q3_us=\([0-9]*\).*/\1/p' <<<"$ln")"
  dr="$(sed -n 's/.*disk_reads=\([0-9]*\).*/\1/p' <<<"$ln")"
  [ "$ans" = "$REF" ] || all_match=0
  [ -n "$u1" ] && { [ -z "$q1min" ] || [ "$u1" -lt "$q1min" ]; } && q1min="$u1"
  [ -n "$u2" ] && { [ -z "$q2min" ] || [ "$u2" -lt "$q2min" ]; } && q2min="$u2"
  [ -n "$u3" ] && { [ -z "$q3min" ] || [ "$u3" -lt "$q3min" ]; } && q3min="$u3"
  echo "[semurg_doc] $ln" >&2
done < <(printf '%s\n' "$QOUT" | grep '^SEMURG_DOC_CYCLE ')

SUM="$(printf '%s\n' "$QOUT" | grep -m1 '^SEMURG_DOC_SUMMARY ')"
SELF_EQUAL="$(sed -n 's/.*self_equal=\([a-z]*\).*/\1/p' <<<"$SUM")"
echo "[semurg_doc] ${SUM:-SEMURG_DOC_SUMMARY missing}" >&2

if [ "$ncyc" -lt 3 ]; then
  echo "LANE=semurg_doc STATUS=dnf REASON=query-engine-error([$(printf '%s' "$QOUT" | tr '\n' ' ' | tail -c 120)])"
  exit 0
fi
if [ "$all_match" != 1 ]; then
  echo "LANE=semurg_doc STATUS=dnf REASON=equal-answer-mismatch(semurg!=independent-awk-reference)"
  exit 0
fi
if [ "$SELF_EQUAL" != "true" ]; then
  echo "LANE=semurg_doc STATUS=dnf REASON=fold-scan-self-check-failed(self_equal=$SELF_EQUAL)"
  exit 0
fi

# ---- (6) verified -> emit the board line (query us -> ms, ceil so a real sub-ms op never reads 0) ---
ceilms(){ local u="${1:-0}"; [ -n "$u" ] || u=0; echo $(( (u + 999) / 1000 )); }
Q1_MS="$(ceilms "$q1min")"; Q2_MS="$(ceilms "$q2min")"; Q3_MS="$(ceilms "$q3min")"
echo "[semurg_doc] EQUAL-ANSWER OK on all 3 cycles (dataset_mb=${DSMB:-?}); emitting board line." >&2
echo "LANE=semurg_doc STATUS=ok LOAD_MS=${LOAD_MS:-0} Q1_MS=$Q1_MS Q2_MS=$Q2_MS Q3_MS=$Q3_MS ANSWER_HASH=$REF"
exit 0
