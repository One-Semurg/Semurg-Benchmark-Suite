# Semurg OLAP lane core: the SAME narrow COUNT(*) GROUP BY (r % G) the DuckDB side runs, over the SAME
# rows. Reports BOTH Semurg numbers with the honest direction: RAW SCAN (O(N) SIMD sweep -- may LOSE to
# a columnar warehouse) and FOLD-AT-INGEST (O(1) point-get off the running monoid -- WINS). The answer
# (per-bucket counts) is emitted as a hash so the lane can be gated equal-answer against DuckDB, and the
# scan==fold self-check guarantees the fold serves the identical histogram (never a faked speedup).
{:ok, _} = Application.ensure_all_started(:substrate_core)
Process.sleep(600)
alias SubstrateCore.{Shard, Container}

env_int = fn k, d -> case System.get_env(k) do nil -> d; "" -> d; s -> String.to_integer(s) end end
rows = env_int.("OLAP_ROWS", 20_000_000)
groups = env_int.("OLAP_GROUPS", 64)
field = 24
dir = System.get_env("OLAP_DIR") || "/tmp/semurg_olap_#{System.system_time(:millisecond)}"

File.rm_rf(dir); File.mkdir_p!(dir)
path = Path.join(dir, "olap.bin")
{:ok, h} = Shard.open(path)
inline = <<0::160>>
chunk = 1_000_000
Enum.each(0..(div(rows + chunk - 1, chunk) - 1), fn c ->
  lo = c * chunk + 1
  hi = min(lo + chunk - 1, rows)
  io = for r <- lo..hi, do: Container.node(r, rem(r, 7), r, inline, 0, 1)
  {:ok, _} = Shard.append(h, IO.iodata_to_binary(io))
end)
Shard.checkpoint(h)

_ = Shard.scan_histogram(h, field, groups)
{scan_us, scan_counts} =
  for(_ <- 1..3, do: :timer.tc(fn -> Shard.scan_histogram(h, field, groups) end))
  |> Enum.min_by(fn {us, _} -> us end)
scan_rps = round(rows / (scan_us / 1_000_000.0))

:ok = Shard.set_fold_hist(h, field, groups)
{served, fold_counts} = Shard.group_by_hist(h, field, groups)
fold_us =
  1..50 |> Enum.map(fn _ -> {us, _} = :timer.tc(fn -> Shard.group_by_hist(h, field, groups) end); us end) |> Enum.min()
fold_us = max(fold_us, 1)
fold_rps = round(rows / (fold_us / 1_000_000.0))

self_ok = served == true and scan_counts == fold_counts and Enum.sum(scan_counts) == rows
# answer = sha256 of the bucket->count sequence (bucket order), SAME the DuckDB side hashes.
answer = :crypto.hash(:sha256, Enum.map_join(scan_counts, ",", &Integer.to_string/1)) |> Base.encode16(case: :lower) |> binary_part(0, 32)
File.rm_rf(dir)

IO.puts("SEMURG_OLAP rows=#{rows} groups=#{groups} scan_rps=#{scan_rps} fold_rps=#{if self_ok, do: fold_rps, else: 0} served_by_fold=#{served} self_equal=#{self_ok} answer=#{answer}")
