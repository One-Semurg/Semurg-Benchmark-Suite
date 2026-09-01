# Semurg GRAPH lane core: open a throwaway store, bulk-ingest the shared edge list, build the
# co-located traversal image (LOAD), then run a whole-graph k-hop from the shared seeds HOT (image
# pre-built) so the query time is comparable to the incumbents' hot query. Prints one machine-readable
# line. Reads: GRAPH_EDGES, GRAPH_SEEDS_FILE, GRAPH_HOPS, GRAPH_STORE.
alias SubstrateCore.Shard

edges = System.fetch_env!("GRAPH_EDGES")
seeds_file = System.fetch_env!("GRAPH_SEEDS_FILE")
hops = String.to_integer(System.get_env("GRAPH_HOPS") || "3")
store = System.get_env("GRAPH_STORE") || "/tmp/semurg_graph_#{System.system_time(:millisecond)}.bin"

seeds =
  seeds_file |> File.read!() |> String.split(~r/\s+/, trim: true) |> Enum.map(&String.to_integer/1)

peak_rss_mb = fn ->
  case File.read("/proc/self/status") do
    {:ok, s} ->
      case Regex.run(~r/VmHWM:\s+(\d+)\s+kB/, s) do
        [_, kb] -> div(String.to_integer(kb), 1024)
        _ -> -1
      end

    _ ->
      -1
  end
end

{:ok, r} = Shard.Native.shard_open(store)
h = {:native, r}

# LOAD = ingest + build the co-located traversal image once (analogous to an incumbent's load+index).
{load_us, {nodes, e}} = :timer.tc(fn -> Shard.ingest_edgelist_bulk(h, edges) end)
{pack_us, {:ok, _}} = :timer.tc(fn -> Shard.pack_graph(h) end)

# QUERY = HOT k-hop (image already built): median of 3 `walk/4` (no rebuild), deterministic counts.
runs =
  for _ <- 1..3 do
    {us, {:ok, {v, ed}}} = :timer.tc(fn -> Shard.walk(h, seeds, hops) end)
    {us, v, ed}
  end

{_, visited, edge_hops} = List.last(runs)
med_us = runs |> Enum.map(&elem(&1, 0)) |> Enum.sort() |> Enum.at(1)
teps = if med_us > 0, do: round(edge_hops / (med_us / 1_000_000)), else: 0

IO.puts(
  "SEMURG_GRAPH nodes=#{nodes} in_edges=#{e} load_ms=#{div(load_us + pack_us, 1000)} " <>
    "query_ms=#{div(med_us, 1000)} visited=#{visited} edge_hops=#{edge_hops} teps=#{teps} peak_rss_mb=#{peak_rss_mb.()}"
)

for ext <- ["", ".arith", ".sb", ".coloc", ".genoff.idx", ".genoff.idx.sparse", ".deadset.idx"],
    do: File.rm(store <> ext)
