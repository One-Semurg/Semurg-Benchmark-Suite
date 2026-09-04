# Semurg EXTREME-HOPS lane core: open a throwaway store, bulk-ingest the shared edge list, build the
# co-located traversal image ONCE (LOAD), then sweep a list of hop DEPTHS, running a HOT whole-graph
# k-hop at each depth with a PER-DEPTH wall-clock cap. On a cap breach (or crash) it prints a truncate
# line and HALTS -- deeper depths are not attempted (the line truncates at that depth). Because the walk
# is SET-based BFS (distinct-nodes frontier, not path enumeration), an expander SATURATES its reachable
# component in ~log_DEG(N) hops, so every depth beyond saturation returns the same visited-count almost
# instantly -- which is exactly why Semurg survives depths where a path-enumerating engine explodes.
#
# One machine-readable line PER DEPTH:
#   SEMURG_HOP depth=D status=ok query_ms=.. cumulative_ms=.. visited=.. peak_rss_mb=..
#   SEMURG_HOP depth=D status=cap query_ms=.. cumulative_ms=.. visited=-1 peak_rss_mb=..   (then halt)
#
# Reads: GRAPH_EDGES, GRAPH_SEEDS_FILE, GRAPH_DEPTHS (comma list), GRAPH_PER_DEPTH_CAP_MS, GRAPH_STORE.
alias SubstrateCore.Shard

edges = System.fetch_env!("GRAPH_EDGES")
seeds_file = System.fetch_env!("GRAPH_SEEDS_FILE")
store = System.get_env("GRAPH_STORE") || "/tmp/semurg_bighops_#{System.system_time(:millisecond)}.bin"
cap_ms = String.to_integer(System.get_env("GRAPH_PER_DEPTH_CAP_MS") || "30000")

depths =
  (System.get_env("GRAPH_DEPTHS") || "2,4,8")
  |> String.split(",", trim: true)
  |> Enum.map(&(&1 |> String.trim() |> String.to_integer()))

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

# LOAD = ingest + build the co-located traversal image ONCE (analogous to an incumbent's load+index).
{load_us, {_nodes, _e}} = :timer.tc(fn -> Shard.ingest_edgelist_bulk(h, edges) end)
{pack_us, {:ok, _}} = :timer.tc(fn -> Shard.pack_graph(h) end)
IO.puts("SEMURG_LOAD load_ms=#{div(load_us + pack_us, 1000)} peak_rss_mb=#{peak_rss_mb.()}")

cleanup = fn ->
  for ext <- ["", ".arith", ".sb", ".coloc", ".genoff.idx", ".genoff.idx.sparse", ".deadset.idx"],
      do: File.rm(store <> ext)
end

# QUERY = HOT k-hop at each depth, each under a PER-DEPTH cap. cumulative_ms accumulates QUERY time only.
Enum.reduce_while(depths, 0, fn depth, cum ->
  parent = self()

  task =
    Task.async(fn ->
      {us, {:ok, {v, _ed}}} = :timer.tc(fn -> Shard.walk(h, seeds, depth) end)
      send(parent, :done)
      {div(us, 1000), v}
    end)

  case Task.yield(task, cap_ms) || Task.shutdown(task, :brutal_kill) do
    {:ok, {qms, visited}} ->
      cum2 = cum + qms

      IO.puts(
        "SEMURG_HOP depth=#{depth} status=ok query_ms=#{qms} cumulative_ms=#{cum2} " <>
          "visited=#{visited} peak_rss_mb=#{peak_rss_mb.()}"
      )

      {:cont, cum2}

    _ ->
      # timed out at cap -> truncate here. cumulative includes the capped time for the line chart.
      cum2 = cum + cap_ms

      IO.puts(
        "SEMURG_HOP depth=#{depth} status=cap query_ms=#{cap_ms} cumulative_ms=#{cum2} " <>
          "visited=-1 peak_rss_mb=#{peak_rss_mb.()}"
      )

      {:halt, cum2}
  end
end)

cleanup.()
