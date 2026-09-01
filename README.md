# Semurg Benchmark Suite

**You don't have to trust our numbers — run them yourself, on your own hardware.**

This is the benchmark harness only (the Semurg engine source is not in this repo). Each lane stands up an
open-source incumbent from its official image, runs the SAME queries on the SAME deterministic data, and
counts a result ONLY if its answer hash matches an independent reference — equal-answer, bit-exact, no fake
numbers. Losses are printed straight, next to the wins.

**Install Semurg first** — the first node is always free: https://github.com/One-Semurg/Semurg-Install
(or `./bin/semurg-arena install`). Then run the arena below. To add nodes or discuss your use case: one.semurg.io.

> License-restricted engines (kdb+, Elasticsearch, TigerGraph, Memgraph, Dragonfly) carry benchmark-disclosure
> ("DeWitt") clauses, so this public suite ships **no lanes** for them. You may of course benchmark any engine
> you are licensed to run, locally, yourself.

---

# Semurg Arena Kit -- reproduce the board on your own hardware

You do not have to trust our numbers. Run them yourself.

## Quick start
    # 1. install the free Semurg node (downloads + verifies the published release):
    ./bin/semurg-arena install     # or: https://github.com/One-Semurg/Semurg-Install
    ./bin/semurg-arena install        # install Semurg R11 (native, on this box)
    ./bin/semurg-arena run --all       # SQL board + open public-license engines, equal-answer
    ./bin/semurg-arena run --graph     # GRAPH head-to-head: Semurg vs Neo4j vs Kuzu (the crown)
    ./bin/semurg-arena run --olap      # OLAP head-to-head: Semurg (scan+fold) vs DuckDB, equal-answer
    ./bin/semurg-arena list            # every lane, its licence, wired/planned status

`run --all` generates a deterministic dataset, stands up each open public-license engine, runs the
SAME queries (Q1 aggregate, Q2 point, Q3 filtered count), and prints one decomposed table. A lane
counts ONLY if its answer hash matches the reference -- mismatches are shown, never hidden.

## The graph crown (`run --graph`)
`run --graph` ingests ONE deterministic graph into Semurg + Neo4j (GPLv3 CE) + Kuzu (MIT) and runs the
SAME k-hop traversal, gated by an INDEPENDENT reference answer (a formula-BFS reachable-count): a lane
counts only if its `nodes_visited` matches. Two regimes:

  * **in-core** (fits everyone's RAM): reported straight. Semurg is competitive and typically beats
    Neo4j here; against a Memgraph/TigerGraph-class engine we may LOSE raw traversal throughput -- we
    do not claim in-core domination, run those yourself (they are license-restricted, opt-in-local).
  * **out-of-core** (the crown): a graph bigger than a FIXED small memory budget (default 2 GB, set by
    the lane -- so the result reproduces regardless of how much RAM your box has). At that budget the
    engines that keep the graph in RAM cannot build/hold it and **DNF**, while Semurg finishes the
    traversal at FLAT memory (disk = truth). That survive-vs-DNF property is the point.

Memgraph / TigerGraph forbid third-party published benchmarks, so they are never on this public board;
run them yourself, local-only, via `run --licensed`.

## Semurg vs DuckDB, the honest way (`run --olap`)
`run --olap` runs the SAME narrow COUNT(*) GROUP BY over the SAME rows in Semurg and DuckDB, equal-answer
gated (identical per-bucket counts). It prints Semurg TWO ways, with the direction stated straight:
  * **raw scan** -- Semurg's O(N) SIMD sweep. A columnar warehouse is built for exactly this, so Semurg
    LOSES here (measured ~25x on our node). We print the loss; no spin.
  * **fold** -- the identical histogram served O(1) off a running monoid (a point-get, not a scan). This
    is the architectural win (delete the scan, don't try to out-scan the scanner) -- measured thousands
    of times DuckDB's aggregate throughput, same bit-exact answer.

## What is on the public board
Open-source / permissively licensed engines only (see MANIFEST.tsv). Wired today: the SQL board
(sqlite + duckdb embedded, plus the Docker SQL lane postgres; the mysql/mariadb/clickhouse/timescale/
questdb/mongodb/redis lanes are scaffolded and listed as *planned* -- contributions welcome) AND the graph head-to-head (Semurg vs neo4j + kuzu, `run --graph`). Vector /
search / stream category lanes are declared in MANIFEST.tsv and land in a later revision -- honestly
skipped until then, never faked.

## License-restricted engines (kdb+, Elasticsearch, TigerGraph, Memgraph)
These forbid third-party published benchmarks or restrict redistribution, so Semurg neither ships
them nor puts their numbers on any public page. If you want them in YOUR comparison:
    I_HAVE_A_LICENCE=yes ./bin/semurg-arena run --licensed
You install them yourself, from the vendor, under your own licence. Results are written only to
./arena_local_results/ on this machine. They are never uploaded and never leave your box.

## Honest by construction
- Deterministic data: same seed -> byte-identical CSV everywhere.
- Equal-answer gate: engines must agree on the answer before any timing is compared.
- Your hardware, your numbers. Our published numbers were measured on our reference node and are
  labelled as such at https://data.semurg.io -- this kit is how you check them.
