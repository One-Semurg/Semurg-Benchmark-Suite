# Semurg Benchmark Suite

Reproducible, one-command benchmarks comparing **Semurg** to leading databases across eleven data domains —
key-value, graph, time-series, analytics, search, streaming, relational, object, document, vector, and
universal (multi-model, one dataset across every model at once). Docker lanes for each incumbent, equal-answer
gates, belt-proven measurements, scored against **10× / 100× / 1000×** tiers.

**This suite publishes no numbers.** You run it on your own hardware and see your own results, reproducible
on your machine. Lanes are landing domain by domain (work in progress); what is here is designed so you get
the same answer we do.

## The two measurements per task

Each task is measured two ways, and both are reported:

- **Relative** — Semurg **and** the incumbent each in a **Docker container, single node**. Apples-to-apples;
  this is the multiple that sets the tier.
- **Absolute** — Semurg on **bare metal, whole cluster, maximum concurrency**. The real product number.

## The tiers (work-deletion depth)

| Tier | Meaning |
|------|---------|
| **10×** | a faster kernel |
| **100×** | deleting the work (better representation / fewer bytes) |
| **1000×** | architectural — the out-of-core moat (bigger-than-RAM, where the incumbent cannot run at all) |

A result is only counted when it is **belt-proven** (the read path is witnessed: direct-IO conveyor engaged,
no page-cache fallback) **and** passes an **equal-answer gate** against the incumbent (identical results, byte
for byte or within documented float tolerance) — *before* any speed is claimed.

## Domains and incumbents

Each domain compares against widely-used open-source systems whose licences permit user-run benchmarking.
Some proprietary engines carry benchmark-disclosure ("DeWitt") clauses in their licences; this suite does not
name or ship reproduction lanes for those — you may of course point the same workload at any engine you are
licensed to benchmark.

| Domain | Incumbents (in this suite) |
|--------|-----------|
| Key-Value | Redis, RocksDB, Aerospike |
| Graph | Neo4j, Memgraph |
| Time-series | QuestDB, TimescaleDB, InfluxDB |
| Analytics / OLAP | DuckDB, ClickHouse |
| Search | Elasticsearch, OpenSearch, Meilisearch |
| Streaming | Kafka, Redpanda, Chronicle Queue |
| Relational | PostgreSQL, MySQL Community, CockroachDB |
| Object | MinIO |
| Document | MongoDB Community |
| Vector | Milvus, Qdrant |
| Universal (multi-model) | SurrealDB, ArangoDB |

## See where Semurg lands — on your own data

**This suite publishes no comparative numbers.** You run it on **your own hardware** and see **your own
results**. That is the honest measure — the numbers are yours, reproducible on your machine, not a marketing
chart. (It also means we make no third-party benchmark *publication*; you measure, you decide.)

For every task each lane prints, live, on your box:

- the **relative** multiple — Semurg vs the incumbent, both in Docker on a single node (apples-to-apples);
- the **absolute** throughput/latency — Semurg on bare metal, whole cluster, max concurrency;
- the **tier reached** (10× / 100× / 1000×) — reported only after the **equal-answer gate** passes and the
  read path is **belt-proven**.

Pick a domain and watch the numbers appear:

```
./run.sh analytics
```

Each domain exercises **large, public, domain-specific datasets** and hard, realistic workloads (deep graph
traversals, high-cardinality aggregations, full-text ranking, nearest-neighbour recall, out-of-core scale),
not toy inputs. Dataset fetch scripts live in `generators/`.

## Run it

```
./run.sh <domain>        # e.g. ./run.sh analytics
./run.sh all             # every domain
```

Each lane starts the incumbent in Docker, loads a deterministic dataset (generators are reproducible), runs
the Semurg lane, checks equal-answer, and prints the numbers. See `docs/METHODOLOGY.md` and `lanes/README.md`.

## What this repo is (and is not)

This is the **benchmark harness only**. The Semurg lane invokes the **released Semurg binary / installer** — the
Semurg engine source is **not** part of this repository. Incumbents run from their official public Docker
images. Nothing here contains credentials, keys, or private data (see `.gitignore`).

## Status

Early and honest: the harness, methodology, and the first incumbent lanes are here; the remaining lanes, the
dataset fetch scripts (`generators/`), and the published Semurg image/installer are landing incrementally. Until
a domain's Semurg lane and its dataset are wired, that domain runs the incumbent side only — no head-to-head
number is produced. When you see a comparison, it has passed the equal-answer gate on your machine.

## Fairness

In the relative configuration both Semurg and the incumbent run in Docker on a single node **with identical CPU
and memory limits**, and each lane prints the thread/core count it used on both sides. Incumbents are run with
their recommended settings; where a caveat applies (e.g. an in-process vs. over-the-wire path), the lane says so.

## Trademarks & affiliation

All product and company names referenced are trademarks of their respective owners and are used for
identification only. Semurg is not affiliated with, endorsed by, or sponsored by any of them. This project
publishes no third-party benchmark results; it is a tool you run yourself.

## License

Apache-2.0 (see `LICENSE`). The licence covers the harness scripts in this repository only — not the Semurg
engine (downloaded separately) and not the third-party database images (each under its own licence).
