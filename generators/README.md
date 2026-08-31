# Data generators

Every lane reads a **deterministic** dataset so both sides — Semurg and the incumbent — index the exact same
bytes and the equal-answer gate is meaningful. Generators are seeded; the same seed produces the same dataset
on any machine. Nothing here is committed as data; you generate (or fetch) it locally into a working directory
and point the lane at it.

## Per-domain dataset contract

Each lane expects its inputs in a working directory (overridable per lane, e.g. `BM25_DIR`, `TS_DIR`), and
prints a `sha256` of its answer so two lanes can be compared:

| Domain | Files the lane reads | Shape |
|--------|----------------------|-------|
| Search | `bulk.ndjson`, `queries.txt` | one JSON doc per line (`{"index":{"_id":N}}` + `{"body":"..."}`); one query per line |
| Time-series | `trades.csv`, `quotes.csv` | `ts,sym,px,sz` and `ts,sym,bid,ask`, sorted by `ts` |

Columns landing as the lanes do (key-value: YCSB A–F; graph: SNAP edge lists; analytics: TPC-H / ClickBench;
vector: GloVe / SIFT1M; …).

## Two modes

- **Synthetic (default):** a seeded generator emits a reproducible dataset (e.g. a Zipf-distributed corpus for
  search, a Poisson tick stream for time-series). Deterministic and dependency-free.
- **Real public datasets (fetch scripts):** for hard, realistic scale each domain also has a fetch script for a
  well-known public dataset (SNAP graphs, TPC-H, MS-MARCO / Wikipedia, GloVe / SIFT, YCSB, real market ticks) and
  an out-of-core-scale variant. These are downloaded, never redistributed here.

Generators are being added domain by domain alongside the lanes; a domain whose generator is not yet here runs
against the synthetic default described above.
