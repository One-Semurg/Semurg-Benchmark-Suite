# Time-series lane

Incumbents in this lane are the open-source, benchmarkable time-series engines — the same set the standard
[TSBS](https://github.com/timescale/tsbs) suite covers:

- **QuestDB** (`questdb.sh`) — the closest open-source challenger to the entrenched HFT tick store: columnar,
  native `ASOF JOIN`, SQL with Postgres wire compatibility. Apache-2.0. **Primary lane.**
- **TimescaleDB** — PostgreSQL extension (hypertables + columnar compression). Reuses the `relational/` Postgres
  container with the TimescaleDB image; lane landing next cycle.
- **InfluxDB** — purpose-built TSDB (v3 columnar). Lane landing next cycle.

The workloads are the ones that actually separate a tick store from a generic column store: **`ASOF JOIN`**
(attach each trade its prevailing quote — the canonical as-of / `aj` query) and **per-symbol VWAP** over a
`SAMPLE BY` window, on the deterministic tick set (`trades.csv` + `quotes.csv`) from `../../generators/`.

Proprietary array-language tick databases carry benchmark-disclosure ("DeWitt") clauses and are **not** named
or shipped as lanes here — point the same `trades.csv` / `quotes.csv` at any engine you are licensed to
benchmark and compare the printed answer + timing yourself.

Every lane binds its container to `127.0.0.1` only and tears it down on exit.
