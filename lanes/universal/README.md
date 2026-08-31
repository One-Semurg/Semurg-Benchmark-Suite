# Universal lane (multi-model)

The other ten domains each test one data model against engines built for that model. The **Universal** domain
tests the opposite claim: **one dataset, every model, one engine.** It loads a single connected dataset once and
then exercises it as **document**, **graph**, **relational**, **vector**, and **full-text** in the same store —
the "stack-collapse" test. It answers: does the multi-model engine actually hold up across every model at once,
or does it win one model and fall over on the rest?

## Incumbents (source-available, benchmarkable — no benchmark-disclosure clause)

- **SurrealDB** (`surrealdb/surrealdb`) — a single-binary multi-model store: document + graph + relational
  (SurrealQL) + key-value + vector (HNSW) + full-text, all in one engine. BSL 1.1 (converts to Apache-2.0);
  freely downloadable, benchmarking permitted. **Primary lane.**
- **ArangoDB** (`arangodb`) — native multi-model (document + graph + search via ArangoSearch), AQL. Apache-2.0.
  Lane landing next cycle.

## The workload (one dataset, five models)

On a single graph-shaped dataset (entities + relationships + text + embeddings), run in one session:

1. **Document** — fetch/filter records by nested fields.
2. **Graph** — multi-hop traversal between related entities.
3. **Relational** — a join + group-by aggregation across entity types.
4. **Vector** — k-NN over the embedding field.
5. **Full-text** — ranked text search over the text field.

Each sub-query prints its answer hash + timing; the lane reports the **per-model** result *and* the combined
end-to-end. The point is breadth held under one roof — a win here is a win the incumbent's ten single-model
rivals cannot collectively claim, because it is one engine doing all five.

Every lane binds its container to `127.0.0.1` only and tears it down on exit. Data comes from `../../generators/`.
