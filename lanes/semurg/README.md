# Semurg lane

The Semurg side of every head-to-head runs the **released Semurg binary** (from the Semurg installer). The
Semurg engine source is **not** part of this repository — this suite proves the numbers without shipping the
engine.

## Getting Semurg

Install the free node (first node is always free, for testing and development), then point the lane at it:

```
export SEMURG_BIN=/opt/semurg/bin/<release>
```

## What the lane does

For a given domain/task the Semurg lane:

1. bulk-ingests the **same generated dataset** the incumbent indexed (deterministic seed);
2. runs the same logical query through the released engine;
3. emits the answer (for the equal-answer gate) and the belt-witness counters (proving the direct-IO conveyor
   is engaged) alongside the timing;
4. reports both the **relative** number (Semurg in Docker, single node) and the **absolute** number (bare
   metal, whole cluster, max concurrency).

Until a released benchmark entry point is wired for a given domain, that lane is marked *building* in the
scorecard and no speed number is claimed for it.
