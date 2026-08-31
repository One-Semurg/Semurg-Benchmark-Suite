# Lanes

One directory per domain. Each domain has an **incumbent lane** per competitor and one **Semurg lane**.

```
lanes/
  <domain>/
    <incumbent>.sh|.q     # starts the incumbent in Docker, loads the dataset, runs its query, emits the answer + timing
  semurg/
    README.md             # the Semurg lane invokes the RELEASED Semurg binary (not engine source)
```

## Contract

Every lane prints one machine-readable result line, for example:

```
LANE=<name> STATUS=ok ANSWER=<hash> METRIC=<rows_per_s|qps|ms|GBps> VALUE=<n>
```

The runner then diffs the Semurg `ANSWER` against the incumbent `ANSWER` (equal-answer gate) and, only if they
match, compares the metrics.

## Datasets

Deterministic — see `generators/`. Both the incumbent and Semurg index the **same** generated data so the
answer comparison is meaningful. A fixed seed makes every run reproducible.

## Incumbent lanes

Incumbents run from their **official public Docker images** with the settings documented in each script
(single shard / segment where relevant, warmed, best-of-N). No incumbent is handicapped; where an incumbent
has a faster path than the default, the script uses it.

## Semurg lane

The Semurg lane calls the **released Semurg binary / installer** — the engine source is not part of this
repository. See `lanes/semurg/README.md`.
