# Methodology

The suite is built to be **honest and reproducible**. Three rules gate every number.

## 1. Equal-answer first

Before any speed number is reported, the Semurg lane must return the **same answer** as the incumbent for the
same input:

- deterministic dataset (the generators use a fixed seed, so both engines index identical data);
- the answer is captured on both sides and compared — byte-identical where the workload is exact (e.g. an
  integer aggregation, a BM25 top-k), or within a documented float tolerance where the incumbent itself is
  lossy;
- for search, parity is checked layer by layer (tokenisation, document frequencies, length norms, per-term
  sub-scores, then the final ranked list) against the incumbent's own `_explain`.

If the answers differ, the lane is fixed until they match. **No speed claim ships before equal-answer is green.**

## 2. Belt-proven

Semurg's read path is a deep-queue-depth direct-IO conveyor (`disk → cache tiers → CPU`). Every measured lane
witnesses that the conveyor is actually engaged — direct-IO opens succeed, there is no buffered/page-cache
fallback, and cold vs warm behaviour is visible — so a number can never be a page-cache artifact. A run whose
counters show the read path escaped the conveyor is void, not reported.

## 3. Two configurations, both reported

- **Relative** — Semurg and the incumbent each in a **Docker container on a single node**. This is the
  apples-to-apples multiple that sets the tier.
- **Absolute** — Semurg on **bare metal across the whole cluster at maximum concurrency**. This is the real
  product number and is never laundered into the relative multiple.

## Tiers

Speed-ups are classified by *how* they are achieved, not just the number:

- **10×** — a faster kernel (SIMD, batching, better memory layout).
- **100×** — deleting work: a denser representation (fewer bytes per row), skipping, pre-computation.
- **1000×** — architectural: the out-of-core moat. When the dataset is larger than RAM, the incumbent cannot
  run at all; Semurg streams it from disk. That is an unbounded win, not a multiple.

## Three-cycle measurement

Each lane runs cold → warm → warm again, so the conveyor filling, the cache warming, and the packing pass are
all visible in the progression rather than hidden behind a single number.

## What is measured vs. what is claimed

Where a comparison is not perfectly apples-to-apples (e.g. an in-process Semurg number vs. an incumbent behind
a network protocol, or an incumbent measured single-connection rather than at its concurrent maximum), the
caveat is stated next to the number. The goal is the honest picture, not a flattering one.
