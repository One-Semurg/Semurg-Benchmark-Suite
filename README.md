# Semurg benchmark suite

Every comparison on [one.semurg.io/benchmark](https://one.semurg.io/benchmark) comes from one kit, and you can run it on
your own machine:

```bash
curl -fsSL https://one.semurg.io/bench | less                       # read it first
curl -fsSL https://one.semurg.io/bench | sudo bash -s -- list       # what it can run, with the disk and time each run needs
curl -fsSL https://one.semurg.io/bench | sudo bash -s -- --graph    # one domain
curl -fsSL https://one.semurg.io/bench | sudo bash -s -- --graph --only "semurg_docker arcadedb"
```

A graph bigger than memory: every engine, Semurg included, held to 12 GB of memory on SNAP's com-Friendster (1.8 billion
relationships). Set `sudo sysctl -w vm.nr_hugepages=0` first: memory reserved that way is outside the 12 GB limit, so the
runner refuses until it is zero.

```bash
curl -fsSL https://one.semurg.io/bench | sudo env BENCH_BAND=exceeds_cap ARENA_MEM_CAP_GB=12 BENCH_DATASET_GRAPH=friendster_cap bash -s -- --graph
```

- Every engine answers the same questions on the same data on the same machine.
- Every answer is checked against a reference answer before its time counts.
- Each result names the Semurg release it was measured on.
- The kit ships with each Semurg release; check the runner against
  [one.semurg.io/dl/SHA256SUMS](https://one.semurg.io/dl/SHA256SUMS).

If you think an engine should run with different settings, or you get a different result on your machine, open an issue
here with the command you ran and its output. We rerun with your settings.

Install a node: [one.semurg.io/install](https://one.semurg.io/install).
