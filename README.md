# Semurg benchmark suite

Every comparison on [one.semurg.io/benchmark](https://one.semurg.io/benchmark) comes from one kit, and you can run it on
your own machine:

```bash
curl -fsSL https://one.semurg.io/bench | less                       # read it first
curl -fsSL https://one.semurg.io/bench | sudo bash -s -- list       # what it can run, with the disk and time each run needs
curl -fsSL https://one.semurg.io/bench | sudo bash -s -- --graph    # one domain
curl -fsSL https://one.semurg.io/bench | sudo bash -s -- --graph --only "semurg_docker arcadedb"
```

- Every engine answers the same questions on the same data on the same machine.
- Every answer is checked against a reference answer before its time counts.
- Each result names the Semurg release it was measured on.
- The kit ships with each Semurg release; check the runner against
  [one.semurg.io/dl/SHA256SUMS](https://one.semurg.io/dl/SHA256SUMS).

If you think an engine should run with different settings, or you get a different result on your machine, open an issue
here with the command you ran and its output. We rerun with your settings.

Install a node: [one.semurg.io/install](https://one.semurg.io/install).
