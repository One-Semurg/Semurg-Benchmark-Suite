#!/usr/bin/env bash
# gen_biggraph.sh -- deterministic BIGGER-THAN-BUDGET graph generator for the EXTREME-HOPS graph lane
# (the third graph benchmark). Same knobs -> byte-identical edge list on any machine, so Semurg + Neo4j
# + Kuzu ingest the SAME graph and the equal-answer check at each hop depth is meaningful.
#
# Structure (identical formula to workload/gen_graph.sh, so the two lanes stay comparable):
#   N nodes (1..N), each with up to DEG distinct directed out-edges:
#     dst(id,k) = ((id-1)*A + k) mod N + 1     for k=1..DEG,  skipping any self-loop (dst==id)
#   A is a fixed prime coprime to N, so multiplying the source id SCRAMBLES each node's neighbour block
#   to a different region -> the k-hop frontier fans out (~DEG^hops until saturation): a real expander
#   whose working set exceeds the fixed memory budget (that is what makes it "bigger than RAM").
#
# "BIGGER THAN RAM" is enforced by a FIXED memory budget in the runner (GRAPH_MEM_CAP, default 2G), not
# by how much RAM the box happens to have -- so the survive-vs-DNF crown is reproducible everywhere. Pick
# N so N*DEG*(edge bytes) exceeds that budget several times over (see SIZING below).
#
# Because dst is a pure FORMULA, the reference reachable-count at every hop depth is computed WITHOUT
# storing adjacency (one BFS to saturation, O(reachable) memory), independent of any engine under test.
# On an expander with out-degree DEG the frontier saturates the reachable component in ~log_DEG(N) hops,
# so the answer CURVE is short (a handful of rows) and every depth >= saturation shares one visited-count.
#
# Emits:
#   $OUT.edges.tsv       TAB  src<TAB>dst          (SNAP contract the bulk loaders parse)
#   $OUT.seeds.txt       one seed node id per line (NSEEDS evenly-spaced, deterministic)
#   $OUT.answercurve.tsv TAB  hop<TAB>visited      (reference: distinct nodes reachable within <=hop of
#                        the seeds, directed out-edges, INCLUDING seeds == Semurg walk semantics; rows
#                        from hop=0 up to saturation). Gated by GEN_REF=1 (default 1); set GEN_REF=0 to
#                        skip for a huge graph where you only care about the survive-vs-DNF crown.
#   $OUT.meta.tsv        key<TAB>value            (N, DEG, A, edges, seeds, saturation hop+count)
#
# SIZING (edge list on disk ~ N*DEG*13 bytes for TSV; each engine's in-memory graph is several x that):
#   self-test  N=20000    DEG=10  -> ~200k edges  (~2.6 MB tsv)   fits everything, proves the mechanism
#   small OOC  N=3000000  DEG=10  -> ~30M edges    (~380 MB tsv)   exceeds a 256M budget
#   real OOC   N=50000000 DEG=10  -> ~500M edges   (~6.5 GB tsv)   exceeds a 2G budget several-fold
set -euo pipefail
OUT="${1:-biggraph}"                    # output basename (writes $OUT.edges.tsv etc.)
N="${GRAPH_NODES:-3000000}"
DEG="${GRAPH_DEG:-10}"
NSEEDS="${GRAPH_SEEDS:-64}"
A="${GRAPH_A:-1000003}"                 # mixing multiplier; must be coprime to N (asserted below)
GEN_REF="${GEN_REF:-1}"                 # 1 = emit the reference answer curve; 0 = skip (huge-graph mode)

# --- A must be coprime to N (else the id-scramble is not a bijection). Assert with gcd. ---
g=$(awk -v a="$A" -v b="$N" 'function gcd(x,y){while(y){t=x%y;x=y;y=t}return x} BEGIN{print gcd(a%b,b)}')
if [ "$g" != "1" ]; then
  echo "gen_biggraph: GRAPH_A=$A is not coprime to GRAPH_NODES=$N (gcd=$g); pick another prime A" >&2
  exit 2
fi

edges="$OUT.edges.tsv"; seeds="$OUT.seeds.txt"; curve="$OUT.answercurve.tsv"; meta="$OUT.meta.tsv"

# --- 1. edge list: up to N*DEG directed edges, TAB-separated. ---
awk -v n="$N" -v deg="$DEG" -v a="$A" 'BEGIN{
  for (id=1; id<=n; id++){
    base=((id-1)*a) % n
    for (k=1; k<=deg; k++){
      d=(base + k) % n + 1
      if (d != id) printf "%d\t%d\n", id, d
    }
  }
}' > "$edges"
EDGES=$(wc -l < "$edges")

# --- 2. seeds: NSEEDS evenly-spaced node ids (deterministic). ---
awk -v n="$N" -v ns="$NSEEDS" 'BEGIN{
  step=int(n/ns); if(step<1)step=1; c=0;
  for (s=1; s<=n && c<ns; s+=step){ print s; c++ }
}' > "$seeds"
NS=$(wc -l < "$seeds")

# --- 3. reference answer CURVE: one formula-BFS to saturation, recording visited-count after each hop. ---
#     SAME formula + SAME self-loop skip as the edge list, so it is over the identical graph. Semantics
#     match Semurg's walk (distinct nodes within <=hop, incl seeds). Row hop=0 is the seed count.
SAT_HOP=""; SAT_CNT=""
if [ "$GEN_REF" = "1" ]; then
  awk -v n="$N" -v deg="$DEG" -v a="$A" '
    { seed[$1]=1 }
    END{
      nf=0
      for (s in seed){ if(!(s in seen)){ seen[s]=1; fr[nf++]=s } }
      c=0; for (x in seen) c++; print 0"\t"c            # hop 0 = seeds
      h=0
      while (1){
        h++
        nn=0
        for (i=0;i<nf;i++){
          id=fr[i]+0
          base=((id-1)*a) % n
          for (k=1;k<=deg;k++){
            d=(base + k) % n + 1
            if (d==id) continue
            if(!(d in seen)){ seen[d]=1; nfr[nn++]=d }
          }
        }
        c=0; for (x in seen) c++; print h"\t"c
        if(nn==0) break
        delete fr; for(j=0;j<nn;j++) fr[j]=nfr[j]; nf=nn; delete nfr
      }
    }' "$seeds" > "$curve"
  SAT_HOP=$(tail -1 "$curve" | cut -f1)
  SAT_CNT=$(tail -1 "$curve" | cut -f2)
else
  : > "$curve"   # empty curve = parity checks are skipped by the runner
fi

# --- 4. meta ---
{
  printf 'nodes\t%s\n' "$N"
  printf 'deg\t%s\n' "$DEG"
  printf 'mult_A\t%s\n' "$A"
  printf 'edges\t%s\n' "$EDGES"
  printf 'seeds\t%s\n' "$NS"
  printf 'gen_ref\t%s\n' "$GEN_REF"
  printf 'saturation_hop\t%s\n' "${SAT_HOP:-NA}"
  printf 'saturation_visited\t%s\n' "${SAT_CNT:-NA}"
} > "$meta"

echo "gen_biggraph: N=$N DEG=$DEG edges=$EDGES seeds=$NS gen_ref=$GEN_REF saturation_hop=${SAT_HOP:-NA} saturation_visited=${SAT_CNT:-NA}"
echo "gen_biggraph: $edges  $seeds  $curve  $meta"
