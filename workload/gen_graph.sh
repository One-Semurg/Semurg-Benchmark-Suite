#!/usr/bin/env bash
# gen_graph.sh -- deterministic graph generator for the arena GRAPH lane. Same knobs -> byte-identical
# edge list on any machine, so Semurg + Neo4j + Kuzu ingest the SAME graph and the equal-answer check is
# meaningful. Structure: N nodes (1..N), each with up to DEG distinct directed out-edges:
#   dst(id,k) = ((id-1)*A + k) mod N + 1     for k=1..DEG,  skipping any self-loop (dst==id)
# A is a fixed multiplier coprime to N, so multiplying the source id SCRAMBLES each node's neighbour
# block to a different region -> the k-hop frontier fans out (~DEG^hops until saturation), a real
# expander that builds a large working set (the whole point of the out-of-core crown). Because dst is a
# pure FORMULA, the reference k-hop reachable-count is computed WITHOUT storing adjacency (O(visited)
# memory), independent of any engine under test. No duplicate out-edges (k distinct) and no self-loops
# (skipped in BOTH the edge list and the reference BFS, so every engine ingests the identical graph).
# Emits: edges.tsv (TAB src<TAB>dst), seeds.txt, answer.txt (reference nodes_visited: distinct nodes
# reachable within <=HOPS of the seeds, directed out-edges, INCLUDING seeds == Semurg walk semantics).
set -euo pipefail
OUT="${1:-graph}"                       # output basename (writes $OUT.edges.tsv etc.)
N="${GRAPH_NODES:-100000}"
DEG="${GRAPH_DEG:-10}"
HOPS="${GRAPH_HOPS:-3}"
NSEEDS="${GRAPH_SEEDS:-64}"
A="${GRAPH_A:-1000003}"                 # mixing multiplier; must be coprime to N (asserted below)

# --- A must be coprime to N (else the id-scramble is not a bijection). Assert with gcd. ---
g=$(awk -v a="$A" -v b="$N" 'function gcd(x,y){while(y){t=x%y;x=y;y=t}return x} BEGIN{print gcd(a%b,b)}')
if [ "$g" != "1" ]; then
  echo "gen_graph: GRAPH_A=$A is not coprime to GRAPH_NODES=$N (gcd=$g); pick another prime A" >&2
  exit 2
fi

edges="$OUT.edges.tsv"; seeds="$OUT.seeds.txt"; answer="$OUT.answer.txt"

# --- 1. edge list: up to N*DEG directed edges, TAB-separated (SNAP contract the bulk loader parses). ---
awk -v n="$N" -v deg="$DEG" -v a="$A" 'BEGIN{
  for (id=1; id<=n; id++){
    base=((id-1)*a) % n
    for (k=1; k<=deg; k++){
      d=(base + k) % n + 1
      if (d != id) printf "%d\t%d\n", id, d
    }
  }
}' > "$edges"

# --- 2. seeds: NSEEDS evenly-spaced node ids (deterministic). ---
awk -v n="$N" -v ns="$NSEEDS" 'BEGIN{
  step=int(n/ns); if(step<1)step=1; c=0;
  for (s=1; s<=n && c<ns; s+=step){ print s; c++ }
}' > "$seeds"

# --- 3. reference answer: formula-BFS, distinct reachable within <=HOPS incl. seeds. O(visited) mem. ---
#     SAME formula + SAME self-loop skip as the edge list, so it is over the identical graph.
awk -v n="$N" -v deg="$DEG" -v a="$A" -v hops="$HOPS" '
  { seed[$1]=1 }
  END{
    nf=0
    for (s in seed){ if(!(s in seen)){ seen[s]=1; fr[nf++]=s } }
    for (h=1; h<=hops; h++){
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
      if(nn==0) break
      delete fr; for(j=0;j<nn;j++) fr[j]=nfr[j]; nf=nn; delete nfr
    }
    c=0; for (x in seen) c++
    print c
  }' "$seeds" > "$answer"

VIS=$(cat "$answer")
echo "gen_graph: N=$N DEG=$DEG edges=$((N*DEG)) seeds=$(wc -l < "$seeds") hops=$HOPS reference_nodes_visited=$VIS"
echo "gen_graph: $edges  $seeds  $answer"
