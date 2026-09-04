#!/usr/bin/env bash
# ############################################################################################
# # run_all_domains.sh -- THE MASTER ORCHESTRATOR. All 11 benchmark domains, IN ORDER, each   #
# # with the SEMURG lane + every incumbent lane, equal-answer gated, into ONE unified TSV.     #
# #                                                                                            #
# #   1 GRAPH  2 VECTOR  3 TIME-SERIES  4 RELATIONAL  5 COLUMNAR  6 KEY-VALUE                   #
# #   7 OBJECT 8 DOCUMENT 9 STREAMING  10 SEARCH     11 UNIVERSAL (multi-modal)                #
# #                                                                                            #
# # DESIGN (one-path, no duplication):                                                         #
# #  * It REUSES the existing per-engine lane scripts (lanes/*.sh, lanes/licensed/*.sh) and     #
# #    the deterministic workload generators (workload/gen_*.sh). It does NOT re-implement any  #
# #    engine logic or any reference computation -- it drives the SAME lanes the individual     #
# #    domain runners drive, and normalises their machine-readable `LANE=...` lines into ONE    #
# #    results TSV: domain  engine  load_ms  q_ms(list)  equal_answer_hash  status.             #
# #  * SEMURG IS IN EVERY DOMAIN. Where a native Semurg lane exists (semurg_graph, semurg_olap) #
# #    it runs it; where one is not yet wired it names the intended substrate lane and reports  #
# #    an honest `planned` row (never a fake number) -- so the board lights up automatically    #
# #    the moment MAIN wires that lane. Relational Semurg = the OLAP-fold answer + graph        #
# #    point-get; KV/object/doc/search/stream/universal Semurg = the substrate lane.            #
# #  * EQUAL-ANSWER GATE. Each lane's answer hash is compared to the domain workload's          #
# #    reference (generator-emitted where the generator produces one -- graph nodes_visited,    #
# #    vector top-K hash; else the first OK lane establishes the cross-engine reference). A     #
# #    lane whose hash disagrees is status=ok-mismatch and is NEVER counted as a win.           #
# #  * DeWitt / licence-restricted engines (kdb+, Memgraph, TigerGraph, Elasticsearch,          #
# #    Dragonfly) run ONLY under --licensed (I_HAVE_A_LICENCE=yes) and are LABELLED             #
# #    "[DeWitt-internal]" in the engine column. By DEFAULT they never appear, so the default   #
# #    TSV is publication-safe. Their numbers must NEVER leave the box (see the banner).        #
# #                                                                                            #
# # SELF-TEST (safe, no heavy sweep): `--plan` enumerates the full plan and the exact per-      #
# # domain run commands and runs NOTHING; `bash -n` parses it. MAIN runs the real sweep.        #
# ############################################################################################
set -uo pipefail

HERE="$(cd "$(dirname "$0")" && pwd)"
LANES="${DOMAIN_LANES_DIR:-$HERE/lanes}"
LIC="$LANES/licensed"
WORK="${DOMAIN_WORK_DIR:-$HERE/workload}"
SCRATCH_ROOT="${DOMAIN_SCRATCH_ROOT:-${TMPDIR:-/tmp}/arena_all_domains}"
TO="${LANE_TIMEOUT:-900}"

# ---- flags -------------------------------------------------------------------------------
LICENSED=0          # --licensed : also run DeWitt lanes (labelled, internal-only)
PLAN_ONLY=0         # --plan     : enumerate the plan + run commands, run nothing
DRY=0               # --dry-run  : print each lane invocation, run nothing
SELFTEST=0          # --selftest-parse : feed synthetic LANE lines through the parser/gate (no data, no lanes)
DOMAINS_SEL=""      # --domains "1,3,5" : run a subset (default: all 11, in order)
OUT_TSV=""          # --out PATH : results TSV (default: arena_results/all_domains_<ts>.tsv)

usage(){ cat <<U
run_all_domains.sh -- all 11 domains, Semurg + incumbents, equal-answer, ONE TSV.

  --plan               enumerate the domain plan + exact per-domain run commands (runs nothing)
  --dry-run            show every lane invocation without executing it
  --licensed           ALSO run DeWitt/licence-restricted lanes (I_HAVE_A_LICENCE=yes),
                       labelled [DeWitt-internal] -- NEVER publish those rows
  --domains "1,3,5"    run only these domain numbers (default: all 11, in canonical order)
  --out PATH           results TSV path (default: $HERE/arena_results/all_domains_<utc>.tsv)
  --help

Dataset knobs (env, honoured by the reused generators): SEMURG_ARENA_ROWS, SEMURG_TS_ROWS,
OLAP_ROWS/OLAP_GROUPS, VEC_N/VEC_D/VEC_Q/VEC_K, GRAPH_INCORE_NODES/GRAPH_OOC_NODES/GRAPH_HOPS/
GRAPH_OOC_HOPS/GRAPH_MEM_CAP, LANE_TIMEOUT.
U
}
while [ $# -gt 0 ]; do
  case "$1" in
    --plan) PLAN_ONLY=1;;
    --dry-run) DRY=1;;
    --selftest-parse) SELFTEST=1;;
    --licensed) LICENSED=1;;
    --domains) DOMAINS_SEL="${2:-}"; shift;;
    --out) OUT_TSV="${2:-}"; shift;;
    -h|--help) usage; exit 0;;
    *) echo "run_all_domains: unknown arg '$1'" >&2; usage; exit 2;;
  esac
  shift
done

[ -f "$LANES/_common.sh" ] && . "$LANES/_common.sh" || { echo "run_all_domains: missing lanes/_common.sh (broken checkout)" >&2; exit 2; }

# ---- THE PLAN (ONE source of truth; both --plan and the runner iterate it) ----------------
# Fields, pipe-separated:  dnum | dname | lane_script | engine_label | role | licence | wtag
#   role    = semurg | public | licensed
#   licence = semurg | public | dewitt
#   wtag    = workload tag (which dataset + which reference bucket the lane belongs to)
# A lane_script that does not exist on disk is reported as an honest `planned` row (the intended
# substrate/incumbent lane), never faked. This is future-proof: wiring the lane lights the row up.
plan_lines(){ cat <<'PLAN'
1|graph|semurg_graph|semurg_graph|semurg|semurg|graph_incore
1|graph|semurg_graph|semurg_graph|semurg|semurg|graph_ooc
1|graph|kuzu|kuzu|public|public|graph_incore
1|graph|kuzu|kuzu|public|public|graph_ooc
1|graph|neo4j|neo4j|public|public|graph_incore
1|graph|neo4j|neo4j|public|public|graph_ooc
1|graph|licensed/memgraph|memgraph|licensed|dewitt|graph_incore
1|graph|licensed/memgraph|memgraph|licensed|dewitt|graph_ooc
1|graph|licensed/tigergraph|tigergraph|licensed|dewitt|graph_incore
1|graph|licensed/tigergraph|tigergraph|licensed|dewitt|graph_ooc
2|vector|semurg_vector|semurg_vector|semurg|semurg|vector
2|vector|faiss|faiss|public|public|vector
2|vector|qdrant|qdrant|public|public|vector
3|timeseries|semurg_ts|semurg_ts|semurg|semurg|ts
3|timeseries|timescale_ts|timescale_ts|public|public|ts
3|timeseries|questdb_ts|questdb_ts|public|public|ts
3|timeseries|licensed/kdb_ts|kdb_ts|licensed|dewitt|ts
4|relational|semurg_relational|semurg_relational|semurg|semurg|orders
4|relational|sqlite|sqlite|public|public|orders
4|relational|postgres|postgres|public|public|orders
4|relational|mysql|mysql|public|public|orders
4|relational|mariadb|mariadb|public|public|orders
5|columnar|semurg_olap|semurg_olap|semurg|semurg|olapfold
5|columnar|duckdb_olap|duckdb_olap|public|public|olapfold
5|columnar|clickhouse|clickhouse|public|public|olapfold
6|kv|semurg_kv|semurg_kv|semurg|semurg|kv
6|kv|redis|redis|public|public|kv
6|kv|rocksdb|rocksdb|public|public|kv
6|kv|licensed/dragonfly|dragonfly|licensed|dewitt|kv
7|object|semurg_object|semurg_object|semurg|semurg|object
7|object|minio|minio|public|public|object
8|document|semurg_doc|semurg_doc|semurg|semurg|doc
8|document|mongodb|mongodb|public|public|doc
9|streaming|semurg_stream|semurg_stream|semurg|semurg|stream
9|streaming|kafka|kafka|public|public|stream
10|search|semurg_search|semurg_search|semurg|semurg|search
10|search|opensearch|opensearch|public|public|search
10|search|licensed/elasticsearch|elasticsearch|licensed|dewitt|search
11|universal|semurg_universal|semurg_universal|semurg|semurg|universal
11|universal|surrealdb|surrealdb|public|public|universal
PLAN
}

DOMAIN_ORDER="1 2 3 4 5 6 7 8 9 10 11"
declare -A DNAME=( [1]=GRAPH [2]=VECTOR [3]=TIME-SERIES [4]=RELATIONAL [5]=COLUMNAR [6]=KEY-VALUE [7]=OBJECT [8]=DOCUMENT [9]=STREAMING [10]=SEARCH [11]=UNIVERSAL )

# a domain is selected if no --domains filter, or it is in the (comma) list
domain_selected(){ local d="$1"; [ -z "$DOMAINS_SEL" ] && return 0; case ",$DOMAINS_SEL," in *,"$d",*) return 0;; *) return 1;; esac; }

# ---- helpers -----------------------------------------------------------------------------
join_semi(){ local out=""; for x in "$@"; do [ -z "$x" ] && continue; out="${out:+$out;}$x"; done; printf '%s' "$out"; }

# Parse ONE lane's raw stdout (which carries a `LANE=...` machine line, or a `SKIP ...`/`status=FAILED`
# line) into the 5 normalised fields we emit. Sets: P_STATUS P_LOAD P_QMS P_HASH P_NOTE.
# Handles every lane schema in the kit:
#   graph  : LANE=x STATUS=ok LOAD_MS Q(UERY)_MS VISITED TEPS PEAK_RSS_MB   (answer = VISITED)
#   vector : LANE=x STATUS=ok LOAD_MS QUERY_MS ANSWER_HASH
#   olap-s : LANE=semurg_olap STATUS=ok SCAN_RPS FOLD_RPS SELF_EQUAL ANSWER
#   olap-d : LANE=duckdb_olap ROWS_PER_S ANSWER                             (no STATUS => ok if ANSWER)
#   sql/ts : LANE=x LOAD_MS Q1_MS Q2_MS Q3_MS ANSWER_HASH                   (no STATUS => ok if hash)
#   skip   : "SKIP x reason=..."  or  "LANE=x STATUS=skip REASON=..."  or  "status=FAILED reason=..."
parse_lane(){
  local raw="$1"
  P_STATUS=""; P_LOAD=""; P_QMS=""; P_HASH=""; P_NOTE=""
  local line skipl
  line="$(grep -m1 '^LANE=' <<<"$raw" 2>/dev/null)"
  skipl="$(grep -m1 -iE '^SKIP ' <<<"$raw" 2>/dev/null)"
  if [ -z "$line" ] && [ -n "$skipl" ]; then
    P_STATUS="skip"; P_NOTE="$(sed -n 's/^SKIP [^ ]* *reason=\(.*\)/\1/Ip' <<<"$skipl")"; return
  fi
  if [ -z "$line" ]; then
    P_STATUS="dnf"; P_NOTE="no-LANE-line(timeout-or-error)"; return
  fi
  # status token (uppercase STATUS= preferred, else lowercase status=)
  local st; st="$(sed -n 's/.*STATUS=\([A-Za-z]*\).*/\1/p' <<<"$line")"; [ -n "$st" ] || st="$(sed -n 's/.*status=\([A-Za-z]*\).*/\1/p' <<<"$line")"
  st="$(printf '%s' "$st" | tr 'A-Z' 'a-z')"
  # numeric + answer fields (whichever the schema carries)
  P_LOAD="$(sed -n 's/.*LOAD_MS=\([0-9]*\).*/\1/p' <<<"$line")"
  local q_query q1 q2 q3 scan fold rps vis teps rss
  q_query="$(sed -n 's/.*QUERY_MS=\([0-9]*\).*/\1/p' <<<"$line")"
  q1="$(sed -n 's/.*Q1_MS=\([0-9]*\).*/\1/p' <<<"$line")"
  q2="$(sed -n 's/.*Q2_MS=\([0-9]*\).*/\1/p' <<<"$line")"
  q3="$(sed -n 's/.*Q3_MS=\([0-9]*\).*/\1/p' <<<"$line")"
  scan="$(sed -n 's/.*SCAN_RPS=\([0-9]*\).*/\1/p' <<<"$line")"
  fold="$(sed -n 's/.*FOLD_RPS=\([0-9]*\).*/\1/p' <<<"$line")"
  rps="$(sed -n 's/.*ROWS_PER_S=\([0-9]*\).*/\1/p' <<<"$line")"
  vis="$(sed -n 's/.*VISITED=\([0-9]*\).*/\1/p' <<<"$line")"
  teps="$(sed -n 's/.*TEPS=\([0-9]*\).*/\1/p' <<<"$line")"
  rss="$(sed -n 's/.*PEAK_RSS_MB=\([0-9-]*\).*/\1/p' <<<"$line")"
  # answer hash (ANSWER_HASH | ANSWER | for graph, the gated quantity is VISITED)
  P_HASH="$(sed -n 's/.*ANSWER_HASH=\([0-9a-f]*\).*/\1/p' <<<"$line")"
  [ -n "$P_HASH" ] || P_HASH="$(sed -n 's/.*ANSWER=\([0-9a-f]*\).*/\1/p' <<<"$line")"
  [ -n "$P_HASH" ] || P_HASH="$vis"
  # q_ms list -- honest per-schema: query timings in ms where they exist, rate metrics labelled
  if [ -n "$q_query" ]; then P_QMS="query_ms=$q_query"
  elif [ -n "$q1$q2$q3" ]; then P_QMS="$(join_semi "${q1:+q1=$q1}" "${q2:+q2=$q2}" "${q3:+q3=$q3}")"
  elif [ -n "$scan$fold" ]; then P_QMS="$(join_semi "${scan:+scan_rps=$scan}" "${fold:+fold_rps=$fold}")"
  elif [ -n "$rps" ]; then P_QMS="rows_per_s=$rps"
  fi
  # graph extras into the note (TEPS/RSS do not fit the fixed columns)
  [ -n "$teps" ] && P_NOTE="$(join_semi "$P_NOTE" "teps=$teps")"
  [ -n "$rss" ] && P_NOTE="$(join_semi "$P_NOTE" "peak_rss_mb=$rss")"
  # status: explicit token, else ok if we recovered an answer, else dnf
  if [ -n "$st" ]; then P_STATUS="$st"
  elif [ -n "$P_HASH" ]; then P_STATUS="ok"
  else P_STATUS="dnf"; [ -n "$P_NOTE" ] || P_NOTE="$(sed -n 's/.*REASON=\(.*\)/\1/p;s/.*reason=\(.*\)/\1/p' <<<"$line")"; fi
  [ -n "$P_NOTE" ] || P_NOTE="$(sed -n 's/.*REASON=\(.*\)/\1/p;s/.*reason=\(.*\)/\1/p' <<<"$line")"
}

emit_row(){ # dname engine load qms hash status
  printf '%s\t%s\t%s\t%s\t%s\t%s\n' "$1" "$2" "${3:--}" "${4:--}" "${5:--}" "$6" >> "$OUT_TSV"
}

# ---- workload references (equal-answer buckets) -------------------------------------------
declare -A REF        # wtag -> reference hash/quantity
declare -A REF_SRC    # wtag -> where the reference came from (generator | first-ok-lane)
declare -A GEN_DONE   # wtag -> 1 once the dataset for that tag has been generated
declare -A GBASE      # wtag -> base path of the generated dataset

# gate a lane's parsed result against its workload reference. Sets the global FSTAT (NOT echoed:
# it MUST run in the current shell so it can persist the first-OK reference into REF[] -- a command
# substitution subshell would discard that write and every lane would look like a fresh reference).
gate_status(){ # wtag
  local wtag="$1"
  case "$P_STATUS" in
    ok)
      if [ -z "${REF[$wtag]:-}" ]; then
        # first OK lane establishes the cross-engine reference for tags without a generator answer
        REF[$wtag]="$P_HASH"; [ -n "${REF_SRC[$wtag]:-}" ] || REF_SRC[$wtag]="first-ok-lane"
        FSTAT="ok-reference"
      elif [ "$P_HASH" = "${REF[$wtag]}" ]; then FSTAT="ok-matched"
      else FSTAT="ok-mismatch"; fi;;
    skip) FSTAT="skip";;
    planned) FSTAT="planned";;
    *) FSTAT="dnf";;
  esac
}

# ---- dataset generation per workload tag (reuses workload/gen_*.sh -- no logic duplicated) --
gen_for_tag(){ # wtag
  local wtag="$1"; [ -n "${GEN_DONE[$wtag]:-}" ] && return 0
  local sc="$SCRATCH_ROOT/$wtag"; rm -rf "$sc" 2>/dev/null; mkdir -p "$sc"
  case "$wtag" in
    graph_incore)
      local base="$sc/g"
      GRAPH_NODES="${GRAPH_INCORE_NODES:-100000}" GRAPH_DEG=10 GRAPH_HOPS="${GRAPH_HOPS:-4}" GRAPH_SEEDS="${GRAPH_SEEDS:-64}" \
        bash "$WORK/gen_graph.sh" "$base" >/dev/null
      awk '{print $1","$2}' "$base.edges.tsv" > "$base.edges.csv"
      seq 1 "${GRAPH_INCORE_NODES:-100000}" > "$base.nodes.csv"
      GBASE[$wtag]="$base"; REF[$wtag]="$(cat "$base.answer.txt")"; REF_SRC[$wtag]="generator(formula-BFS nodes_visited)";;
    graph_ooc)
      local base="$sc/g"
      GRAPH_NODES="${GRAPH_OOC_NODES:-3000000}" GRAPH_DEG=10 GRAPH_HOPS="${GRAPH_OOC_HOPS:-4}" GRAPH_SEEDS="${GRAPH_SEEDS:-64}" \
        bash "$WORK/gen_graph.sh" "$base" >/dev/null
      awk '{print $1","$2}' "$base.edges.tsv" > "$base.edges.csv"
      seq 1 "${GRAPH_OOC_NODES:-3000000}" > "$base.nodes.csv"
      GBASE[$wtag]="$base"; REF[$wtag]="$(cat "$base.answer.txt")"; REF_SRC[$wtag]="generator(formula-BFS nodes_visited)";;
    vector)
      local base="$sc/vec"
      VEC_N="${VEC_N:-20000}" VEC_D="${VEC_D:-64}" VEC_Q="${VEC_Q:-100}" VEC_K="${VEC_K:-10}" \
        bash "$WORK/gen_vectors.sh" "$base" >/dev/null
      GBASE[$wtag]="$base"; REF[$wtag]="$(cat "$base.answer.hash")"; REF_SRC[$wtag]="generator(exact top-K awk)";;
    ts)
      local data="$sc/ts_data.csv"
      SEMURG_TS_ROWS="${SEMURG_TS_ROWS:-500000}" bash "$WORK/gen_timeseries.sh" "$data" >/dev/null
      GBASE[$wtag]="$data"; REF_SRC[$wtag]="first-ok-lane(cross-engine downsample)";;
    orders)
      local data="$sc/orders.csv"
      SEMURG_ARENA_ROWS="${SEMURG_ARENA_ROWS:-200000}" bash "$WORK/gen_dataset.sh" "$data" >/dev/null
      GBASE[$wtag]="$sc"; REF_SRC[$wtag]="first-ok-lane(sqlite, orders Q1/Q2/Q3)";;
    olapfold)
      GBASE[$wtag]="$sc"; REF_SRC[$wtag]="first-ok-lane(semurg_olap self-equal scan==fold)";;
    *)
      # kv/object/doc/stream/search/universal -- no shared generator yet
      GBASE[$wtag]="$sc"; REF_SRC[$wtag]="first-ok-lane(pending category dataset)";;
  esac
  GEN_DONE[$wtag]=1
}

# ---- per-lane environment (reuses each lane's documented env contract) --------------------
# Runs one lane script under the correct env for its workload tag; captures stdout into $RAW.
run_lane_capture(){ # lane_script wtag scratch
  local script_rel="$1" wtag="$2" sc="$3"
  local script="$LANES/$script_rel.sh"
  [ -f "$script" ] || script="$LANES/$script_rel.sh"   # licensed/ prefix already in script_rel
  if [ ! -f "$script" ]; then RAW="__PLANNED__"; return; fi
  mkdir -p "$sc" 2>/dev/null || true
  local base="${GBASE[$wtag]:-$sc}"
  # licensed lanes require the operator's licence assertion
  local licenv=(); [ "$LICENSED" = 1 ] && licenv=(I_HAVE_A_LICENCE=yes)
  case "$wtag" in
    graph_incore|graph_ooc)
      local cap=0; [ "$wtag" = graph_ooc ] && cap="${GRAPH_MEM_CAP:-2G}"
      local hops="${GRAPH_HOPS:-4}"; [ "$wtag" = graph_ooc ] && hops="${GRAPH_OOC_HOPS:-4}"
      local ncap; case "$cap" in 0|"") ncap=0;; *[Gg]) ncap=$(( ${cap%[Gg]} * 1024 ));; *[Mm]) ncap=${cap%[Mm]};; *) ncap=0;; esac
      local seeds_csv; seeds_csv="$(paste -sd, "$base.seeds.txt" 2>/dev/null)"
      RAW="$( env "${licenv[@]}" \
        GRAPH_WALK_EXS="${GRAPH_WALK_EXS:-$WORK/semurg_walk.exs}" \
        GRAPH_SCRATCH="$sc" GRAPH_EDGES="$base.edges.tsv" GRAPH_EDGES_CSV="$base.edges.csv" \
        GRAPH_NODES_CSV="$base.nodes.csv" GRAPH_SEEDS_FILE="$base.seeds.txt" GRAPH_SEEDS_CSV="$seeds_csv" \
        GRAPH_HOPS="$hops" GRAPH_MEM_CAP="$cap" GRAPH_CAP_MB="$ncap" \
        timeout -k 10 "${TO}s" bash "$script" </dev/null 2>/dev/null )";;
    vector)
      RAW="$( env "${licenv[@]}" \
        VEC_BASE_CSV="$base.base.csv" VEC_QUERY_CSV="$base.query.csv" VEC_META="$base.meta" \
        VEC_K="${VEC_K:-10}" VEC_SCRATCH="$sc" \
        timeout -k 10 "${TO}s" bash "$script" </dev/null 2>/dev/null )";;
    ts)
      # derive Q2 device + Q3 window from the ACTUAL data (same derivation the ts runner uses)
      local wlo whi dev
      read -r wlo whi < <(awk -F, 'NR>1{if(mn==""||$2<mn)mn=$2; if($2>mx)mx=$2} END{sp=mx-mn; printf "%d %d\n", mn+int(sp/4), mn+int(sp/2)}' "$base")
      dev="$(awk -F, 'NR>1{c[$1]++} END{best=-1;bid=0; for(k in c){ if(c[k]>best || (c[k]==best && k<bid)){best=c[k];bid=k} } print bid}' "$base")"
      RAW="$( env "${licenv[@]}" \
        TS_DATA="$base" TS_BUCKET="${TS_BUCKET:-3600}" WIN_LO="$wlo" WIN_HI="$whi" Q2_DEVICE="$dev" \
        SEMURG_TS_ROWS="${SEMURG_TS_ROWS:-500000}" \
        timeout -k 10 "${TO}s" bash "$script" </dev/null 2>/dev/null )";;
    orders)
      RAW="$( env "${licenv[@]}" ARENA_DATA="$base" \
        timeout -k 10 "${TO}s" bash "$script" </dev/null 2>/dev/null )";;
    olapfold)
      RAW="$( env "${licenv[@]}" \
        OLAP_EXS="${OLAP_EXS:-$WORK/semurg_olap.exs}" OLAP_SCRATCH="$sc" \
        OLAP_ROWS="${OLAP_ROWS:-20000000}" OLAP_GROUPS="${OLAP_GROUPS:-64}" \
        timeout -k 10 "${TO}s" bash "$script" </dev/null 2>/dev/null )";;
    *)
      RAW="$( env "${licenv[@]}" ARENA_DATA="$sc" \
        timeout -k 10 "${TO}s" bash "$script" </dev/null 2>/dev/null )";;
  esac
}

# ============================================================================================
# PLAN-ONLY : enumerate the plan + the exact per-domain run commands, run nothing.
# ============================================================================================
print_plan(){
  echo "############################################################################################"
  echo "# run_all_domains.sh -- PLAN (all 11 domains, in canonical order). Semurg is in EVERY domain.#"
  echo "# licensed (DeWitt) lanes shown here are run ONLY with --licensed and NEVER published.       #"
  echo "############################################################################################"
  local d
  for d in $DOMAIN_ORDER; do
    domain_selected "$d" || continue
    echo
    echo "== DOMAIN $d : ${DNAME[$d]} =="
    printf "   %-3s %-22s %-10s %-9s %-16s %s\n" "#" engine role licence lane-script wtag
    local n=0
    while IFS='|' read -r dnum dname script engine role lic wtag; do
      [ "$dnum" = "$d" ] || continue
      [ "$role" = licensed ] && [ "$LICENSED" != 1 ] && continue
      local sp="$LANES/$script.sh"; local wired="planned"; [ -f "$sp" ] && wired="wired"
      n=$((n+1))
      printf "   %-3s %-22s %-10s %-9s %-16s %s\n" "$n" "$engine" "$role" "$lic" "$script.sh($wired)" "$wtag"
    done < <(plan_lines)
  done
  echo
  echo "############################################################################################"
  echo "# EXACT per-domain run commands (each reuses the SAME lanes/generators this orchestrator drives)"
  echo "############################################################################################"
  cat <<CMDS

  # Whole board, publication-safe (no DeWitt), one TSV:
  bash $HERE/run_all_domains.sh --out $HERE/arena_results/all_domains.tsv

  # Whole board INCLUDING DeWitt internal engines (labelled; never publish those rows):
  I_HAVE_A_LICENCE=yes bash $HERE/run_all_domains.sh --licensed --out $HERE/arena_results/all_domains_internal.tsv

  # A single domain (e.g. GRAPH, then VECTOR):
  bash $HERE/run_all_domains.sh --domains 1
  bash $HERE/run_all_domains.sh --domains 2

  # The wired single-domain runners this orchestrator reuses under the hood:
  bash $HERE/graph_run.sh                                   # 1 GRAPH   (Semurg vs Neo4j vs Kuzu, in-core + OOC)
  bash $HERE/vector_run.sh                                  # 2 VECTOR  (Semurg vs FAISS vs Qdrant, exact top-K)
  bash $HERE/timeseries_run.sh                              # 3 TIME-SERIES (Semurg vs Timescale vs QuestDB)
  bash $HERE/olap_run.sh                                    # 5 COLUMNAR (Semurg scan+fold vs DuckDB)
  # Relational SQL board (domain 4) via the arena driver:
  bash $HERE/bin/semurg-arena run --all
  # DeWitt internal runners (LOCAL ONLY, never published):
  I_HAVE_A_LICENCE=yes bash $LIC/graph_run_licensed.sh      # 1 GRAPH  + Memgraph + TigerGraph
  I_HAVE_A_LICENCE=yes bash $LIC/timeseries_run_licensed.sh # 3 TIME-SERIES + kdb+
CMDS
  echo
  echo "Domains 6-11 (KV / OBJECT / DOCUMENT / STREAMING / SEARCH / UNIVERSAL): lane scripts are"
  echo "declared but several are still 'planned-category' -- those rows report status=planned"
  echo "(honest, never faked) until MAIN wires the substrate + incumbent lanes; the row lights up"
  echo "automatically the moment lanes/<engine>.sh exists."
}

# ============================================================================================
# RUNNER : drive every selected domain's lanes, gate, emit the unified TSV.
# ============================================================================================
run_board(){
  : > "$OUT_TSV"
  printf 'domain\tengine\tload_ms\tq_ms(list)\tequal_answer_hash\tstatus\n' >> "$OUT_TSV"
  echo "############################################################################################"
  echo "# run_all_domains.sh -- LIVE BOARD  ($(uname -sm), $(nproc) cores)  ->  $OUT_TSV"
  [ "$LICENSED" = 1 ] && echo "# --licensed ON: DeWitt rows are LABELLED [DeWitt-internal] and MUST NEVER be published."
  echo "############################################################################################"
  local d
  for d in $DOMAIN_ORDER; do
    domain_selected "$d" || continue
    echo; echo "== DOMAIN $d : ${DNAME[$d]} =="
    printf "   %-24s %-10s %-26s %-14s %s\n" engine load_ms q_ms equal-answer status
    # collect this domain's plan rows, generating each workload tag once
    while IFS='|' read -r dnum dname script engine role lic wtag; do
      [ "$dnum" = "$d" ] || continue
      [ "$role" = licensed ] && [ "$LICENSED" != 1 ] && continue
      local elabel="$engine"; [ "$lic" = dewitt ] && elabel="$engine[DeWitt-internal]"
      local sc="$SCRATCH_ROOT/$wtag/$engine"
      if [ "$DRY" = 1 ]; then
        # DRY: show the invocation, generate NO dataset, run NO lane.
        local sp="$LANES/$script.sh"; local wired="planned"; [ -f "$sp" ] && wired="wired"
        echo "   [dry-run] $engine  ($role/$lic, wtag=$wtag) -> lanes/$script.sh ($wired)"
        emit_row "$dname" "$elabel" "-" "-" "-" "dry-run"
        continue
      fi
      gen_for_tag "$wtag"
      run_lane_capture "$script" "$wtag" "$sc"
      if [ "${RAW:-}" = "__PLANNED__" ]; then
        P_STATUS=planned; P_LOAD=""; P_QMS=""; P_HASH=""; P_NOTE="lane-not-wired-yet(intended:lanes/$script.sh)"
      else
        parse_lane "$RAW"
      fi
      gate_status "$wtag"
      printf "   %-24s %-10s %-26s %-14s %s\n" "$elabel" "${P_LOAD:--}" "${P_QMS:--}" "${REF[$wtag]:+ref=${REF[$wtag]:0:8}}" "$FSTAT${P_NOTE:+ ($P_NOTE)}"
      emit_row "$dname" "$elabel" "$P_LOAD" "$P_QMS" "$P_HASH" "$FSTAT"
    done < <(plan_lines)
  done
  rm -rf "$SCRATCH_ROOT" 2>/dev/null || true
  echo
  echo "== equal-answer references used (independent where a generator emits one; else first-OK lane) =="
  local w
  for w in graph_incore graph_ooc vector ts orders olapfold kv object doc stream search universal; do
    [ -n "${REF_SRC[$w]:-}" ] && printf "   %-13s %s\n" "$w" "${REF_SRC[$w]}"
  done
  echo
  echo "== unified results TSV written: $OUT_TSV =="
  echo "   columns: domain  engine  load_ms  q_ms(list)  equal_answer_hash  status"
  echo "   status:  ok-reference (established the equal-answer ref) | ok-matched | ok-mismatch"
  echo "            | skip | dnf | planned (lane not yet wired -- never a fake number)"
  if [ "$LICENSED" = 1 ]; then
    echo "   NOTE: rows whose engine ends [DeWitt-internal] are licence-restricted -- LOCAL ONLY, never publish."
  fi
  return 0
}

# ============================================================================================
# SELF-TEST : prove parse_lane + gate_status normalise every lane schema, with NO dataset, NO
# lane execution, NO docker. Feeds one synthetic LANE=/SKIP line per schema and prints the TSV row.
# ============================================================================================
selftest_parse(){
  echo "== selftest-parse: synthetic LANE lines -> normalised TSV fields (no data, no lanes) =="
  printf "   %-18s %-24s | %-8s %-24s %-16s %s\n" schema in-wtag load q_ms hash status
  local cases=(
    "graph_incore|semurg_graph|LANE=semurg_graph STATUS=ok LOAD_MS=120 QUERY_MS=88 VISITED=99999 TEPS=1234567 PEAK_RSS_MB=512"
    "graph_ooc|neo4j|LANE=neo4j STATUS=dnf REASON=oom-at-cap-2G"
    "vector|faiss|LANE=faiss STATUS=ok LOAD_MS=30 QUERY_MS=12 ANSWER_HASH=deadbeefcafef00ddeadbeefcafef00d"
    "olapfold|semurg_olap|LANE=semurg_olap STATUS=ok SCAN_RPS=800000 FOLD_RPS=999000000 SELF_EQUAL=yes ANSWER=abc123abc123abc123abc123abc123ab"
    "olapfold|duckdb_olap|LANE=duckdb_olap ROWS_PER_S=25000000 ANSWER=abc123abc123abc123abc123abc123ab"
    "orders|sqlite|LANE=sqlite LOAD_MS=210 Q1_MS=9 Q2_MS=1 Q3_MS=7 ANSWER_HASH=feedfacefeedfacefeedfacefeedface"
    "orders|mysql|LANE=mysql LOAD_MS=980 Q1_MS=40 Q2_MS=2 Q3_MS=33 ANSWER_HASH=feedfacefeedfacefeedfacefeedface"
    "orders|mariadb|SKIP mariadb reason=docker-daemon-down(sudo systemctl start docker)"
    "ts|timescale_ts|LANE=timescale_ts LOAD_MS=1500 Q1_MS=60 Q2_MS=45 Q3_MS=20 ANSWER_HASH=0011223344556677889900aabbccddee"
    "kv|redis|LANE=redis STATUS=ok LOAD_MS=8200 GET_RPS_C1=210000 GET_RPS_C2=240000 GET_RPS_C3=245000 BATCH_RPS_C1=3500000 BATCH_RPS_C2=3700000 BATCH_RPS_C3=3800000 ROWS_PER_S=245000 ANSWER=48cd91f5d7162a858d526ef992d4b413 keys=1000000 batch=100 io_threads=16 clients=64 HIT_RATIO=1.0000 SELF=yes MODE=in-ram"
  )
  local c
  for c in "${cases[@]}"; do
    local schema="${c%%|*}"; local rest="${c#*|}"; local eng="${rest%%|*}"; local raw="${rest#*|}"
    parse_lane "$raw"
    gate_status "$schema"
    printf "   %-18s %-24s | %-8s %-24s %-16s %s\n" "$schema" "$eng" "${P_LOAD:--}" "${P_QMS:--}" "${P_HASH:--}" "$FSTAT"
  done
  echo
  echo "   Expect: graph row -> hash=VISITED(99999), q_ms=query_ms=88, teps/rss in note; the SECOND"
  echo "   olapfold+orders rows MATCH the first (ok-matched) since the hash equals the established ref;"
  echo "   dnf/skip carry their reason; mysql matches sqlite (ok-matched), duckdb matches semurg_olap."
}

# ---- main --------------------------------------------------------------------------------
if [ "$SELFTEST" = 1 ]; then selftest_parse; exit 0; fi
if [ "$PLAN_ONLY" = 1 ]; then print_plan; exit 0; fi
[ -n "$OUT_TSV" ] || OUT_TSV="$HERE/arena_results/all_domains_$(date -u +%Y%m%dT%H%M%SZ).tsv"
mkdir -p "$(dirname "$OUT_TSV")"
mkdir -p "$SCRATCH_ROOT"
run_board
exit 0
