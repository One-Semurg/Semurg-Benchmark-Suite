#!/usr/bin/env bash
# Redis KEY-VALUE lane (DOMAIN 6). Stands up the official redis:7.2 image on YOUR box, loads the SAME
# 1M-key, 20-byte-value keyspace the semurg_kv lane uses, then measures point GET + batch GET (MGET)
# throughput at MAX CONCURRENCY (all cores) and gates the run with the cross-engine equal-answer.
#
# WHAT MAKES IT APPLES-TO-APPLES (same keyset, same value function, same answer as semurg_kv):
#   value(id) = 20-byte big-endian id == printf '%040x' id (40 lowercase hex chars) -- byte-identical
#   to the Semurg KV lane's <<id::unsigned-big-integer-size(160)>>. Keys are k:%012d(id) so redis-
#   benchmark's __rand_int__ (12-digit zero-padded) lands EXACTLY on the loaded keys (proven by the
#   keyspace_hits check the lane reports as HIT_RATIO -- if it ever dropped, the number would be void).
#   ANSWER = first 32 hex chars of sha256 over the values of ids 1..S in id order, each value emitted as
#   its 40-char lowercase hex -- the SAME reference semurg_kv computes independently. A run counts only
#   if its ANSWER equals the board reference (equal-answer gate). Verified: S=4096 -> 48cd91f5...
#
# FAIRNESS (drive the incumbent at max concurrency on its node): redis is started with io-threads = all
# cores (io-threads-do-reads yes), persistence off (pure in-RAM KV, disk is not on the GET path so the
# 2-disk fairness clause does not apply to Redis), and driven by redis-benchmark at -c many concurrent
# clients so every core is exercised. The lane reports io_threads + clients + HIT_RATIO so the drive is
# transparent and never a single-thread/single-client under-drive (which the founder forbids as unfair).
#
# 3 CYCLES (cold -> warm -> warm2) like every board lane: GET_RPS_C1/2/3 + BATCH_RPS_C1/2/3 ride the
# line self-descriptively; ROWS_PER_S = the warm point-GET/s (C3), the apples-to-apples KV metric that
# run_all_domains.sh reads for the q_ms column (as rows_per_s=..). LOAD_MS -> the load column, ANSWER ->
# the equal-answer gate.
#
# Emits (parsed by run_all_domains.sh -> ok-reference / ok-matched against the kv workload reference):
#   LANE=redis STATUS=ok LOAD_MS=.. GET_RPS_C1/2/3=.. BATCH_RPS_C1/2/3=.. ROWS_PER_S=<warm GET/s> \
#     ANSWER=<hash> keys=.. batch=.. io_threads=.. clients=.. HIT_RATIO=.. MODE=in-ram
# ROBUST: docker/image/engine unavailable -> a clean SKIP line with the exact fix (never a crash, never
# a fake number). NO `set -e` -- a lane must isolate its own failure to one line, never abort the board.
set -uo pipefail
here="$(cd "$(dirname "$0")" && pwd)"; . "$here/_common.sh" 2>/dev/null || true

IMG="${REDIS_IMAGE:-redis:7.2}"
KEYS="${KV_KEYS:-1000000}"                 # total keyspace size (ids 0..KEYS-1), matches semurg_kv 1M
ASAMPLE="${KV_ANSWER_SAMPLE:-4096}"        # equal-answer over ids 1..ASAMPLE (matches semurg_kv)
GSAMPLE="${KV_GET_SAMPLE:-200000}"         # GET/MGET random keyspace range (matches semurg_kv)
IO_THREADS="${REDIS_IO_THREADS:-$(nproc 2>/dev/null || echo 4)}"   # io-threads = all cores (fairness)
CLIENTS="${REDIS_CLIENTS:-64}"             # concurrent benchmark clients (saturate the io-threads)
REQ="${REDIS_REQUESTS:-1000000}"           # requests per benchmark cycle
BATCH="${REDIS_BATCH:-100}"                # keys per MGET (batch GET)
CHUNK="${REDIS_LOAD_CHUNK:-20000}"         # keys loaded per Lua EVAL (keeps each script well under the busy limit)
C="arena_redis_$$"

# clamp the answer/keyspace samples to the loaded keyspace so we never read a key that was not loaded.
[ "$ASAMPLE" -lt "$KEYS" ] || ASAMPLE=$((KEYS-1))
[ "$GSAMPLE" -le "$KEYS" ] || GSAMPLE="$KEYS"

# ---- docker availability: classify ONCE, SKIP with the actionable fix (never a cryptic error) --------
st="$(arena_docker_status 2>/dev/null || echo missing)"
[ "$st" = ok ] || { echo "LANE=redis STATUS=skip REASON=docker-$st($(arena_docker_fix "$st"))"; exit 0; }

docker rm -f "$C" >/dev/null 2>&1 || true
trap 'docker rm -f "$C" >/dev/null 2>&1 || true' EXIT INT TERM

# ---- stand up redis:7.2 at max concurrency (io-threads = all cores, persistence off = pure in-RAM KV)
if ! docker run -d --name "$C" --label "$ARENA_LABEL=1" "$IMG" \
      redis-server --io-threads "$IO_THREADS" --io-threads-do-reads yes \
      --save "" --appendonly no --protected-mode no >/tmp/redis_$$.err 2>&1; then
  echo "LANE=redis STATUS=skip REASON=image-start-failed([$(tr -d '\n' </tmp/redis_$$.err | tail -c 90)])fix=[docker pull $IMG]"
  rm -f /tmp/redis_$$.err; exit 0
fi
rm -f /tmp/redis_$$.err
alive(){ docker ps -q --no-trunc | grep -q "$(docker inspect -f '{{.Id}}' "$C" 2>/dev/null)"; }
rcli(){ docker exec -i "$C" redis-cli "$@"; }

ready=0
for i in $(seq 1 60); do
  [ "$(rcli ping 2>/dev/null)" = PONG ] && { ready=1; break; }
  alive || break; sleep 1
done
[ "$ready" = 1 ] || { echo "LANE=redis STATUS=dnf REASON=engine-not-ready-in-60s(logs: docker logs $C)"; exit 0; }

# ---- LOAD: value(id) = 20-byte big-endian id, built SERVER-SIDE in Lua (no binary crosses the shell) -
# chunked so no single write-script blocks the server past the busy limit. Keys k:%012d(id), ids 0..KEYS-1.
LOAD_LUA='local lo=tonumber(ARGV[1]) local hi=tonumber(ARGV[2])
for id=lo,hi do
  local b={} local x=id
  for k=20,1,-1 do b[k]=string.char(x%256) x=math.floor(x/256) end
  redis.call("SET","k:"..string.format("%012d",id),table.concat(b))
end
return hi-lo+1'
t_load=$(now_ns)
loaded=1
id=0
while [ "$id" -lt "$KEYS" ]; do
  hi=$((id+CHUNK-1)); [ "$hi" -ge "$KEYS" ] && hi=$((KEYS-1))
  if ! rcli EVAL "$LOAD_LUA" 0 "$id" "$hi" >/dev/null 2>&1; then loaded=0; break; fi
  id=$((hi+1))
done
[ "$loaded" = 1 ] || { echo "LANE=redis STATUS=dnf REASON=load-failed(EVAL SET error; logs: docker logs $C)"; exit 0; }
load_ms=$(( ( $(now_ns) - t_load ) / 1000000 ))

# ---- benchmark helpers -------------------------------------------------------------------------------
# parse redis-benchmark -q output (has CR progress + a final "N requests per second" summary line).
parse_rps(){ printf '%s' "$1" | tr '\r' '\n' | grep -i 'requests per second' | tail -1 \
             | sed -E 's/.* ([0-9]+(\.[0-9]+)?) requests per second.*/\1/'; }
rnd(){ printf '%.0f' "${1:-0}" 2>/dev/null || echo 0; }

# GET: point-get, 1 key/request, -c many clients. keys/sec == requests/sec.
bench_get(){ docker exec "$C" redis-benchmark -q -r "$GSAMPLE" -n "$REQ" -c "$CLIENTS" GET 'k:__rand_int__' 2>/dev/null; }
# MGET: batch-get, BATCH keys/request. keys/sec == requests/sec * BATCH.
MGET_ARGS=""; for _i in $(seq 1 "$BATCH"); do MGET_ARGS="$MGET_ARGS k:__rand_int__"; done
bench_mget(){ docker exec "$C" redis-benchmark -q -r "$GSAMPLE" -n "$REQ" -c "$CLIENTS" MGET $MGET_ARGS 2>/dev/null; }
hits(){ rcli info stats 2>/dev/null | tr -d '\r' | sed -n 's/^keyspace_hits:\([0-9]*\).*/\1/p'; }
misses(){ rcli info stats 2>/dev/null | tr -d '\r' | sed -n 's/^keyspace_misses:\([0-9]*\).*/\1/p'; }

# ---- 3 CYCLES: point GET then batch GET each cycle; hit ratio measured across the GET benchmarks -----
declare -a GRPS BRPS
h0="$(hits)"; m0="$(misses)"
for cyc in 1 2 3; do
  g="$(parse_rps "$(bench_get)")";  GRPS[$cyc]="$(rnd "$g")"
  bmr="$(parse_rps "$(bench_mget)")"
  # keys/sec for MGET = requests/sec * BATCH
  BRPS[$cyc]="$(rnd "$(awk -v r="${bmr:-0}" -v b="$BATCH" 'BEGIN{printf "%.0f", r*b}')")"
done
h1="$(hits)"; m1="$(misses)"
dh=$(( ${h1:-0} - ${h0:-0} )); dm=$(( ${m1:-0} - ${m0:-0} ))
hit_ratio="$(awk -v h="$dh" -v m="$dm" 'BEGIN{t=h+m; if(t<=0){print "na"}else{printf "%.4f", h/t}}')"

# a benchmark that produced no rps is an honest DNF (engine up but redis-benchmark failed), not a 0.
if [ -z "${GRPS[3]:-}" ] || [ "${GRPS[3]:-0}" = 0 ]; then
  echo "LANE=redis STATUS=dnf REASON=redis-benchmark-produced-no-throughput(logs: docker logs $C)"; exit 0
fi

# ---- EQUAL-ANSWER: sha256 over the values of ids 1..ASAMPLE, each as 40-char lowercase hex ----------
# built server-side (hex-encode in Lua) then hashed in the shell -- byte-identical to semurg_kv + the awk
# reference. self-check: the value of id 1 must hex to 40 chars ending in ...01 (never a faked answer).
AHEX_LUA='local function tohex(s) return (s:gsub(".", function(c) return string.format("%02x", string.byte(c)) end)) end
local n=tonumber(ARGV[1]) local p={}
for id=1,n do local v=redis.call("GET","k:"..string.format("%012d",id)) p[#p+1]=(v and tohex(v)) or "" end
return table.concat(p)'
AHEX="$(rcli EVAL "$AHEX_LUA" 0 "$ASAMPLE" 2>/dev/null)"
want_len=$(( ASAMPLE * 40 ))
if [ "${#AHEX}" -ne "$want_len" ]; then
  echo "LANE=redis STATUS=dnf REASON=answer-read-incomplete(got ${#AHEX} hex chars, want $want_len; a key was missing)"; exit 0
fi
ANSWER="$(printf '%s' "$AHEX" | sha256sum | cut -c1-32)"
self_id1="${AHEX:0:40}"   # first value's hex == printf '%040x' 1
self_ok=no; [ "$self_id1" = "0000000000000000000000000000000000000001" ] && self_ok=yes

echo "LANE=redis STATUS=ok LOAD_MS=$load_ms \
GET_RPS_C1=${GRPS[1]} GET_RPS_C2=${GRPS[2]} GET_RPS_C3=${GRPS[3]} \
BATCH_RPS_C1=${BRPS[1]} BATCH_RPS_C2=${BRPS[2]} BATCH_RPS_C3=${BRPS[3]} \
ROWS_PER_S=${GRPS[3]} ANSWER=$ANSWER keys=$KEYS batch=$BATCH io_threads=$IO_THREADS clients=$CLIENTS \
HIT_RATIO=$hit_ratio SELF=$self_ok MODE=in-ram"
exit 0
