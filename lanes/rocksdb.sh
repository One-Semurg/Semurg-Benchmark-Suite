#!/usr/bin/env bash
# RocksDB KEY-VALUE lane (domain 6). Embedded RocksDB (librocksdb, Apache-2.0/GPLv2 dual, public-OK),
# compiled into a tiny point-GET bench binary and driven at MAX CONCURRENCY over the SAME deterministic
# keyset as semurg_kv/redis, with an EQUAL-ANSWER parity gate. No DeWitt engine touched; fully publishable.
#
# WHY DOCKER FOR AN "EMBEDDED" ENGINE: RocksDB has no maintained official image and the host carries no
# librocksdb-dev. Rather than pollute the box with system packages, this lane BUILDS a small image ONCE
# (base python:3.12-slim already local, `apt-get install librocksdb-dev g++`, compile the embedded bench)
# and caches it as arena_rocksdb:v1. The engine is still EMBEDDED RocksDB (the bench links librocksdb and
# opens a local DB), just built + run inside an ephemeral container so the host stays clean. The image is
# built only on the FIRST run (needs network for apt); every later run reuses the cached image and is
# fully offline. If docker is unavailable, or the image is absent AND the build fails (offline), the lane
# emits a clean SKIP with the exact fix -- it NEVER fakes a number and NEVER crashes the board.
#
# DATASET (identical to semurg_kv + redis, so the answer is cross-engine): key = 8-byte big-endian id,
# value = 20-byte big-endian id (== printf '%040x' id in hex), for ids 1..KV_KEYS. Point-GET over ids
# 1..KV_GET_SAMPLE, 3 cycles (cold->warm->warm2). ANSWER = first 32 hex of sha256 over the concatenated
# lowercase-hex of the VALUES READ BACK for ids 1..KV_ANSWER_SAMPLE -- computed from what RocksDB actually
# returns (a broken store cannot pass), and byte-identical crypto to semurg_kv (:crypto.hash(:sha256,..)).
#
# FAIRNESS (founder benchmark-fairness law): the incumbent is driven at MAX CONCURRENCY on its node --
# all cores (KV_THREADS = nproc, parallel load + parallel point-GET) and BOTH NVMe pipes (RocksDB
# db_paths spread SSTs across /data0 + /data1 when both are present) -- never single-thread, never
# single-disk. THREADS + PIPES ride the machine line so the drive level is transparent.
#
# Emits (parsed by run_all_domains.sh -> ROWS_PER_S = the point-GET/s metric, ANSWER = the equal-answer):
#   LANE=rocksdb STATUS=ok LOAD_MS=.. GET_RPS_C1/2/3=.. ROWS_PER_S=<warm point-GET/s> ANSWER=<hash> \
#     keys=.. THREADS=.. PIPES=..
set -uo pipefail
here="$(cd "$(dirname "$0")" && pwd)"; . "$here/_common.sh" 2>/dev/null || true

IMG_TAG="${ROCKSDB_IMAGE:-arena_rocksdb:v1}"
KV_KEYS="${KV_KEYS:-1000000}"
KV_GET_SAMPLE="${KV_GET_SAMPLE:-200000}"
KV_ANSWER_SAMPLE="${KV_ANSWER_SAMPLE:-4096}"
KV_THREADS="${KV_THREADS:-$(nproc 2>/dev/null || echo 4)}"
TO="${ROCKSDB_TIMEOUT:-600}"
C="arena_rocksdb_$$"
CTX=""; D0=""; D1=""
cleanup(){ docker rm -f "$C" >/dev/null 2>&1 || true; [ -n "$CTX" ] && rm -rf "$CTX" 2>/dev/null; [ -n "$D0" ] && rm -rf "$D0" 2>/dev/null; [ -n "$D1" ] && rm -rf "$D1" 2>/dev/null; return 0; }
trap cleanup EXIT INT TERM

# ---- 1. docker availability (never crash: SKIP with the ONE actionable fix) --------------------------
st="$(arena_docker_status)"
[ "$st" = ok ] || { echo "LANE=rocksdb STATUS=skip REASON=docker-$st($(arena_docker_fix "$st"))"; exit 0; }

# ---- 2. ensure the embedded-RocksDB bench image (build once from the context embedded below) ---------
if ! docker image inspect "$IMG_TAG" >/dev/null 2>&1; then
  CTX="$(mktemp -d)"
  cat > "$CTX/rocksdb_kv_bench.cpp" <<'CPP'
// Embedded RocksDB KV point-get bench for the Semurg public benchmark suite (KEY-VALUE domain).
// Deterministic, cross-engine equal-answer: key = 8-byte big-endian id, value = 20-byte big-endian id
// (== printf %040x id in hex), for ids 1..KEYS. Point-GET over ids 1..GET_SAMPLE, 3 cycles, driven at
// max concurrency (all cores). ANSWER material = concatenated lowercase hex of the values read back for
// ids 1..ANSWER_SAMPLE, written to ANSWER_FILE; the entrypoint sha256s it -> matches semurg_kv exactly.
#include <rocksdb/db.h>
#include <rocksdb/options.h>
#include <rocksdb/write_batch.h>
#include <rocksdb/slice.h>
#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <string>
#include <thread>
#include <vector>
using namespace std::chrono;

static uint64_t env_u64(const char* k, uint64_t d){ const char* v=getenv(k); if(!v||!*v) return d; return strtoull(v,nullptr,10); }
static inline void put_be(char* buf,int nbytes,uint64_t v){ for(int i=0;i<nbytes;i++) buf[i]=0; for(int i=0;i<8&&i<nbytes;i++) buf[nbytes-1-i]=(char)((v>>(8*i))&0xff); }

int main(){
  const char* p0=getenv("DB_PATH0"); const char* p1=getenv("DB_PATH1");
  const char* answer_file=getenv("ANSWER_FILE");
  uint64_t keys=env_u64("KV_KEYS",1000000ULL);
  uint64_t get_sample=std::min(env_u64("KV_GET_SAMPLE",200000ULL),keys);
  uint64_t answer_sample=std::min(env_u64("KV_ANSWER_SAMPLE",4096ULL),keys);
  uint64_t threads=env_u64("KV_THREADS",(uint64_t)std::max(1u,std::thread::hardware_concurrency()));
  if(threads<1) threads=1;
  std::string dbname = (p0&&*p0)? std::string(p0) : std::string("/tmp/rdb");

  rocksdb::Options options; options.create_if_missing=true; options.error_if_exists=false;
  options.IncreaseParallelism((int)threads); options.OptimizeLevelStyleCompaction();
  int pipes=1;
  if(p1&&*p1){ uint64_t cap=(uint64_t)200*1024*1024*1024ULL; options.db_paths.clear();
    options.db_paths.emplace_back(dbname,cap); options.db_paths.emplace_back(std::string(p1),cap); pipes=2; }
  rocksdb::DB* db=nullptr; auto s=rocksdb::DB::Open(options,dbname,&db);
  if(!s.ok()){ printf("ROCKSDB_ERROR=open-failed(%s)\n",s.ToString().c_str()); return 2; }

  rocksdb::WriteOptions wo; wo.disableWAL=true; wo.sync=false;
  auto t0=steady_clock::now();
  { std::vector<std::thread> ths; uint64_t per=(keys+threads-1)/threads;
    for(uint64_t t=0;t<threads;t++){ ths.emplace_back([&,t](){
      uint64_t lo=t*per+1; uint64_t hi=std::min(keys,(t+1)*per); if(lo>hi) return;
      rocksdb::WriteBatch wb; char k[8],v[20]; uint64_t cnt=0;
      for(uint64_t id=lo;id<=hi;id++){ put_be(k,8,id); put_be(v,20,id);
        wb.Put(rocksdb::Slice(k,8),rocksdb::Slice(v,20));
        if(++cnt%1000==0){ db->Write(wo,&wb); wb.Clear(); } }
      if(wb.Count()>0) db->Write(wo,&wb); }); }
    for(auto&th:ths) th.join(); }
  db->Flush(rocksdb::FlushOptions());
  auto load_ms=duration_cast<milliseconds>(steady_clock::now()-t0).count();
  printf("ROCKSDB_LOAD_MS=%lld\n",(long long)load_ms);

  rocksdb::ReadOptions ro;
  for(int cyc=1;cyc<=3;cyc++){ std::atomic<uint64_t> done{0}; auto g0=steady_clock::now();
    std::vector<std::thread> ths; uint64_t per=(get_sample+threads-1)/threads;
    for(uint64_t t=0;t<threads;t++){ ths.emplace_back([&,t](){
      uint64_t lo=t*per+1; uint64_t hi=std::min(get_sample,(t+1)*per); if(lo>hi) return;
      std::string val; char k[8]; uint64_t ok=0;
      for(uint64_t id=lo;id<=hi;id++){ put_be(k,8,id); if(db->Get(ro,rocksdb::Slice(k,8),&val).ok()) ok++; }
      done+=ok; }); }
    for(auto&th:ths) th.join();
    double secs=duration_cast<microseconds>(steady_clock::now()-g0).count()/1e6;
    unsigned long long rps = secs>0 ? (unsigned long long)(get_sample/secs) : 0ULL;
    printf("ROCKSDB_GET_RPS_C%d=%llu\n",cyc,rps);
    if(done.load()!=get_sample){ printf("ROCKSDB_ERROR=get-miss(got=%llu want=%llu)\n",(unsigned long long)done.load(),(unsigned long long)get_sample); return 3; } }

  { FILE* f=fopen(answer_file,"wb"); if(!f){ printf("ROCKSDB_ERROR=answer-file-open\n"); return 4; }
    std::string val; char k[8]; static const char hx[]="0123456789abcdef"; char line[40];
    for(uint64_t id=1;id<=answer_sample;id++){ put_be(k,8,id);
      auto st=db->Get(ro,rocksdb::Slice(k,8),&val);
      if(!st.ok()||val.size()!=20){ fclose(f); printf("ROCKSDB_ERROR=answer-get-fail(id=%llu)\n",(unsigned long long)id); return 5; }
      for(int b=0;b<20;b++){ unsigned char c=(unsigned char)val[b]; line[b*2]=hx[c>>4]; line[b*2+1]=hx[c&0xf]; }
      fwrite(line,1,40,f); }
    fclose(f); }
  printf("ROCKSDB_KEYS=%llu\n",(unsigned long long)keys);
  printf("ROCKSDB_THREADS=%llu\n",(unsigned long long)threads);
  printf("ROCKSDB_PIPES=%d\n",pipes);
  printf("ROCKSDB_DONE=1\n");
  db->Close(); delete db; return 0;
}
CPP
  cat > "$CTX/run_bench.sh" <<'SH'
#!/bin/sh
# container entrypoint: prep fresh db dir(s), run the embedded RocksDB bench, sha256 the answer material
# (identical crypto to semurg_kv) and print the equal-answer token. All output on stdout for the lane.
set -u
: "${DB_PATH0:=/db0}"
rm -rf "$DB_PATH0"/* 2>/dev/null || true; mkdir -p "$DB_PATH0" 2>/dev/null || true
if [ -n "${DB_PATH1:-}" ]; then rm -rf "$DB_PATH1"/* 2>/dev/null || true; mkdir -p "$DB_PATH1" 2>/dev/null || true; fi
ANSWER_FILE="$DB_PATH0/answer_hex.txt"; export ANSWER_FILE
/bench/rocksdb_kv_bench; rc=$?
[ "$rc" = 0 ] || exit "$rc"
ans=$(sha256sum "$ANSWER_FILE" | cut -c1-32)
echo "ROCKSDB_ANSWER=$ans"
rm -f "$ANSWER_FILE" 2>/dev/null || true
SH
  cat > "$CTX/Dockerfile" <<'DOCKER'
FROM python:3.12-slim
RUN apt-get update && apt-get install -y --no-install-recommends \
      g++ librocksdb-dev libsnappy-dev liblz4-dev libzstd-dev zlib1g-dev libbz2-dev \
    && rm -rf /var/lib/apt/lists/*
COPY rocksdb_kv_bench.cpp /bench/rocksdb_kv_bench.cpp
COPY run_bench.sh /bench/run_bench.sh
RUN g++ -O3 -std=c++17 /bench/rocksdb_kv_bench.cpp -o /bench/rocksdb_kv_bench \
      -lrocksdb -lpthread -ldl \
    && chmod +x /bench/run_bench.sh /bench/rocksdb_kv_bench
ENTRYPOINT ["/bench/run_bench.sh"]
DOCKER
  if ! timeout "$TO" docker build -t "$IMG_TAG" --label "$ARENA_LABEL=1" "$CTX" >/tmp/rocksdb_build_$$.log 2>&1; then
    det="$(tail -c 110 /tmp/rocksdb_build_$$.log 2>/dev/null | tr '\n' ' ')"
    echo "LANE=rocksdb STATUS=skip REASON=image-build-failed(first-build-needs-network-for-apt-librocksdb-dev; offline=clean-skip; detail=[$det])"
    rm -f /tmp/rocksdb_build_$$.log; exit 0
  fi
  rm -f /tmp/rocksdb_build_$$.log
  rm -rf "$CTX"; CTX=""
fi

# ---- 3. data dirs: fan RocksDB across BOTH NVMe pipes (db_paths) for a fair multi-disk drive ---------
roots_env="${KV_STRIPE_ROOTS:-${SEMURG_STRIPE_ROOTS:-}}"
cand=()
if [ -n "$roots_env" ]; then IFS=', ' read -r -a raw <<<"$roots_env"; else raw=(/data0 /data1); fi
for r in "${raw[@]}"; do [ -n "$r" ] && [ -d "$r" ] && [ -w "$r" ] && cand+=("$r"); done
[ "${#cand[@]}" -ge 1 ] || cand=("${ARENA_DATA:-/tmp}")
D0="${cand[0]}/arena_rocksdb_${$}_0"; mkdir -p "$D0" 2>/dev/null || true
mounts=(-v "$D0":/db0); envs=(-e DB_PATH0=/db0); PIPES=1
if [ "${#cand[@]}" -ge 2 ]; then
  D1="${cand[1]}/arena_rocksdb_${$}_1"; mkdir -p "$D1" 2>/dev/null || true
  mounts+=(-v "$D1":/db1); envs+=(-e DB_PATH1=/db1); PIPES=2
fi

# ---- 4. run the embedded bench in the container, at max concurrency, capture the machine tokens ------
docker rm -f "$C" >/dev/null 2>&1 || true
OUT="$(timeout -k 10 "$TO" docker run --rm --name "$C" --label "$ARENA_LABEL=1" \
  -e KV_KEYS="$KV_KEYS" -e KV_GET_SAMPLE="$KV_GET_SAMPLE" -e KV_ANSWER_SAMPLE="$KV_ANSWER_SAMPLE" -e KV_THREADS="$KV_THREADS" \
  "${envs[@]}" "${mounts[@]}" "$IMG_TAG" 2>&1)"

# ---- 5. parse + emit the ONE machine line (or an honest dnf/skip; never a fake number) ---------------
g(){ sed -n "s/^$1=\([0-9a-f]*\).*/\1/p" <<<"$OUT" | tail -1; }
ans="$(g ROCKSDB_ANSWER)"; load="$(g ROCKSDB_LOAD_MS)"
c1="$(g ROCKSDB_GET_RPS_C1)"; c2="$(g ROCKSDB_GET_RPS_C2)"; c3="$(g ROCKSDB_GET_RPS_C3)"
err="$(sed -n 's/^ROCKSDB_ERROR=\(.*\)/\1/p' <<<"$OUT" | tail -1)"

if [ -n "$ans" ] && [ -n "$c3" ]; then
  echo "LANE=rocksdb STATUS=ok LOAD_MS=${load:-0} GET_RPS_C1=${c1:-0} GET_RPS_C2=${c2:-0} GET_RPS_C3=${c3:-0} \
ROWS_PER_S=${c3:-0} ANSWER=$ans keys=$KV_KEYS THREADS=$KV_THREADS PIPES=$PIPES"
  echo "ROCKSDB_INFO engine=rocksdb(embedded,librocksdb) threads=$KV_THREADS disk_pipes=$PIPES \
load_ms=${load:-0} get_rps_cold=${c1:-0} get_rps_warm=${c3:-0} note=[max-concurrency point-GET, WAL-off parallel load, both-NVMe db_paths]"
elif [ -n "$err" ]; then
  echo "LANE=rocksdb STATUS=dnf REASON=engine-error($err)"
elif printf '%s' "$OUT" | grep -qiE 'killed|out of memory|oom'; then
  echo "LANE=rocksdb STATUS=dnf REASON=oom-killed"
elif [ -z "$OUT" ]; then
  echo "LANE=rocksdb STATUS=dnf REASON=no-output(timeout>${TO}s-or-container-crash; retry:docker run $IMG_TAG)"
else
  echo "LANE=rocksdb STATUS=dnf REASON=no-result([$(printf '%s' "$OUT" | tr '\n' ' ' | tail -c 120)])"
fi
exit 0
