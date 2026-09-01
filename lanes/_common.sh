# sourced by every lane. helpers for timing + answer hashing + ROBUST docker lifecycle.
now_ns(){ date +%s%N; }
ms_since(){ echo $(( ( $(now_ns) - $1 ) / 1000000 )); }
hash_answer(){ printf '%s' "$1" | sha256sum | cut -c1-32; }

# Every arena-managed container carries this label, so entry/exit cleanup can find + reap leftovers
# from an interrupted prior run no matter what pid named them -> idempotent, resumable, never wedged.
ARENA_LABEL="semurg-arena"

# Classify docker availability ONCE, with an actionable fix. Echoes: ok | missing | down | noperm
arena_docker_status(){
  command -v docker >/dev/null 2>&1 || { echo missing; return; }
  if docker info >/dev/null 2>&1; then echo ok; return; fi
  local err; err="$(docker info 2>&1 >/dev/null)"
  case "$err" in
    *permission\ denied*) echo noperm;;
    *"Cannot connect to the Docker daemon"*|*"Is the docker daemon running"*) echo down;;
    *) echo down;;
  esac
}
# status -> the ONE thing to do (never a cryptic docker error)
arena_docker_fix(){
  case "$1" in
    missing) echo "install Docker (https://docs.docker.com/engine/install/), or run the embedded lanes only";;
    down)    echo "start the Docker daemon: sudo systemctl start docker";;
    noperm)  echo "add your user to the docker group: sudo usermod -aG docker \$USER && newgrp docker (or re-run with sudo)";;
    *) echo "";;
  esac
}
# Reap every arena-labelled container (leftovers from an interrupted run). Safe, idempotent, no-op if
# docker is missing/down. Called on run entry AND exit so back-to-back runs never wedge each other.
arena_cleanup_containers(){
  command -v docker >/dev/null 2>&1 || return 0
  docker info >/dev/null 2>&1 || return 0
  local ids; ids="$(docker ps -aq --filter "label=$ARENA_LABEL" 2>/dev/null)"
  [ -n "$ids" ] && docker rm -f $ids >/dev/null 2>&1
  return 0
}
