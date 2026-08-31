#!/usr/bin/env bash
# Semurg lane: DOWNLOAD + RUN Semurg for a domain benchmark. The engine source is NOT in this repo; Semurg is
# fetched from the published release channel (the free first node is always free for testing/development).
#
#   SEMURG_MODE=docker     (default) run Semurg in a Docker container  -> the RELATIVE config (both sides in
#                                     Docker, single node; the apples-to-apples multiple).
#   SEMURG_MODE=baremetal            install + run Semurg on the host   -> the ABSOLUTE config (whole cluster,
#                                     max concurrency). Only when the OS+hardware support it (Linux kernel >= 5.1
#                                     for io_uring, a real block device for O_DIRECT); otherwise falls back to docker.
#
# Overridable (pin these to exact, checksum-verified artifacts in real runs):
#   SEMURG_IMAGE   - the published Semurg benchmark Docker image (pin by @sha256:... digest)
#   SEMURG_BIN     - path to an already-installed Semurg release binary (skips download)
#   SEMURG_INSTALLER, SEMURG_INSTALLER_SHA256 - bare-metal installer URL + its sha256 (verified before running)
set -euo pipefail
DOMAIN="${1:?usage: semurg_lane.sh <domain>}"
MODE="${SEMURG_MODE:-docker}"
SEMURG_IMAGE="${SEMURG_IMAGE:-}"
SEMURG_BIN="${SEMURG_BIN:-}"

hw_supports_baremetal() {
  [ "$(uname -s)" = "Linux" ] || return 1
  local rel kmaj kmin; rel="$(uname -r)"; kmaj="${rel%%.*}"; rel="${rel#*.}"; kmin="${rel%%.*}"
  if [ "${kmaj:-0}" -gt 5 ]; then return 0; fi
  [ "${kmaj:-0}" -eq 5 ] && [ "${kmin:-0}" -ge 1 ] && return 0
  return 1
}

run_docker() {
  if [ -z "$SEMURG_IMAGE" ]; then
    echo "SEMURG lane: SEMURG_IMAGE not set. Install/pull the published Semurg benchmark image and set"
    echo "  SEMURG_IMAGE=<registry>/semurg@sha256:<digest>   (see lanes/semurg/README.md)."
    echo "SEMURG_RESULT domain=$DOMAIN status=semurg-not-available"
    return 0
  fi
  echo "SEMURG lane: docker (relative config)"
  docker pull "$SEMURG_IMAGE" >/dev/null
  # FAIRNESS: pin Semurg to the SAME CPU/memory limits the incumbent lane uses (BENCH_CPUS / BENCH_MEM), so the
  # relative multiple is a like-for-like single-node comparison, never uncapped-Semurg vs capped-incumbent.
  docker run --rm \
    ${BENCH_CPUS:+--cpuset-cpus="$BENCH_CPUS"} ${BENCH_MEM:+--memory="$BENCH_MEM"} \
    -e DOMAIN="$DOMAIN" "$SEMURG_IMAGE" bench "$DOMAIN"
}

verify_and_install() {  # download the installer, verify sha256, then run it (no blind curl|sh)
  [ -n "${SEMURG_INSTALLER:-}" ] && [ -n "${SEMURG_INSTALLER_SHA256:-}" ] || {
    echo "SEMURG lane: set SEMURG_INSTALLER + SEMURG_INSTALLER_SHA256 (or SEMURG_BIN) to install on bare metal."
    echo "SEMURG_RESULT domain=$DOMAIN status=semurg-not-available"; return 1; }
  local tmp; tmp="$(mktemp)"
  curl -fsSL "$SEMURG_INSTALLER" -o "$tmp"
  echo "${SEMURG_INSTALLER_SHA256}  ${tmp}" | sha256sum -c - || { echo "SEMURG lane: installer checksum MISMATCH - aborting"; rm -f "$tmp"; return 1; }
  sh "$tmp"; rm -f "$tmp"
}

run_baremetal() {
  if ! hw_supports_baremetal; then
    echo "SEMURG lane: this OS/kernel does not support the bare-metal belt (need Linux kernel >= 5.1 for io_uring)."
    echo "SEMURG lane: falling back to docker."
    SEMURG_MODE=docker run_docker; return
  fi
  echo "SEMURG lane: bare-metal (absolute config, whole-cluster max concurrency)"
  local bin="${SEMURG_BIN:-/opt/semurg/bin/semurg}"
  [ -x "$bin" ] || { verify_and_install || return 0; bin="${SEMURG_BIN:-/opt/semurg/bin/semurg}"; }
  [ -x "$bin" ] || { echo "SEMURG_RESULT domain=$DOMAIN status=semurg-not-installed"; return 0; }
  "$bin" bench "$DOMAIN"
}

case "$MODE" in
  docker)    run_docker ;;
  baremetal) run_baremetal ;;
  *) echo "SEMURG lane: unknown SEMURG_MODE=$MODE (docker|baremetal)"; exit 2 ;;
esac
