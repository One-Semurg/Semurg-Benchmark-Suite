#!/usr/bin/env bash
# The Semurg benchmark, one command  --  https://one.semurg.io/bench
#   curl -fsSL https://one.semurg.io/bench | sudo bash                  the domains one.semurg.io/benchmark publishes
#   curl -fsSL https://one.semurg.io/bench | sudo bash -s -- --graph    only the domains you name, in that order
# Downloads the benchmark runner, checks it against the checksum written into this script when both were published,
# unpacks it under /opt/semurg-bench and runs `semurg-arena start`: the box check, the run, the table. Nothing runs from
# a byte that did not match. Read it before you run it:  curl -fsSL https://one.semurg.io/bench | less
set -euo pipefail
BASE="https://one.semurg.io/dl"
NAME="semurg-bench-runner"
EXPECT="85fb5075b051ae9bf27c5f3f452521a972eba39ac4ff4cb5615d24c3ba384e1b"
VERSION="r11.142.20261030.e037138"
bold(){ printf '\033[1m%s\033[0m\n' "$*"; }
die(){  printf '\n\033[31mThe Semurg benchmark did not start:\033[0m %s\n' "$*" >&2; exit 1; }
[ "$(id -u)" = 0 ] || die "run it as root:  curl -fsSL https://one.semurg.io/bench | sudo bash"
for c in curl tar sha256sum; do command -v "$c" >/dev/null 2>&1 || die "missing '$c'. Install it with:  apt-get install -y curl tar coreutils"; done
DEST="/opt/semurg-bench/${EXPECT:0:12}"
if [ -x "$DEST/$NAME/bin/semurg-arena" ]; then
  bold "==> The benchmark runner $VERSION is already here: $DEST (checked when it was unpacked)"
else
  bold "==> Downloading the Semurg benchmark runner $VERSION"
  TMP="$(mktemp -d)" || exit 2; [ -d "$TMP" ] && [ "$TMP" != / ] || exit 2; trap 'rm -rf "$TMP"' EXIT
  curl -fSL --retry 3 --retry-delay 2 -o "$TMP/$NAME.tar.gz" "$BASE/$NAME.tar.gz" || die "could not download $BASE/$NAME.tar.gz"
  bold "==> Checking it against the published checksum"
  printf '%s  %s\n' "$EXPECT" "$TMP/$NAME.tar.gz" | sha256sum -c - >/dev/null 2>&1 \
    || die "the download does not match the checksum this script was published with ($EXPECT). A new release may have been published a moment ago: run the command again"
  printf '    OK  %s\n' "$EXPECT"
  rm -rf "$DEST.part" && mkdir -p "$DEST.part" && tar xzf "$TMP/$NAME.tar.gz" -C "$DEST.part" \
    && rm -rf "$DEST" && mv "$DEST.part" "$DEST" || die "could not unpack it into $DEST"
fi
cd "$DEST/$NAME"
bold "==> Running: ./bin/semurg-arena start $*"
exec ./bin/semurg-arena start "$@" < /dev/null
