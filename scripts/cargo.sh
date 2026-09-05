#!/usr/bin/env bash
# Run cargo inside the official rust image — no host toolchain needed.
#   scripts/cargo.sh build
#   scripts/cargo.sh test
#   scripts/cargo.sh clippy -- -D warnings
#
# The cargo registry and the target dir live in named Docker volumes so builds
# stay fast on WSL /mnt/* mounts. `RUST_IMAGE` overrides the image tag.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BASE="${RUST_IMAGE:-rust:1-slim-bookworm}"
IMAGE="bookend-dev:local"

# Build the dev image (base + clippy + rustfmt) once; rebuild with REBUILD_DEV=1.
if [[ -n "${REBUILD_DEV:-}" ]] || ! docker image inspect "$IMAGE" >/dev/null 2>&1; then
  docker build -q --build-arg "RUST_IMAGE=$BASE" -t "$IMAGE" -f "$ROOT/scripts/Dockerfile.dev" "$ROOT/scripts" >/dev/null
fi

tty_flag=()
[[ -t 0 && -t 1 ]] && tty_flag=(-it)

exec docker run --rm "${tty_flag[@]}" \
  -v "$ROOT:/work" \
  -v bookend-cargo-registry:/usr/local/cargo/registry \
  -v bookend-target:/work/target \
  -w /work \
  -e CARGO_TERM_COLOR=always \
  -e RUST_LOG \
  -e RUST_BACKTRACE=1 \
  --network host \
  "$IMAGE" cargo "$@"
