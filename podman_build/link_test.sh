#!/usr/bin/env bash
# Run the test suite inside the rdmalib container, linking against the
# real RDMA libraries (libibverbs, librdmacm).
#
# No RDMA hardware is needed: the tests compile, link, and load the
# real libraries, but never exercise the verbs. This is the
# link-verification counterpart of the host development loop, where
# only `cargo check` and `cargo clippy` run (the host has no RDMA
# stack).
#
# Usage: podman_build/link_test.sh [cargo test arguments]

set -euo pipefail

image=rdmalib-dev
root="$(cd "$(dirname "$0")/.." && pwd)"

# Build or reuse the cached image.
podman build -qt "$image" "$root/podman_build" >/dev/null

# Test artifacts stay inside the container, so the host target/ is untouched.
exec podman run --rm \
    -v "$root":/workspace \
    -w /workspace \
    -e CARGO_TARGET_DIR=/tmp/target \
    "$image" \
    cargo test "$@"
