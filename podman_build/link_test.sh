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
# Cluster tests (tests/) are the exception: deploying them needs the
# host's ssh config/keys for the node hostnames, which the container
# does not have. So when a deploy trigger flag (--ignored or
# --include-ignored, the same flags scripts/test_runner.py looks for)
# is passed, the container only BUILDS the test binaries
# (cargo test --no-run, artifacts persisted in target-cluster/), and
# scripts/test_runner.py is then run on the host, which rsyncs them to
# the nodes and launches them there.
#
# Usage: podman_build/link_test.sh [cargo test arguments]
#   ./podman_build/link_test.sh                                  # unit tests, in the container
#   ./podman_build/link_test.sh verbs_struct                     # one unit test
#   ./podman_build/link_test.sh --test read_test -- --ignored    # cluster test, deployed from the host

set -euo pipefail

image=rdmalib-dev
root="$(cd "$(dirname "$0")/.." && pwd)"

# Deploy trigger flags — keep in sync with DEPLOY_FLAGS in scripts/test_runner.py.
deploy=0
for arg in "$@"; do
  case "$arg" in
    --ignored|--include-ignored) deploy=1 ;;
  esac
done

# Build or reuse the cached image.
podman build -qt "$image" "$root/podman_build" >/dev/null

if [[ "$deploy" -eq 0 ]]; then
  # Test artifacts stay inside the container, so the host target/ is untouched.
  exec podman run --rm \
      -v "$root":/workspace \
      -w /workspace \
      -e CARGO_TARGET_DIR=/tmp/target \
      "$image" \
      cargo test "$@"
fi

# --- Cluster-test mode: build in the container, deploy from the host. ---

# Cargo arguments (before `--`) vs. test-harness arguments (after, forwarded
# to the remote processes).
build_args=()
runner_args=()
for_tests=0
for arg in "$@"; do
  if [[ "$for_tests" -eq 0 && "$arg" == "--" ]]; then
    for_tests=1
  elif [[ "$for_tests" -eq 0 ]]; then
    build_args+=("$arg")
  else
    runner_args+=("$arg")
  fi
done

echo "[link_test] cluster-test mode: building in the container, deploying from the host" >&2
target_dir="$root/target-cluster"
podman run --rm \
    -v "$root":/workspace \
    -w /workspace \
    -e CARGO_TARGET_DIR=/workspace/target-cluster \
    "$image" \
    cargo test --no-run "${build_args[@]}"

# Which test binaries to deploy: those selected with --test, or all the
# cluster tests listed in tests/_test_config.json.
test_names=()
grab=0
for arg in "${build_args[@]}"; do
  if [[ "$grab" -eq 1 ]]; then
    test_names+=("$arg")
    grab=0
  else
    case "$arg" in
      --test) grab=1 ;;
      --test=*) test_names+=("${arg#--test=}") ;;
    esac
  fi
done
if [[ "${#test_names[@]}" -eq 0 ]]; then
  mapfile -t test_names < <(python3 -c \
    "import json; print('\n'.join(json.load(open('$root/tests/_test_config.json'))['tests']))")
fi
if [[ "${#test_names[@]}" -eq 0 ]]; then
  echo "[link_test] ERROR: no cluster tests selected or listed in tests/_test_config.json" >&2
  exit 1
fi

# Newest built executable for a test name (cargo appends a hash to the file name).
newest_binary() {
  local best="" best_t=-1 t
  for f in "$target_dir/debug/deps/$1"-*; do
    [[ -f "$f" && -x "$f" ]] || continue
    t="$(stat -c '%Y' "$f")"
    if (( t > best_t )); then
      best_t="$t"
      best="$f"
    fi
  done
  printf '%s' "$best"
}

status=0
for name in "${test_names[@]}"; do
  bin="$(newest_binary "$name")"
  if [[ -z "$bin" ]]; then
    echo "[link_test] ERROR: no built binary for test '$name' under $target_dir/debug/deps" >&2
    status=1
    continue
  fi
  echo "[link_test] deploying '$name': $bin"
  python3 "$root/scripts/test_runner.py" "$bin" "${runner_args[@]}" || status=1
done
exit "$status"
