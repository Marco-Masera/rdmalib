#!/usr/bin/env bash
# Build and run RDMA cluster benchmarks.
#
# Compiles benchmark executables inside the Podman container (which has the real
# libibverbs and librdmacm installed), then deploys and runs them across cluster
# nodes from the host via scripts/bench_runner.py.
#
# Usage:
#   ./podman_build/run_bench.sh [--debug|--release] <benchmark_name> [-- <bench_args...>]
#   ./podman_build/run_bench.sh --list
#   ./podman_build/run_bench.sh --all [--debug|--release] [-- <bench_args...>]
#
# Examples:
#   ./podman_build/run_bench.sh read_latency
#   ./podman_build/run_bench.sh --debug read_latency
#   ./podman_build/run_bench.sh read_latency -- --iters 50000 --size 1024

set -euo pipefail

image=rdmalib-dev
root="$(cd "$(dirname "$0")/.." && pwd)"
config_file="$root/benchmarks/_bench_config.json"

profile="release"
benchmarks=()
run_all=0
forward_args=()
parsing_forward=0

while [[ $# -gt 0 ]]; do
  if [[ "$parsing_forward" -eq 1 ]]; then
    forward_args+=("$1")
    shift
    continue
  fi

  case "$1" in
    --)
      parsing_forward=1
      shift
      ;;
    --debug)
      profile="debug"
      shift
      ;;
    --release)
      profile="release"
      shift
      ;;
    --list)
      python3 "$root/scripts/bench_runner.py" --list
      exit 0
      ;;
    --all)
      run_all=1
      shift
      ;;
    -h|--help)
      echo "Usage: $0 [--debug|--release] [--all | <benchmark_name>] [-- <bench_args...>]"
      echo "       $0 --list"
      exit 0
      ;;
    *)
      benchmarks+=("$1")
      shift
      ;;
  esac
done

if [[ "$run_all" -eq 1 ]]; then
  if [[ ! -f "$config_file" ]]; then
    echo "[run_bench] ERROR: $config_file not found" >&2
    exit 1
  fi
  mapfile -t benchmarks < <(python3 -c \
    "import json; print('\n'.join(json.load(open('$config_file'))['benchmarks'].keys()))")
fi

if [[ "${#benchmarks[@]}" -eq 0 ]]; then
  echo "[run_bench] ERROR: no benchmark specified. Use '$0 --list' to see available benchmarks." >&2
  exit 1
fi

# Build or reuse cached podman image
echo "[run_bench] Ensuring podman image '$image' is up to date..." >&2
podman build -qt "$image" "$root/podman_build" >/dev/null

target_dir="$root/target-cluster"
cargo_build_flags=()
if [[ "$profile" == "release" ]]; then
  cargo_build_flags+=("--release")
fi

# Build requested benchmark binaries inside container
for bench in "${benchmarks[@]}"; do
  echo "[run_bench] Building '$bench' in container (profile: $profile)..." >&2
  podman run --rm \
      -v "$root":/workspace \
      -w /workspace \
      -e CARGO_TARGET_DIR=/workspace/target-cluster \
      "$image" \
      cargo build "${cargo_build_flags[@]}" -p rdmalib-benchmarks --bin "$bench"
done

# Deploy and run each benchmark from the host
status=0
for bench in "${benchmarks[@]}"; do
  bin_path="$target_dir/$profile/$bench"
  if [[ ! -x "$bin_path" ]]; then
    echo "[run_bench] ERROR: binary not found at $bin_path" >&2
    status=1
    continue
  fi

  echo "[run_bench] Launching benchmark '$bench' on cluster..."
  python3 "$root/scripts/bench_runner.py" \
      "$bin_path" \
      --profile "$profile" \
      -- "${forward_args[@]}" || status=1
done

exit "$status"
