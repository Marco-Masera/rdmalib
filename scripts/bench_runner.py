#!/usr/bin/env python3
"""Cluster runner for rdmalib benchmarks.

Deploys benchmark binaries to the nodes configured in benchmarks/_bench_config.json
and executes them concurrently over SSH, passing cluster layout via environment
variables:

  RDMALIB_TEST_MY_NODE   index of the node the process runs on
  RDMALIB_TEST_NODES     comma-separated "<ip>:<tcp-port>:<rdma-port>"

Usage:
  scripts/bench_runner.py <binary_path> [-- <bench_args...>]
  scripts/bench_runner.py --bench <bench_name> [--profile release|debug] [-- <bench_args...>]
"""

import argparse
import asyncio
import json
import os
from pathlib import Path
import subprocess
import sys

ROOT = Path(__file__).resolve().parent.parent
CONFIG_FILE = ROOT / "benchmarks" / "_bench_config.json"

ENV_NODES = "RDMALIB_TEST_NODES"
ENV_MY_NODE = "RDMALIB_TEST_MY_NODE"


async def stream_pipe(pipe, prefix):
    while True:
        line = await pipe.readline()
        if not line:
            break
        print(f"[{prefix}] {line.decode().rstrip()}", flush=True)


async def run_on_node(hostname, remote_cmd):
    """Launches the benchmark process on a single node concurrently."""
    process = await asyncio.create_subprocess_exec(
        "ssh",
        hostname,
        remote_cmd,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )

    await asyncio.gather(
        stream_pipe(process.stdout, f"{hostname}/stdout"),
        stream_pipe(process.stderr, f"{hostname}/stderr"),
    )

    return hostname, await process.wait()


def load_config():
    if not CONFIG_FILE.exists():
        print(f"Error: config file {CONFIG_FILE} not found.", file=sys.stderr)
        sys.exit(1)
    with open(CONFIG_FILE, "r") as f:
        return json.load(f)


def main():
    parser = argparse.ArgumentParser(
        description="Deploy and run rdmalib cluster benchmarks."
    )
    parser.add_argument(
        "binary_or_bench",
        nargs="?",
        help="Path to compiled benchmark binary or benchmark name",
    )
    parser.add_argument(
        "--bench",
        help="Benchmark name (if binary path not directly specified)",
    )
    parser.add_argument(
        "--profile",
        choices=["release", "debug"],
        default="release",
        help="Build profile (default: release)",
    )
    parser.add_argument(
        "--list",
        action="store_true",
        help="List available benchmarks in config",
    )
    parser.add_argument(
        "bench_args",
        nargs=argparse.REMAINDER,
        help="Arguments forwarded to the benchmark binary (use after --)",
    )

    args = parser.parse_args()
    config = load_config()

    if args.list:
        print("Configured benchmarks:")
        for name, spec in config.get("benchmarks", {}).items():
            nodes = spec.get("nodes", [])
            desc = spec.get("description", "")
            print(f"  - {name:20} (nodes: {nodes}) {desc}")
        sys.exit(0)

    # Clean up forwarded args if '--' was passed
    bench_args = args.bench_args
    if bench_args and bench_args[0] == "--":
        bench_args = bench_args[1:]

    # Resolve benchmark name and binary path
    target = args.bench or args.binary_or_bench
    if not target:
        parser.print_help()
        sys.exit(1)

    binary_path = Path(target)
    if binary_path.exists() and binary_path.is_file():
        bench_name = binary_path.name
    else:
        bench_name = target
        # Look in target-cluster/<profile>/
        binary_path = ROOT / "target-cluster" / args.profile / bench_name
        if not binary_path.exists():
            # Also check target-cluster/<profile>/deps or plain target/
            alt_path = ROOT / "target" / args.profile / bench_name
            if alt_path.exists():
                binary_path = alt_path

    if not binary_path.exists():
        print(
            f"Error: binary '{binary_path}' not found. Please compile it first.",
            file=sys.stderr,
        )
        sys.exit(1)

    bench_cfg = config.get("benchmarks", {}).get(bench_name)
    if bench_cfg is None:
        # Default fallback to first 2 nodes if benchmark isn't specifically mapped
        print(
            f"[bench_runner] Note: benchmark '{bench_name}' not listed in {CONFIG_FILE}. Defaulting to nodes [0, 1]."
        )
        node_indices = [0, 1]
    else:
        node_indices = bench_cfg.get("nodes", [0, 1])

    all_nodes = config.get("nodes", [])
    nodes_config = [all_nodes[i] for i in node_indices if i < len(all_nodes)]
    if not nodes_config:
        print(f"Error: no valid nodes resolved for benchmark '{bench_name}'.", file=sys.stderr)
        sys.exit(1)

    remote_dir = config.get("remote_path", "/tmp/rdmalib-benchmarks")
    remote_binary_path = f"{remote_dir}/{binary_path.name}"

    print(
        f"\n[bench_runner] Deploying '{bench_name}' on "
        f"{[n['hostname'] for n in nodes_config]} node(s) (profile: {args.profile}).",
        flush=True,
    )
    if bench_args:
        print(f"[bench_runner] Forwarded arguments: {' '.join(bench_args)}", flush=True)

    # 1. Sync binary to each node
    for node in nodes_config:
        subprocess.run(["ssh", node["hostname"], f"mkdir -p {remote_dir}"], check=True)
        subprocess.run(
            ["rsync", "-avz", str(binary_path), f"{node['hostname']}:{remote_binary_path}"],
            check=True,
        )

    # 2. Build cluster network layout
    base_tcp = config.get("base_ports", {}).get("TCP", 8300)
    base_rdma = config.get("base_ports", {}).get("RDMA", 9300)
    ports_and_addresses = [
        {
            "node": i,
            "tcp_port": base_tcp + i,
            "rdma_port": base_rdma + i,
            "ip_address": node["ip"],
        }
        for i, node in enumerate(nodes_config)
    ]
    nodes_env = ",".join(
        f"{n['ip_address']}:{n['tcp_port']}:{n['rdma_port']}"
        for n in ports_and_addresses
    )

    # 3. Launch concurrently
    async def run_cluster():
        tasks = [
            run_on_node(
                node["hostname"],
                " ".join([
                    "env",
                    f"{ENV_MY_NODE}={i}",
                    f"{ENV_NODES}={nodes_env}",
                    remote_binary_path,
                    *bench_args,
                ]),
            )
            for i, node in enumerate(nodes_config)
        ]
        return await asyncio.gather(*tasks)

    results = asyncio.run(run_cluster())

    # 4. Check exit codes
    failed = False
    for hostname, code in results:
        if code != 0:
            print(
                f"[bench_runner] ERROR: node {hostname} exited with code {code}",
                file=sys.stderr,
            )
            failed = True

    sys.exit(1 if failed else 0)


if __name__ == "__main__":
    main()
