#!/usr/bin/env python3
"""Cluster runner for rdmalib benchmarks.

Deploys benchmark binaries to the nodes configured in benchmarks/_bench_config.json
and executes them concurrently over SSH, passing cluster layout via environment
variables:

  RDMALIB_TEST_MY_NODE   index of the node the process runs on
  RDMALIB_TEST_NODES     comma-separated "<ip>:<tcp-port>:<rdma-port>"

After the run:
  - The JSON latency file is rsynced from the client node back to
    benchmarks/results/<bench_name>/<bench_name>_<timestamp>.json
  - The terminal summary (the ====...==== block) is also saved as
    benchmarks/results/<bench_name>/<bench_name>_<timestamp>.txt

Usage:
  scripts/bench_runner.py [--profile release|debug] <binary_path> [-- <bench_args...>]
  scripts/bench_runner.py --list
"""

import argparse
import asyncio
import json
import re
import subprocess
import sys
from datetime import datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
CONFIG_FILE = ROOT / "benchmarks" / "_bench_config.json"
RESULTS_DIR = ROOT / "benchmarks" / "results"

ENV_NODES = "RDMALIB_TEST_NODES"
ENV_MY_NODE = "RDMALIB_TEST_MY_NODE"


async def stream_pipe(pipe, prefix, lines_out: list):
    """Stream lines from a pipe, printing with prefix and collecting into lines_out."""
    while True:
        line = await pipe.readline()
        if not line:
            break
        text = line.decode().rstrip()
        print(f"[{prefix}] {text}", flush=True)
        lines_out.append((prefix, text))


async def run_on_node(hostname, remote_cmd, lines_out: list):
    """Launches the benchmark process on a single node concurrently."""
    process = await asyncio.create_subprocess_exec(
        "ssh",
        hostname,
        remote_cmd,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )

    await asyncio.gather(
        stream_pipe(process.stdout, f"{hostname}/stdout", lines_out),
        stream_pipe(process.stderr, f"{hostname}/stderr", lines_out),
    )

    return hostname, await process.wait()


def load_config():
    if not CONFIG_FILE.exists():
        print(f"Error: config file {CONFIG_FILE} not found.", file=sys.stderr)
        sys.exit(1)
    with open(CONFIG_FILE, "r") as f:
        return json.load(f)


def extract_summary(lines: list, client_hostname: str) -> str:
    """Extract the ===...=== summary block from the client node's stdout.

    The block layout emitted by print_summary is:
        =======...=======   ← separator 1 (opening)
        === <title>
        =======...=======   ← separator 2 (after title)
        <data lines>
        =======...=======   ← separator 3 (closing)
    We collect everything from separator 1 through separator 3 inclusive.
    """
    summary_lines = []
    in_block = False
    sep_count = 0
    for prefix, text in lines:
        if f"{client_hostname}/stdout" not in prefix:
            continue
        if text.startswith("======="):
            sep_count += 1
            if sep_count == 1:
                in_block = True
            if in_block:
                summary_lines.append(text)
            if sep_count == 3:
                # Closing separator collected; done.
                in_block = False
                sep_count = 0
            continue
        if in_block:
            summary_lines.append(text)
    return "\n".join(summary_lines)


def main():
    if "--" in sys.argv:
        dash_idx = sys.argv.index("--")
        runner_argv = sys.argv[1:dash_idx]
        bench_args = sys.argv[dash_idx + 1:]
    else:
        runner_argv = sys.argv[1:]
        bench_args = []

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

    args, extra = parser.parse_known_args(runner_argv)
    bench_args = bench_args + extra
    config = load_config()

    if args.list:
        print("Configured benchmarks:")
        for name, spec in config.get("benchmarks", {}).items():
            nodes = spec.get("nodes", [])
            desc = spec.get("description", "")
            print(f"  - {name:20} (nodes: {nodes}) {desc}")
        sys.exit(0)

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
        binary_path = ROOT / "target-cluster" / args.profile / bench_name
        if not binary_path.exists():
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

    # Timestamped filename used for both the remote JSON and the local results.
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    result_stem = f"{bench_name}_{timestamp}"
    remote_json_path = f"{remote_dir}/{result_stem}.json"

    # Inject --output into bench_args (only if the user hasn't already passed one).
    if "--output" not in bench_args and "-o" not in bench_args:
        bench_args = [*bench_args, "--output", remote_json_path]

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
            ["rsync", "-az", str(binary_path), f"{node['hostname']}:{remote_binary_path}"],
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

    # 3. Launch concurrently, collecting all output lines
    all_lines: list = []

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
                all_lines,
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

    if failed:
        sys.exit(1)

    # 5. Copy results from the client node (node index 1 in a 2-node layout,
    #    or the last node in general — measurements always run on non-server nodes).
    #    The client is nodes_config[1] when len > 1, else nodes_config[0].
    client_node = nodes_config[1] if len(nodes_config) > 1 else nodes_config[0]
    client_hostname = client_node["hostname"]

    local_results_dir = RESULTS_DIR / bench_name
    local_results_dir.mkdir(parents=True, exist_ok=True)

    local_json = local_results_dir / f"{result_stem}.json"
    local_txt = local_results_dir / f"{result_stem}.txt"

    print(f"\n[bench_runner] Fetching results from '{client_hostname}'...", flush=True)
    rsync_result = subprocess.run(
        ["rsync", "-az", f"{client_hostname}:{remote_json_path}", str(local_json)],
    )
    if rsync_result.returncode != 0:
        print(
            f"[bench_runner] WARNING: could not fetch {remote_json_path} from {client_hostname}. "
            f"The run may have failed before writing results.",
            file=sys.stderr,
        )
    else:
        print(f"[bench_runner] JSON results saved to: {local_json}", flush=True)

    # 6. Extract and save the summary block
    summary = extract_summary(all_lines, client_hostname)
    if summary:
        local_txt.write_text(summary + "\n")
        print(f"[bench_runner] Summary saved to:      {local_txt}", flush=True)
    else:
        print("[bench_runner] WARNING: no summary block found in output.", file=sys.stderr)


if __name__ == "__main__":
    main()
