#!/usr/bin/env python3
"""Cargo test runner for the cluster integration tests.

Cargo invokes this script for every test binary (see .cargo/config.toml).
A binary whose name — the part before the '-' hash in the executable file
name — is listed in tests/_test_config.json is a *cluster* test: it is
copied to every node configured for it and launched there via ssh.

Each remote process learns the cluster layout through environment
variables set by this script:

  RDMALIB_TEST_MY_NODE   index of the node the process runs on
                         (its position in RDMALIB_TEST_NODES)
  RDMALIB_TEST_NODES     comma-separated "<ip>:<tcp-port>:<rdma-port>",
                         one entry per deployed node, in config order
                         (IPv4 only; IPv6 would need another separator)

Cluster tests need RDMA hardware, so they are #[ignore]-gated and are only
deployed when --ignored (or --include-ignored) is among the forwarded
test-harness arguments:

    ./podman_build/link_test.sh --test read_test -- --ignored

Deployed runs get --nocapture appended automatically (unless already
present), so the tests' println! output streams back live.

Every other binary — and cluster tests when no such flag is passed — is
executed locally, untouched, so a plain `cargo test` stays green on
machines without a cluster.
"""

import asyncio
import json
import os
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
CONFIG_FILE = ROOT / "tests" / "_test_config.json"

ENV_NODES = "RDMALIB_TEST_NODES"
ENV_MY_NODE = "RDMALIB_TEST_MY_NODE"
# Test-harness flags that ask for the #[ignore]d (hardware) tests.
DEPLOY_FLAGS = {"--ignored", "--include-ignored"}


def run_locally(binary_path, test_args):
    """Exec the binary on this machine, exactly as cargo would have."""
    sys.stdout.flush()
    sys.stderr.flush()
    try:
        os.execv(binary_path, [binary_path, *test_args])
    except OSError as e:
        print(f"[test_runner] failed to run {binary_path} locally: {e}", file=sys.stderr)
        sys.exit(1)


async def stream_pipe(pipe, prefix):
    while True:
        line = await pipe.readline()
        if not line:
            break
        print(f"[{prefix}] {line.decode().rstrip()}", flush=True)


async def run_on_node(hostname, remote_cmd):
    """Launches the test process on a single node concurrently."""
    process = await asyncio.create_subprocess_exec(
        "ssh", hostname, remote_cmd,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )

    # Stream stdout and stderr concurrently for this node
    await asyncio.gather(
        stream_pipe(process.stdout, f"{hostname}/stdout"),
        stream_pipe(process.stderr, f"{hostname}/stderr"),
    )

    # Wait for process completion and return the exit code
    return hostname, await process.wait()


def main():
    if len(sys.argv) < 2:
        print("Error: no test executable passed by cargo", file=sys.stderr)
        sys.exit(1)

    binary_path = Path(sys.argv[1]).resolve()
    test_args = sys.argv[2:]  # forwarded to the test harness (e.g. filters, --nocapture)

    # 1. Is this binary a cluster test?
    # Cargo names test binaries like 'read_test-8f3b12a9e102c4d'.
    test_name = binary_path.stem.split("-")[0]

    if CONFIG_FILE.exists():
        config = json.loads(CONFIG_FILE.read_text())
        test_cfg = config.get("tests", {}).get(test_name)
    else:
        config, test_cfg = None, None

    if test_cfg is None:
        run_locally(binary_path, test_args)  # e.g. the lib unit tests

    if not DEPLOY_FLAGS.intersection(test_args):
        print(f"[test_runner] '{test_name}' is a cluster test but no "
              f"{'/'.join(sorted(DEPLOY_FLAGS))} flag was passed; running it locally",
              flush=True)
        run_locally(binary_path, test_args)

    # The harness captures each test's stdout and only shows it on failure;
    # on the cluster we stream per-node output live, so stop capturing.
    # (println! from passing tests would otherwise be dropped entirely.)
    if "--nocapture" not in test_args:
        test_args = [*test_args, "--nocapture"]

    # 2. Resolve the nodes this test deploys on.
    nodes_config = [config["nodes"][i] for i in test_cfg["nodes"]]

    remote_dir = config.get("remote_path")
    if not remote_dir:
        print(f"Error: set 'remote_path' in {CONFIG_FILE} before running cluster tests.",
              file=sys.stderr)
        sys.exit(1)
    remote_binary_path = f"{remote_dir}/{binary_path.name}"

    print(f"\n[test_runner] Deploying '{test_name}' on "
          f"{[n['hostname'] for n in nodes_config]} node(s).", flush=True)

    # 3. Transfer the compiled binary to the nodes.
    for node in nodes_config:
        # Ensure remote directory exists
        subprocess.run(["ssh", node["hostname"], f"mkdir -p {remote_dir}"], check=True)
        # Sync binary to the cluster
        subprocess.run(["rsync", "-avz", str(binary_path),
                        f"{node['hostname']}:{remote_binary_path}"], check=True)

    # 4. Assign ports and build the layout handed to every binary.
    base_tcp = config["base_ports"]["TCP"]
    base_rdma = config["base_ports"]["RDMA"]
    ports_and_addresses_info = [
        {
            "node": i,
            "tcp_port": base_tcp + i,
            "rdma_port": base_rdma + i,
            "ip_address": node["ip"],
        }
        for i, node in enumerate(nodes_config)
    ]
    # One shell-safe word: "<ip>:<tcp-port>:<rdma-port>,..." (IPv4 only).
    nodes_env = ",".join(
        f"{n['ip_address']}:{n['tcp_port']}:{n['rdma_port']}"
        for n in ports_and_addresses_info
    )

    async def run_cluster():
        tasks = [
            run_on_node(
                node["hostname"],
                " ".join([
                    "env",
                    f"{ENV_MY_NODE}={i}",
                    f"{ENV_NODES}={nodes_env}",
                    remote_binary_path,
                    *test_args,
                ]),
            )
            for i, node in enumerate(nodes_config)
        ]
        return await asyncio.gather(*tasks)

    results = asyncio.run(run_cluster())

    # 5. Check exit codes across all nodes.
    failed = False
    for hostname, code in results:
        if code != 0:
            print(f"[test_runner] ERROR: node {hostname} exited with code {code}",
                  file=sys.stderr)
            failed = True

    sys.exit(1 if failed else 0)


if __name__ == "__main__":
    main()
