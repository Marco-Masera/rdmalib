//! Benchmark utilities for rdmalib cluster benchmarks.
//!
//! Provides cluster node discovery (`ClusterContext`), synchronization barrier
//! across nodes (`GlobalSynch`), and latency/throughput measurement tools (`LatencyHistogram`).

use std::env;
use std::fmt;
use std::io::{self, Read, Write};
use std::net::{IpAddr, Ipv4Addr, SocketAddr, SocketAddrV4, TcpListener, TcpStream};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc;
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

use rdmalib::{RemoteMemoryProviderAddr, SharedMemoryRegionProviderAddr};

pub const ENV_NODES: &str = "RDMALIB_TEST_NODES";
pub const ENV_MY_NODE: &str = "RDMALIB_TEST_MY_NODE";

/// One deployed node in the benchmark cluster.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NodeInfo {
    pub index: usize,
    pub ip: String,
    pub tcp_port: u16,
    pub rdma_port: u16,
}

impl NodeInfo {
    /// Server-side address: provider listening on this node.
    pub fn provider_addr(&self) -> SharedMemoryRegionProviderAddr {
        SharedMemoryRegionProviderAddr::new(self.rdma_port, self.tcp_port)
    }

    /// Client-side address: reader connecting to this node from another node.
    pub fn remote_addr(&self) -> RemoteMemoryProviderAddr {
        RemoteMemoryProviderAddr::new(self.ip.clone(), self.rdma_port, self.tcp_port)
    }
}

impl fmt::Display for NodeInfo {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} (tcp {}, rdma {})", self.ip, self.tcp_port, self.rdma_port)
    }
}

/// The nodes deployed for this benchmark run, and which of them this process is.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClusterContext {
    pub my_node: usize,
    pub nodes: Vec<NodeInfo>,
}

impl ClusterContext {
    /// Reads and parses the runner-provided environment variables.
    pub fn from_env() -> Self {
        let nodes = env::var(ENV_NODES).unwrap_or_else(|_| {
            panic!("{ENV_NODES} is not set: cluster benchmarks must run under scripts/bench_runner.py")
        });
        let my_node = env::var(ENV_MY_NODE).unwrap_or_else(|_| {
            panic!("{ENV_MY_NODE} is not set: cluster benchmarks must run under scripts/bench_runner.py")
        });
        Self::parse(&my_node, &nodes)
            .unwrap_or_else(|e| panic!("invalid {ENV_MY_NODE}/{ENV_NODES} from runner: {e}"))
    }

    pub fn parse(my_node: &str, nodes: &str) -> Result<Self, String> {
        let parsed: Vec<NodeInfo> = nodes
            .split(',')
            .enumerate()
            .map(|(index, spec)| parse_node(index, spec))
            .collect::<Result<_, _>>()?;
        if parsed.is_empty() {
            return Err(format!("no nodes in `{nodes}`"));
        }
        let my_node: usize = my_node
            .parse()
            .map_err(|e| format!("bad {ENV_MY_NODE} `{my_node}`: {e}"))?;
        if my_node >= parsed.len() {
            return Err(format!(
                "{ENV_MY_NODE}={my_node} out of range ({} nodes deployed)",
                parsed.len()
            ));
        }
        Ok(Self {
            my_node,
            nodes: parsed,
        })
    }

    /// Whether this node is node 0 (conventionally the server/provider).
    pub fn is_server(&self) -> bool {
        self.my_node == 0
    }

    /// The node this process runs on.
    pub fn me(&self) -> &NodeInfo {
        &self.nodes[self.my_node]
    }

    /// The node with the given index.
    pub fn node(&self, index: usize) -> &NodeInfo {
        self.nodes.get(index).unwrap_or_else(|| {
            panic!(
                "benchmark uses node {index} but only {} node(s) were deployed",
                self.nodes.len()
            )
        })
    }

    pub fn len(&self) -> usize {
        self.nodes.len()
    }

    pub fn is_empty(&self) -> bool {
        self.nodes.is_empty()
    }

    /// All nodes except the one this process runs on.
    pub fn others(&self) -> impl Iterator<Item = &NodeInfo> {
        self.nodes
            .iter()
            .enumerate()
            .filter(|(index, _)| *index != self.my_node)
            .map(|(_, node)| node)
    }
}

fn parse_node(index: usize, spec: &str) -> Result<NodeInfo, String> {
    let parts: Vec<&str> = spec.split(':').collect();
    if parts.len() != 3 {
        return Err(format!(
            "node {index}: expected `<ip>:<tcp-port>:<rdma-port>`, got `{spec}`"
        ));
    }
    let port = |name: &str, raw: &str| {
        raw.parse::<u16>()
            .map_err(|e| format!("node {index}: bad {name} port `{raw}`: {e}"))
    };
    Ok(NodeInfo {
        index,
        ip: parts[0].to_string(),
        tcp_port: port("tcp", parts[1])?,
        rdma_port: port("rdma", parts[2])?,
    })
}

const SYNCH_POLL_INTERVAL: Duration = Duration::from_millis(10);
const SYNCH_IO_TIMEOUT: Duration = Duration::from_millis(1000);
const SYNCH_TIMEOUT: Duration = Duration::from_secs(60);

/// Barrier across deployed benchmark nodes.
pub struct GlobalSynch {
    peers: Vec<Ipv4Addr>,
    my_ip: Ipv4Addr,
    port: u16,
    timeout: Duration,
    clock: u64,
    known_clocks: Vec<u64>,
}

impl GlobalSynch {
    pub fn new(cluster: &ClusterContext) -> Self {
        let smallest = cluster
            .nodes
            .iter()
            .map(|node| node.tcp_port)
            .min()
            .expect("at least one node");
        assert!(
            smallest > 1,
            "no port below the smallest node TCP port {smallest}"
        );
        Self::with_port(cluster, smallest - 1)
    }

    pub fn with_port(cluster: &ClusterContext, port: u16) -> Self {
        let ip = |node: &NodeInfo| {
            node.ip
                .parse::<Ipv4Addr>()
                .unwrap_or_else(|e| panic!("node {}: bad IPv4 `{}`: {e}", node.index, node.ip))
        };
        let peers: Vec<Ipv4Addr> = cluster.others().map(ip).collect();
        Self {
            peers,
            my_ip: ip(cluster.me()),
            port,
            timeout: SYNCH_TIMEOUT,
            clock: 0,
            known_clocks: vec![0; cluster.len().saturating_sub(1)],
        }
    }

    pub fn synch(&mut self) -> io::Result<()> {
        self.clock += 1;
        let round = self.clock;
        if self.peers.is_empty() {
            return Ok(());
        }
        let listener = TcpListener::bind(SocketAddrV4::new(self.my_ip, self.port))?;
        listener.set_nonblocking(true)?;

        let (seen_tx, seen_rx) = mpsc::channel::<(IpAddr, u64)>();
        let shutdown = Arc::new(AtomicBool::new(false));
        let my_clock = self.clock;
        let responder = {
            let shutdown = Arc::clone(&shutdown);
            thread::spawn(move || {
                while !shutdown.load(Ordering::Relaxed) {
                    match listener.accept() {
                        Ok((stream, _)) => {
                            let _ = answer(stream, my_clock, &seen_tx);
                        }
                        Err(_) => thread::sleep(SYNCH_POLL_INTERVAL),
                    }
                }
            })
        };

        let deadline = Instant::now() + self.timeout;
        loop {
            while let Ok((ip, clock)) = seen_rx.try_recv() {
                if let Some(index) = self.peers.iter().position(|&peer| IpAddr::from(peer) == ip) {
                    self.known_clocks[index] = self.known_clocks[index].max(clock);
                }
            }
            let mut heard_from_all = true;
            for (index, &peer) in self.peers.iter().enumerate() {
                if self.known_clocks[index] >= round {
                    continue;
                }
                heard_from_all = false;
                if let Ok(clock) = self.poll(peer) {
                    self.known_clocks[index] = self.known_clocks[index].max(clock);
                }
            }
            if heard_from_all {
                break;
            }
            if Instant::now() >= deadline {
                self.bail_out(round);
            }
            thread::sleep(SYNCH_POLL_INTERVAL);
        }

        shutdown.store(true, Ordering::Relaxed);
        let _ = responder.join();
        Ok(())
    }

    fn bail_out(&self, round: u64) -> ! {
        let waiting: Vec<String> = self
            .peers
            .iter()
            .zip(&self.known_clocks)
            .filter(|&(_, &known)| known < round)
            .map(|(peer, &known)| match known {
                0 => format!("{peer} (never heard from)"),
                _ => format!("{peer} (last clock {known})"),
            })
            .collect();
        eprintln!(
            "GlobalSynch: barrier {round} on {}:{} did not resolve within {:?}; \
             still waiting for: {}. Exiting.",
            self.my_ip, self.port, self.timeout, waiting.join(", ")
        );
        std::process::exit(1)
    }

    fn poll(&self, peer: Ipv4Addr) -> io::Result<u64> {
        let addr = SocketAddr::V4(SocketAddrV4::new(peer, self.port));
        let mut stream = TcpStream::connect_timeout(&addr, SYNCH_IO_TIMEOUT)?;
        stream.set_read_timeout(Some(SYNCH_IO_TIMEOUT))?;
        stream.set_write_timeout(Some(SYNCH_IO_TIMEOUT))?;
        stream.write_all(&self.clock.to_le_bytes())?;
        let mut reply = [0u8; 8];
        stream.read_exact(&mut reply)?;
        Ok(u64::from_le_bytes(reply))
    }
}

fn answer(
    mut stream: TcpStream,
    my_clock: u64,
    seen: &mpsc::Sender<(IpAddr, u64)>,
) -> io::Result<()> {
    stream.set_read_timeout(Some(SYNCH_IO_TIMEOUT))?;
    stream.set_write_timeout(Some(SYNCH_IO_TIMEOUT))?;
    let mut request = [0u8; 8];
    stream.read_exact(&mut request)?;
    let their_clock = u64::from_le_bytes(request);
    let peer_ip = stream.peer_addr()?.ip();
    let _ = seen.send((peer_ip, their_clock));
    let reply = my_clock.to_le_bytes();
    stream.write_all(&reply)
}

/// Latency statistics collector using HDR Histogram.
pub struct LatencyHistogram {
    hist: hdrhistogram::Histogram<u64>,
}

impl LatencyHistogram {
    /// Tracks latencies in nanoseconds up to 10 seconds with 3 sig figs.
    pub fn new() -> Self {
        Self {
            hist: hdrhistogram::Histogram::<u64>::new_with_bounds(1, 10_000_000_000, 3)
                .expect("valid histogram bounds"),
        }
    }

    /// Record a single measurement duration.
    pub fn record(&mut self, duration: Duration) {
        let nanos = duration.as_nanos() as u64;
        let _ = self.hist.record(nanos.max(1));
    }

    /// Prints a formatted summary table of percentiles in microseconds.
    pub fn print_summary(&self, title: &str, total_elapsed: Duration, payload_bytes: usize) {
        let count = self.hist.len();
        if count == 0 {
            println!("No samples recorded for {title}");
            return;
        }

        let to_micros = |val: u64| val as f64 / 1_000.0;
        let min = to_micros(self.hist.min());
        let mean = to_micros(self.hist.mean() as u64);
        let p50 = to_micros(self.hist.value_at_quantile(0.50));
        let p90 = to_micros(self.hist.value_at_quantile(0.90));
        let p99 = to_micros(self.hist.value_at_quantile(0.99));
        let p999 = to_micros(self.hist.value_at_quantile(0.999));
        let max = to_micros(self.hist.max());

        let total_secs = total_elapsed.as_secs_f64();
        let ops_per_sec = count as f64 / total_secs;
        let mb_per_sec = (count as f64 * payload_bytes as f64) / (1024.0 * 1024.0 * total_secs);

        println!("\n=== {title} ===");
        println!("Operations:    {count}");
        println!("Payload Size:  {payload_bytes} B");
        println!("Total Time:    {total_secs:.3} s");
        println!("Throughput:    {ops_per_sec:.1} ops/s ({mb_per_sec:.2} MB/s)");
        println!("Latency (µs):");
        println!("  min:   {min:>8.2}");
        println!("  mean:  {mean:>8.2}");
        println!("  p50:   {p50:>8.2}");
        println!("  p90:   {p90:>8.2}");
        println!("  p99:   {p99:>8.2}");
        println!("  p99.9: {p999:>8.2}");
        println!("  max:   {max:>8.2}");
    }
}

impl Default for LatencyHistogram {
    fn default() -> Self {
        Self::new()
    }
}
