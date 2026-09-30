//! Benchmark utilities for rdmalib cluster benchmarks.
//!
//! Provides cluster node discovery (`ClusterContext`), synchronization barrier
//! across nodes (`GlobalSynch`), latency tracking (`LatencyHistogram`), data
//! structures for typed benchmarks (`Word8`), and runner routines for single
//! and concurrent RDMA read/write benchmarks.

use std::env;
use std::ffi::c_int;
use std::fmt;
use std::future::Future;
use std::io::{self, Read, Write};
use std::net::{IpAddr, Ipv4Addr, SocketAddr, SocketAddrV4, TcpListener, TcpStream};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc;
use std::task::{Context, Poll};
use std::thread;
use std::time::{Duration, Instant};

use rdmalib::{
    Engine, RBufOp, RegionOp, RemoteMemoryProvider, RemoteMemoryProviderAddr, RemoteSafe,
    SharedMemoryRegionProvider, SharedMemoryRegionProviderAddr,
};

pub const ENV_NODES: &str = "RDMALIB_TEST_NODES";
pub const ENV_MY_NODE: &str = "RDMALIB_TEST_MY_NODE";

/// The CPU the benchmark client's engine thread is pinned to by
/// default — the thread that posts and polls every operation.
pub const DEFAULT_ENGINE_CPU: u32 = 0;

/// The CPU the benchmark client's own (submitting and waiting)
/// thread is pinned to by default — the engine's L2 mate on the
/// first benchmark cluster's nodes (cpu0/cpu2 share an L2): the two
/// threads share the mailbox and slab locks across every submission
/// and resolution, and on those FSB-era nodes a lock line that
/// stays inside one L2 transfers in tens of nanoseconds while one
/// that crosses packages costs microseconds — sharing the L2 with
/// the engine beat a private one by ~2.7 µs per sequential op.
/// Away from the NIC's completion interrupt (CPU 1) either way.
/// The placement is a per-node property (same LLC as the engine is
/// the generic rule; where that is depends on the machine — see
/// "Tuning for latency" in the library's engine docs), so every
/// placement-taking benchmark exposes `--engine-cpu`/`--client-cpu`
/// to override these defaults.
pub const DEFAULT_CLIENT_CPU: u32 = 2;

/// Where a benchmark client pins its threads: the engine's CPU and
/// the client's own (submitting and waiting) CPU. The defaults are
/// the first benchmark cluster's placement; override per node with
/// `--engine-cpu`/`--client-cpu` once the machine's topology and
/// interrupt placement are known.
#[derive(Clone, Copy, Debug)]
pub struct Placement {
    pub engine_cpu: u32,
    pub client_cpu: u32,
}

impl Default for Placement {
    fn default() -> Self {
        Self {
            engine_cpu: DEFAULT_ENGINE_CPU,
            client_cpu: DEFAULT_CLIENT_CPU,
        }
    }
}

/// glibc's `cpu_set_t` is a fixed 128-byte mask of 1024 CPUs — the
/// same mirror `rdmalib`'s internal os module carries. The library
/// exposes engine-thread pinning (`Engine::on_cpu`) but not
/// pin-this-thread, so the benchmark declares its own — same FFI
/// rules: `pid` 0 addresses the calling thread, verified against
/// sched_setaffinity(2).
const CPU_SET_BYTES: usize = 128;
type CpuSet = [u64; CPU_SET_BYTES / 8];

unsafe extern "C" {
    fn sched_setaffinity(pid: c_int, cpusetsize: usize, mask: *const CpuSet) -> c_int;
}

/// Pin the calling thread to `cpu`.
fn pin_current_thread(cpu: u32) -> io::Result<()> {
    if cpu >= 1024 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("cpu id {cpu} exceeds the affinity mask's limit of 1023"),
        ));
    }
    let mut set: CpuSet = [0; CPU_SET_BYTES / 8];
    set[(cpu / 64) as usize] |= 1u64 << (cpu % 64);
    let ret = unsafe { sched_setaffinity(0, CPU_SET_BYTES, &set) };
    if ret != 0 {
        return Err(io::Error::other(format!(
            "sched_setaffinity({cpu}) failed: {}",
            io::Error::last_os_error()
        )));
    }
    Ok(())
}

/// Set the benchmark client up the way the latency numbers want it:
/// the engine pinned to `placement.engine_cpu`, this thread to
/// `placement.client_cpu` — returned for `RemoteMemoryProvider::
/// with_engine`.
fn pinned_client(placement: Placement) -> io::Result<Engine> {
    // The engine first: its CPU is validated against this thread's
    // mask as it is now — the spawned engine thread inherits it, so
    // pinning this thread first would hide the engine's CPU from the
    // check. Only then does this thread pin itself to its own CPU.
    let engine = Engine::on_cpu(placement.engine_cpu)?;
    pin_current_thread(placement.client_cpu)?;
    println!(
        "[client] engine pinned to CPU {}, client thread pinned to CPU {}",
        placement.engine_cpu, placement.client_cpu
    );
    Ok(engine)
}

/// How a single-operation benchmark's client waits for each
/// operation: parked (`RegionOp::wait` — the blocking join) or
/// spinning (a busy-poll of the future — the floor without the
/// park/unpark cycle).
#[derive(Clone, Copy, Debug, PartialEq, Eq, clap::ValueEnum)]
pub enum WaitMode {
    Park,
    Spin,
}

/// Wait for `future` by busy-polling it — no parked thread, no futex:
/// the wait side's floor (compare against [`WaitMode::Park`] to see
/// the park/unpark cycle's cost). The engine does the completing on
/// its own core; this core polls the future until it resolves.
pub fn spin_block_on<F: Future>(future: F) -> F::Output {
    let waker = futures::task::noop_waker();
    let mut cx = Context::from_waker(&waker);
    let mut future = std::pin::pin!(future);
    loop {
        if let Poll::Ready(output) = future.as_mut().poll(&mut cx) {
            return output;
        }
        std::hint::spin_loop();
    }
}

/// 8-word struct (64 bytes total) for typed multi-word benchmarks.
#[repr(C)]
#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct Word8 {
    pub words: [u64; 8],
}

impl Word8 {
    pub const fn new(val: u64) -> Self {
        Self { words: [val; 8] }
    }
}

impl Default for Word8 {
    fn default() -> Self {
        Self::new(0)
    }
}

rdmalib::impl_remote_safe!(Word8);

/// Memory buffer constants.
/// Large buffer ensuring concurrent operations access independent disjoint sections.
pub const UNTYPED_BUFFER_BYTES: usize = 4 * 1024 * 1024; // 4 MB
pub const SECTION_SIZE_BYTES: usize = 4096; // 4 KB per concurrent section

pub const TYPED_BUFFER_ELEMS: usize = 65536; // 64K Word8 elements = 4 MB
pub const SECTION_SIZE_ELEMS: usize = 64; // 64 Word8 elements = 4096 bytes per section

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum OpKind {
    Read,
    Write,
}

impl fmt::Display for OpKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            OpKind::Read => write!(f, "Read"),
            OpKind::Write => write!(f, "Write"),
        }
    }
}

/// Benchmark measurement output saved to disk.
#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub struct BenchmarkResult {
    pub benchmark: String,
    pub op: String,
    pub is_typed: bool,
    pub iters: usize,
    pub concurrency: usize,
    pub entries_per_op: usize,
    pub payload_bytes_per_op: usize,
    pub unit: String,
    pub latencies_ms: Vec<f64>,
}

impl BenchmarkResult {
    pub fn save_to_file(&self, path: &str) -> io::Result<()> {
        let json = serde_json::to_string_pretty(self).map_err(io::Error::other)?;
        std::fs::write(path, json)?;
        println!(
            "\n[output] Stored {} latency measurements in milliseconds to '{}'",
            self.latencies_ms.len(),
            path
        );
        Ok(())
    }
}

/// One deployed node in the benchmark cluster.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NodeInfo {
    pub index: usize,
    pub ip: String,
    pub tcp_port: u16,
    pub rdma_port: u16,
}

impl NodeInfo {
    pub fn provider_addr(&self) -> SharedMemoryRegionProviderAddr {
        SharedMemoryRegionProviderAddr::new(self.rdma_port, self.tcp_port)
    }

    pub fn remote_addr(&self) -> RemoteMemoryProviderAddr {
        RemoteMemoryProviderAddr::new(self.ip.clone(), self.rdma_port, self.tcp_port)
    }
}

impl fmt::Display for NodeInfo {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{} (tcp {}, rdma {})",
            self.ip, self.tcp_port, self.rdma_port
        )
    }
}

/// The nodes deployed for this benchmark run, and which of them this process is.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClusterContext {
    pub my_node: usize,
    pub nodes: Vec<NodeInfo>,
}

impl ClusterContext {
    pub fn from_env() -> Self {
        let nodes = env::var(ENV_NODES).unwrap_or_else(|_| {
            panic!(
                "{ENV_NODES} is not set: cluster benchmarks must run under scripts/bench_runner.py"
            )
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

    pub fn is_server(&self) -> bool {
        self.my_node == 0
    }

    pub fn me(&self) -> &NodeInfo {
        &self.nodes[self.my_node]
    }

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
            self.my_ip,
            self.port,
            self.timeout,
            waiting.join(", ")
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
    pub fn new() -> Self {
        Self {
            hist: hdrhistogram::Histogram::<u64>::new_with_bounds(1, 10_000_000_000, 3)
                .expect("valid histogram bounds"),
        }
    }

    pub fn record(&mut self, duration: Duration) {
        let nanos = duration.as_nanos() as u64;
        let _ = self.hist.record(nanos.max(1));
    }

    pub fn print_summary(&self, title: &str, total_elapsed: Duration, payload_bytes: usize) {
        let count = self.hist.len();
        if count == 0 {
            println!("No samples recorded for {title}");
            return;
        }

        let to_micros = |val: u64| val as f64 / 1_000.0;
        let to_millis = |val: u64| val as f64 / 1_000_000.0;

        let min_us = to_micros(self.hist.min());
        let mean_us = to_micros(self.hist.mean() as u64);
        let p50_us = to_micros(self.hist.value_at_quantile(0.50));
        let p90_us = to_micros(self.hist.value_at_quantile(0.90));
        let p99_us = to_micros(self.hist.value_at_quantile(0.99));
        let p999_us = to_micros(self.hist.value_at_quantile(0.999));
        let max_us = to_micros(self.hist.max());

        let min_ms = to_millis(self.hist.min());
        let mean_ms = to_millis(self.hist.mean() as u64);
        let p50_ms = to_millis(self.hist.value_at_quantile(0.50));
        let p90_ms = to_millis(self.hist.value_at_quantile(0.90));
        let p99_ms = to_millis(self.hist.value_at_quantile(0.99));
        let p999_ms = to_millis(self.hist.value_at_quantile(0.999));
        let max_ms = to_millis(self.hist.max());

        let total_secs = total_elapsed.as_secs_f64();
        let ops_per_sec = count as f64 / total_secs;
        let mb_per_sec = (count as f64 * payload_bytes as f64) / (1024.0 * 1024.0 * total_secs);

        println!("\n=======================================================");
        println!("=== {title}");
        println!("=======================================================");
        println!("Iterations:       {count}");
        println!("Payload per op:   {payload_bytes} B");
        println!("Total Duration:   {total_secs:.4} s");
        println!("Throughput:       {ops_per_sec:.1} ops/s ({mb_per_sec:.2} MB/s)");
        println!("Latency Percentiles:");
        println!("  min:   {:>8.2} µs  ({:>8.4} ms)", min_us, min_ms);
        println!("  mean:  {:>8.2} µs  ({:>8.4} ms)", mean_us, mean_ms);
        println!("  p50:   {:>8.2} µs  ({:>8.4} ms)", p50_us, p50_ms);
        println!("  p90:   {:>8.2} µs  ({:>8.4} ms)", p90_us, p90_ms);
        println!("  p99:   {:>8.2} µs  ({:>8.4} ms)", p99_us, p99_ms);
        println!("  p99.9: {:>8.2} µs  ({:>8.4} ms)", p999_us, p999_ms);
        println!("  max:   {:>8.2} µs  ({:>8.4} ms)", max_us, max_ms);
        println!("=======================================================");
    }
}

impl Default for LatencyHistogram {
    fn default() -> Self {
        Self::new()
    }
}

// -----------------------------------------------------------------------------
// Benchmark Execution Helpers
// -----------------------------------------------------------------------------

const REGION_NAME_UNTYPED: &str = "bench_untyped_region";
const REGION_NAME_TYPED: &str = "bench_typed_region";

/// Wait for one operation per `mode` — parked (the blocking join) or
/// spinning (the floor).
fn wait_op<T: RemoteSafe>(op: RegionOp<T>, mode: WaitMode) -> io::Result<Vec<T>> {
    match mode {
        WaitMode::Park => op.wait(),
        WaitMode::Spin => spin_block_on(op),
    }
}

/// Single word (8 bytes), untyped Read or Write.
pub fn run_single_untyped(
    op: OpKind,
    iters: usize,
    warmup: usize,
    output_path: Option<String>,
    wait_mode: WaitMode,
    placement: Placement,
) -> Result<(), Box<dyn std::error::Error>> {
    let cluster = ClusterContext::from_env();
    let mut synch = GlobalSynch::new(&cluster);
    const WORD_BYTES: usize = 8;

    if cluster.is_server() {
        println!(
            "[node {}] server: registering untyped buffer ({} MB)...",
            cluster.my_node,
            UNTYPED_BUFFER_BYTES / (1024 * 1024)
        );
        let owner = SharedMemoryRegionProvider::new(cluster.me().provider_addr());
        let _handle = owner.register_typed(REGION_NAME_UNTYPED, vec![0xABu8; UNTYPED_BUFFER_BYTES]);
        owner.serve()?;

        println!("[node {}] server listening, barrier 1...", cluster.my_node);
        synch.synch()?;
        println!(
            "[node {}] server waiting for client to finish, barrier 2...",
            cluster.my_node
        );
        synch.synch()?;
        println!("[node {}] server finished.", cluster.my_node);
    } else {
        println!("[node {}] client: waiting for server...", cluster.my_node);
        synch.synch()?;

        let engine = pinned_client(placement)?;
        let server_node = cluster.node(0);
        let reader = RemoteMemoryProvider::with_engine(server_node.remote_addr(), engine);
        reader.update(0)?;

        let catalog = reader.get_remote_mr_metadata();
        let region_meta = catalog
            .iter()
            .find(|m| m.name == REGION_NAME_UNTYPED)
            .ok_or("untyped region not found in catalog")?;
        let region = reader
            .get_remote_mr(region_meta, Some(0))
            .ok_or("failed to get remote MR")?;

        // One buffer per direction, reused: an operation resolves with
        // its buffer back, so every iteration after the first
        // allocates nothing (perftest likewise posts one pre-allocated
        // buffer) — the loop measures the round trip, not the
        // allocator.
        let mut read_buffer = vec![0u8; WORD_BYTES];
        let mut write_buffer = vec![0x55u8; WORD_BYTES];

        if warmup > 0 {
            println!(
                "[node {}] running {} warmup iterations...",
                cluster.my_node, warmup
            );
            for _ in 0..warmup {
                match op {
                    OpKind::Read => {
                        read_buffer = wait_op(
                            region.read_into_async(0, std::mem::take(&mut read_buffer))?,
                            wait_mode,
                        )?;
                    }
                    OpKind::Write => {
                        write_buffer = wait_op(
                            region.write_async(0, std::mem::take(&mut write_buffer))?,
                            wait_mode,
                        )?;
                    }
                }
            }
        }

        println!(
            "[node {}] running {} measured iterations ({} single word untyped, wait: {:?})...",
            cluster.my_node, iters, op, wait_mode
        );
        let mut latencies_ms = Vec::with_capacity(iters);
        let mut hist = LatencyHistogram::new();
        let start_all = Instant::now();

        for _ in 0..iters {
            let t0 = Instant::now();
            match op {
                OpKind::Read => {
                    read_buffer = wait_op(
                        region.read_into_async(0, std::mem::take(&mut read_buffer))?,
                        wait_mode,
                    )?;
                }
                OpKind::Write => {
                    write_buffer = wait_op(
                        region.write_async(0, std::mem::take(&mut write_buffer))?,
                        wait_mode,
                    )?;
                }
            }
            let elapsed = t0.elapsed();
            latencies_ms.push(elapsed.as_secs_f64() * 1000.0);
            hist.record(elapsed);
        }

        let total_time = start_all.elapsed();
        let bench_name = format!("single_{}_untyped", op.to_string().to_lowercase());
        hist.print_summary(
            &format!("Single {} Untyped (1 word = 8 B)", op),
            total_time,
            WORD_BYTES,
        );

        let result = BenchmarkResult {
            benchmark: bench_name.clone(),
            op: op.to_string(),
            is_typed: false,
            iters,
            concurrency: 1,
            entries_per_op: 1,
            payload_bytes_per_op: WORD_BYTES,
            unit: "milliseconds".to_string(),
            latencies_ms,
        };
        let save_path = output_path.unwrap_or_else(|| format!("{}_latencies.json", bench_name));
        result.save_to_file(&save_path)?;

        synch.synch()?;
    }
    Ok(())
}

/// Single multi-word, typed (Word8 struct = 8 words = 64 bytes).
/// Entries: 1 (8 words), 4 (32 words), 16 (128 words), 32 (256 words).
pub fn run_single_typed(
    op: OpKind,
    entries: usize,
    iters: usize,
    warmup: usize,
    output_path: Option<String>,
    wait_mode: WaitMode,
    placement: Placement,
) -> Result<(), Box<dyn std::error::Error>> {
    let cluster = ClusterContext::from_env();
    let mut synch = GlobalSynch::new(&cluster);
    let payload_bytes = entries * std::mem::size_of::<Word8>();

    if cluster.is_server() {
        println!(
            "[node {}] server: registering typed Word8 buffer ({} elements)...",
            cluster.my_node, TYPED_BUFFER_ELEMS
        );
        let owner = SharedMemoryRegionProvider::new(cluster.me().provider_addr());
        let _handle = owner.register_typed(
            REGION_NAME_TYPED,
            vec![Word8::new(0x1234); TYPED_BUFFER_ELEMS],
        );
        owner.serve()?;

        println!("[node {}] server listening, barrier 1...", cluster.my_node);
        synch.synch()?;
        println!(
            "[node {}] server waiting for client to finish, barrier 2...",
            cluster.my_node
        );
        synch.synch()?;
        println!("[node {}] server finished.", cluster.my_node);
    } else {
        println!("[node {}] client: waiting for server...", cluster.my_node);
        synch.synch()?;

        let engine = pinned_client(placement)?;
        let server_node = cluster.node(0);
        let reader = RemoteMemoryProvider::with_engine(server_node.remote_addr(), engine);
        reader.update(0)?;

        let catalog = reader.get_remote_mr_metadata();
        let region_meta = catalog
            .iter()
            .find(|m| m.name == REGION_NAME_TYPED)
            .ok_or("typed region not found in catalog")?;
        let region = reader
            .get_remote_mr(region_meta, Some(0))
            .ok_or("failed to get remote MR")?;

        // One buffer per direction, reused — an operation resolves
        // with its buffer back, so every iteration after the first
        // allocates (and, for the write, clones) nothing.
        let mut read_buffer = vec![Word8::default(); entries];
        let mut write_buffer = vec![Word8::new(0x5678); entries];

        if warmup > 0 {
            println!(
                "[node {}] running {} warmup iterations...",
                cluster.my_node, warmup
            );
            for _ in 0..warmup {
                match op {
                    OpKind::Read => {
                        read_buffer = wait_op(
                            region.read_into_typed_async::<Word8>(
                                0,
                                std::mem::take(&mut read_buffer),
                            )?,
                            wait_mode,
                        )?;
                    }
                    OpKind::Write => {
                        write_buffer = wait_op(
                            region.write_typed_async(0, std::mem::take(&mut write_buffer))?,
                            wait_mode,
                        )?;
                    }
                }
            }
        }

        println!(
            "[node {}] running {} measured iterations ({} single typed, {} entries = {} words = {} B, wait: {:?})...",
            cluster.my_node,
            iters,
            op,
            entries,
            entries * 8,
            payload_bytes,
            wait_mode
        );
        let mut latencies_ms = Vec::with_capacity(iters);
        let mut hist = LatencyHistogram::new();
        let start_all = Instant::now();

        for _ in 0..iters {
            let t0 = Instant::now();
            match op {
                OpKind::Read => {
                    read_buffer = wait_op(
                        region
                            .read_into_typed_async::<Word8>(0, std::mem::take(&mut read_buffer))?,
                        wait_mode,
                    )?;
                }
                OpKind::Write => {
                    write_buffer = wait_op(
                        region.write_typed_async(0, std::mem::take(&mut write_buffer))?,
                        wait_mode,
                    )?;
                }
            }
            let elapsed = t0.elapsed();
            latencies_ms.push(elapsed.as_secs_f64() * 1000.0);
            hist.record(elapsed);
        }

        let total_time = start_all.elapsed();
        let bench_name = format!(
            "single_{}_typed_{}entries",
            op.to_string().to_lowercase(),
            entries
        );
        hist.print_summary(
            &format!(
                "Single {} Typed ({} entries = {} words = {} B)",
                op,
                entries,
                entries * 8,
                payload_bytes
            ),
            total_time,
            payload_bytes,
        );

        let result = BenchmarkResult {
            benchmark: bench_name.clone(),
            op: op.to_string(),
            is_typed: true,
            iters,
            concurrency: 1,
            entries_per_op: entries,
            payload_bytes_per_op: payload_bytes,
            unit: "milliseconds".to_string(),
            latencies_ms,
        };
        let save_path = output_path.unwrap_or_else(|| format!("{}_latencies.json", bench_name));
        result.save_to_file(&save_path)?;

        synch.synch()?;
    }
    Ok(())
}

/// Wait for one registered-buffer operation per `mode` — parked (the
/// blocking join) or spinning (the floor).
fn wait_rbuf_op<T: RemoteSafe>(op: RBufOp<'_, T>, mode: WaitMode) -> io::Result<()> {
    match mode {
        WaitMode::Park => op.wait(),
        WaitMode::Spin => spin_block_on(op),
    }
}

/// Single multi-word typed (Word8 struct = 8 words = 64 B) Read or
/// Write on a registered buffer (RBuf): the client's buffer is
/// registered once, up front, and every operation posts against the
/// registration directly — no pooled copy, no per-operation hold, the
/// zero-copy counterpart of `run_single_typed` (same payload, same
/// server, one measurement apart).
pub fn run_single_rbuf(
    op: OpKind,
    entries: usize,
    iters: usize,
    warmup: usize,
    output_path: Option<String>,
    wait_mode: WaitMode,
    placement: Placement,
) -> Result<(), Box<dyn std::error::Error>> {
    let cluster = ClusterContext::from_env();
    let mut synch = GlobalSynch::new(&cluster);
    let payload_bytes = entries * std::mem::size_of::<Word8>();

    if cluster.is_server() {
        println!(
            "[node {}] server: registering typed Word8 buffer ({} elements)...",
            cluster.my_node, TYPED_BUFFER_ELEMS
        );
        let owner = SharedMemoryRegionProvider::new(cluster.me().provider_addr());
        let _handle = owner.register_typed(
            REGION_NAME_TYPED,
            vec![Word8::new(0x1234); TYPED_BUFFER_ELEMS],
        );
        owner.serve()?;

        println!("[node {}] server listening, barrier 1...", cluster.my_node);
        synch.synch()?;
        println!(
            "[node {}] server waiting for client to finish, barrier 2...",
            cluster.my_node
        );
        synch.synch()?;
        println!("[node {}] server finished.", cluster.my_node);
    } else {
        println!("[node {}] client: waiting for server...", cluster.my_node);
        synch.synch()?;

        let engine = pinned_client(placement)?;
        let server_node = cluster.node(0);
        let reader = RemoteMemoryProvider::with_engine(server_node.remote_addr(), engine);
        reader.update(0)?;

        let catalog = reader.get_remote_mr_metadata();
        let region_meta = catalog
            .iter()
            .find(|m| m.name == REGION_NAME_TYPED)
            .ok_or("typed region not found in catalog")?;
        let region = reader
            .get_remote_mr(region_meta, Some(0))
            .ok_or("failed to get remote MR")?;

        // The client's buffer, registered once with the group's
        // connection domain and posted against directly ever after:
        // the operation's path has no copy, no pool slice, no hold —
        // the zero-copy half of the single-typed measurement.
        let mut rbuf = reader.register_buffer(0, vec![Word8::new(0x5678); entries])?;

        if warmup > 0 {
            println!(
                "[node {}] running {} warmup iterations...",
                cluster.my_node, warmup
            );
            for _ in 0..warmup {
                let op_future = match op {
                    OpKind::Read => region.read_into_rbuf(0, &mut rbuf)?,
                    OpKind::Write => region.write_rbuf(0, &mut rbuf)?,
                };
                wait_rbuf_op(op_future, wait_mode)?;
            }
        }

        println!(
            "[node {}] running {} measured iterations ({} single typed, {} entries = {} words = {} B, registered buffer, wait: {:?})...",
            cluster.my_node,
            iters,
            op,
            entries,
            entries * 8,
            payload_bytes,
            wait_mode
        );
        let mut latencies_ms = Vec::with_capacity(iters);
        let mut hist = LatencyHistogram::new();
        let start_all = Instant::now();

        for _ in 0..iters {
            let t0 = Instant::now();
            let op_future = match op {
                OpKind::Read => region.read_into_rbuf(0, &mut rbuf)?,
                OpKind::Write => region.write_rbuf(0, &mut rbuf)?,
            };
            wait_rbuf_op(op_future, wait_mode)?;
            let elapsed = t0.elapsed();
            latencies_ms.push(elapsed.as_secs_f64() * 1000.0);
            hist.record(elapsed);
        }

        let total_time = start_all.elapsed();
        let bench_name = format!(
            "single_{}_rbuf_{}entries",
            op.to_string().to_lowercase(),
            entries
        );
        hist.print_summary(
            &format!(
                "Single {} Typed, registered buffer ({} entries = {} words = {} B)",
                op,
                entries,
                entries * 8,
                payload_bytes
            ),
            total_time,
            payload_bytes,
        );

        let result = BenchmarkResult {
            benchmark: bench_name.clone(),
            op: op.to_string(),
            is_typed: true,
            iters,
            concurrency: 1,
            entries_per_op: entries,
            payload_bytes_per_op: payload_bytes,
            unit: "milliseconds".to_string(),
            latencies_ms,
        };
        let save_path = output_path.unwrap_or_else(|| format!("{}_latencies.json", bench_name));
        result.save_to_file(&save_path)?;

        synch.synch()?;
    }
    Ok(())
}

/// Concurrent untyped Read or Write: N concurrent operations (16 or 32),
/// 1 word (8 bytes) per operation, each targeting an independent section.
/// All concurrent operations waited together.
pub fn run_concurrent_untyped(
    op: OpKind,
    concurrency: usize,
    iters: usize,
    warmup: usize,
    output_path: Option<String>,
    placement: Placement,
) -> Result<(), Box<dyn std::error::Error>> {
    let cluster = ClusterContext::from_env();
    let mut synch = GlobalSynch::new(&cluster);
    const WORD_BYTES: usize = 8;

    assert!(
        concurrency * SECTION_SIZE_BYTES <= UNTYPED_BUFFER_BYTES,
        "concurrency {concurrency} exceeds buffer capacity"
    );

    if cluster.is_server() {
        println!(
            "[node {}] server: registering untyped buffer ({} MB)...",
            cluster.my_node,
            UNTYPED_BUFFER_BYTES / (1024 * 1024)
        );
        let owner = SharedMemoryRegionProvider::new(cluster.me().provider_addr());
        let _handle = owner.register_typed(REGION_NAME_UNTYPED, vec![0xABu8; UNTYPED_BUFFER_BYTES]);
        owner.serve()?;

        println!("[node {}] server listening, barrier 1...", cluster.my_node);
        synch.synch()?;
        println!(
            "[node {}] server waiting for client to finish, barrier 2...",
            cluster.my_node
        );
        synch.synch()?;
        println!("[node {}] server finished.", cluster.my_node);
    } else {
        println!("[node {}] client: waiting for server...", cluster.my_node);
        synch.synch()?;

        let engine = pinned_client(placement)?;
        let server_node = cluster.node(0);
        let reader = RemoteMemoryProvider::with_engine(server_node.remote_addr(), engine);
        reader.update(0)?;

        let catalog = reader.get_remote_mr_metadata();
        let region_meta = catalog
            .iter()
            .find(|m| m.name == REGION_NAME_UNTYPED)
            .ok_or("untyped region not found in catalog")?;
        let region = reader
            .get_remote_mr(region_meta, Some(0))
            .ok_or("failed to get remote MR")?;

        if warmup > 0 {
            println!(
                "[node {}] running {} warmup iterations (concurrency: {})...",
                cluster.my_node, warmup, concurrency
            );
            for _ in 0..warmup {
                match op {
                    OpKind::Read => {
                        let ops = (0..concurrency)
                            .map(|i| region.read_async((i * SECTION_SIZE_BYTES) as u64, WORD_BYTES))
                            .collect::<io::Result<Vec<_>>>()?;
                        let _ = futures::executor::block_on(futures::future::join_all(ops));
                    }
                    OpKind::Write => {
                        let ops = (0..concurrency)
                            .map(|i| {
                                region.write_async(
                                    (i * SECTION_SIZE_BYTES) as u64,
                                    vec![0x55u8; WORD_BYTES],
                                )
                            })
                            .collect::<io::Result<Vec<_>>>()?;
                        let _ = futures::executor::block_on(futures::future::join_all(ops));
                    }
                }
            }
        }

        println!(
            "[node {}] running {} measured iterations (concurrent {} untyped, concurrency: {}, 1 word per op)...",
            cluster.my_node, iters, op, concurrency
        );
        let mut latencies_ms = Vec::with_capacity(iters);
        let mut hist = LatencyHistogram::new();
        let start_all = Instant::now();

        for _ in 0..iters {
            let t0 = Instant::now();
            match op {
                OpKind::Read => {
                    let ops = (0..concurrency)
                        .map(|i| region.read_async((i * SECTION_SIZE_BYTES) as u64, WORD_BYTES))
                        .collect::<io::Result<Vec<_>>>()?;
                    let _ = futures::executor::block_on(futures::future::join_all(ops));
                }
                OpKind::Write => {
                    let ops = (0..concurrency)
                        .map(|i| {
                            region.write_async(
                                (i * SECTION_SIZE_BYTES) as u64,
                                vec![0x55u8; WORD_BYTES],
                            )
                        })
                        .collect::<io::Result<Vec<_>>>()?;
                    let _ = futures::executor::block_on(futures::future::join_all(ops));
                }
            }
            let elapsed = t0.elapsed();
            latencies_ms.push(elapsed.as_secs_f64() * 1000.0);
            hist.record(elapsed);
        }

        let total_time = start_all.elapsed();
        let bench_name = format!(
            "concurrent_{}_untyped_c{}",
            op.to_string().to_lowercase(),
            concurrency
        );
        hist.print_summary(
            &format!(
                "Concurrent {} Untyped ({} concurrent ops, 1 word = 8 B each)",
                op, concurrency
            ),
            total_time,
            WORD_BYTES * concurrency,
        );

        let result = BenchmarkResult {
            benchmark: bench_name.clone(),
            op: op.to_string(),
            is_typed: false,
            iters,
            concurrency,
            entries_per_op: 1,
            payload_bytes_per_op: WORD_BYTES,
            unit: "milliseconds".to_string(),
            latencies_ms,
        };
        let save_path = output_path.unwrap_or_else(|| format!("{}_latencies.json", bench_name));
        result.save_to_file(&save_path)?;

        synch.synch()?;
    }
    Ok(())
}

/// Concurrent typed Read or Write: N concurrent operations (16 or 32),
/// 1 entry of Word8 (64 bytes) per operation, each targeting an independent section.
/// All concurrent operations waited together.
pub fn run_concurrent_typed(
    op: OpKind,
    concurrency: usize,
    iters: usize,
    warmup: usize,
    output_path: Option<String>,
    placement: Placement,
) -> Result<(), Box<dyn std::error::Error>> {
    let cluster = ClusterContext::from_env();
    let mut synch = GlobalSynch::new(&cluster);
    const ELEM_BYTES: usize = std::mem::size_of::<Word8>();

    assert!(
        concurrency * SECTION_SIZE_ELEMS <= TYPED_BUFFER_ELEMS,
        "concurrency {concurrency} exceeds buffer capacity"
    );

    if cluster.is_server() {
        println!(
            "[node {}] server: registering typed Word8 buffer ({} elements)...",
            cluster.my_node, TYPED_BUFFER_ELEMS
        );
        let owner = SharedMemoryRegionProvider::new(cluster.me().provider_addr());
        let _handle = owner.register_typed(
            REGION_NAME_TYPED,
            vec![Word8::new(0x1234); TYPED_BUFFER_ELEMS],
        );
        owner.serve()?;

        println!("[node {}] server listening, barrier 1...", cluster.my_node);
        synch.synch()?;
        println!(
            "[node {}] server waiting for client to finish, barrier 2...",
            cluster.my_node
        );
        synch.synch()?;
        println!("[node {}] server finished.", cluster.my_node);
    } else {
        println!("[node {}] client: waiting for server...", cluster.my_node);
        synch.synch()?;

        let engine = pinned_client(placement)?;
        let server_node = cluster.node(0);
        let reader = RemoteMemoryProvider::with_engine(server_node.remote_addr(), engine);
        reader.update(0)?;

        let catalog = reader.get_remote_mr_metadata();
        let region_meta = catalog
            .iter()
            .find(|m| m.name == REGION_NAME_TYPED)
            .ok_or("typed region not found in catalog")?;
        let region = reader
            .get_remote_mr(region_meta, Some(0))
            .ok_or("failed to get remote MR")?;

        let write_payload = vec![Word8::new(0x9999); 1];

        if warmup > 0 {
            println!(
                "[node {}] running {} warmup iterations (concurrency: {})...",
                cluster.my_node, warmup, concurrency
            );
            for _ in 0..warmup {
                match op {
                    OpKind::Read => {
                        let ops = (0..concurrency)
                            .map(|i| {
                                region.read_typed_async::<Word8>((i * SECTION_SIZE_ELEMS) as u64, 1)
                            })
                            .collect::<io::Result<Vec<_>>>()?;
                        let _ = futures::executor::block_on(futures::future::join_all(ops));
                    }
                    OpKind::Write => {
                        let ops = (0..concurrency)
                            .map(|i| {
                                region.write_typed_async(
                                    (i * SECTION_SIZE_ELEMS) as u64,
                                    write_payload.clone(),
                                )
                            })
                            .collect::<io::Result<Vec<_>>>()?;
                        let _ = futures::executor::block_on(futures::future::join_all(ops));
                    }
                }
            }
        }

        println!(
            "[node {}] running {} measured iterations (concurrent {} typed, concurrency: {}, 1 entry = 8 words = {} B per op)...",
            cluster.my_node, iters, op, concurrency, ELEM_BYTES
        );
        let mut latencies_ms = Vec::with_capacity(iters);
        let mut hist = LatencyHistogram::new();
        let start_all = Instant::now();

        for _ in 0..iters {
            let t0 = Instant::now();
            match op {
                OpKind::Read => {
                    let ops = (0..concurrency)
                        .map(|i| {
                            region.read_typed_async::<Word8>((i * SECTION_SIZE_ELEMS) as u64, 1)
                        })
                        .collect::<io::Result<Vec<_>>>()?;
                    let _ = futures::executor::block_on(futures::future::join_all(ops));
                }
                OpKind::Write => {
                    let ops = (0..concurrency)
                        .map(|i| {
                            region.write_typed_async(
                                (i * SECTION_SIZE_ELEMS) as u64,
                                write_payload.clone(),
                            )
                        })
                        .collect::<io::Result<Vec<_>>>()?;
                    let _ = futures::executor::block_on(futures::future::join_all(ops));
                }
            }
            let elapsed = t0.elapsed();
            latencies_ms.push(elapsed.as_secs_f64() * 1000.0);
            hist.record(elapsed);
        }

        let total_time = start_all.elapsed();
        let bench_name = format!(
            "concurrent_{}_typed_c{}",
            op.to_string().to_lowercase(),
            concurrency
        );
        hist.print_summary(
            &format!(
                "Concurrent {} Typed ({} concurrent ops, 1 entry = 8 words = {} B each)",
                op, concurrency, ELEM_BYTES
            ),
            total_time,
            ELEM_BYTES * concurrency,
        );

        let result = BenchmarkResult {
            benchmark: bench_name.clone(),
            op: op.to_string(),
            is_typed: true,
            iters,
            concurrency,
            entries_per_op: 1,
            payload_bytes_per_op: ELEM_BYTES,
            unit: "milliseconds".to_string(),
            latencies_ms,
        };
        let save_path = output_path.unwrap_or_else(|| format!("{}_latencies.json", bench_name));
        result.save_to_file(&save_path)?;

        synch.synch()?;
    }
    Ok(())
}
