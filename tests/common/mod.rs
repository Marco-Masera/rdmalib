//! Cluster-layout helpers shared by the integration tests.
//!
//! `scripts/test_runner.py` deploys a test binary to the nodes configured
//! for it in `tests/_test_config.json` and launches it on each node with
//! two environment variables:
//!
//! - [`ENV_NODES`]: comma-separated `<ip>:<tcp-port>:<rdma-port>`, one
//!   entry per deployed node, in config order (IPv4 only).
//! - [`ENV_MY_NODE`]: the index of the node this process runs on.
//!
//! [`TestCluster::from_env`] parses both into a [`TestCluster`], whose
//! [`NodeInfo`] entries also build the provider/reader addresses of each
//! node.
//!
//! [`GlobalSynch`] is a barrier across the deployed nodes: every node
//! builds one from the same [`TestCluster`], and each of its
//! [`GlobalSynch::synch`] calls blocks until every other node has
//! called it as many times — tests hold their phases apart with it (a
//! reader connects only after the owner serves, say).

use std::env;
use std::fmt;
use std::io::{self, Read, Write};
use std::net::{IpAddr, Ipv4Addr, SocketAddr, SocketAddrV4, TcpListener, TcpStream};
use std::process::Command;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

use rdmalib::{RemoteMemoryProviderAddr, SharedMemoryRegionProviderAddr};

pub const ENV_NODES: &str = "RDMALIB_TEST_NODES";
pub const ENV_MY_NODE: &str = "RDMALIB_TEST_MY_NODE";

/// One deployed node: where it is and which ports its test process uses.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NodeInfo {
    pub index: usize,
    pub ip: String,
    pub tcp_port: u16,
    pub rdma_port: u16,
}

impl NodeInfo {
    /// Server-side address: a provider listening on this node.
    pub fn provider_addr(&self) -> SharedMemoryRegionProviderAddr {
        SharedMemoryRegionProviderAddr::new(self.rdma_port, self.tcp_port)
    }

    /// Client-side address: a reader connecting to this node from elsewhere.
    pub fn remote_addr(&self) -> RemoteMemoryProviderAddr {
        RemoteMemoryProviderAddr::new(self.ip.clone(), self.rdma_port, self.tcp_port)
    }
}

impl fmt::Display for NodeInfo {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} (tcp {}, rdma {})", self.ip, self.tcp_port, self.rdma_port)
    }
}

/// The nodes a test was deployed on, and which of them this process is.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TestCluster {
    pub my_node: usize,
    pub nodes: Vec<NodeInfo>,
}

impl TestCluster {
    /// Reads and parses the runner-provided environment variables.
    ///
    /// Panics with an explanation when they are absent, i.e. when the test
    /// binary was started outside `scripts/test_runner.py`.
    pub fn from_env() -> TestCluster {
        let nodes = env::var(ENV_NODES).unwrap_or_else(|_| {
            panic!("{ENV_NODES} is not set: cluster tests must run under scripts/test_runner.py")
        });
        let my_node = env::var(ENV_MY_NODE).unwrap_or_else(|_| {
            panic!("{ENV_MY_NODE} is not set: cluster tests must run under scripts/test_runner.py")
        });
        Self::parse(&my_node, &nodes)
            .unwrap_or_else(|e| panic!("invalid {ENV_MY_NODE}/{ENV_NODES} from test runner: {e}"))
    }

    /// Parses the runner format: `my_node` is a decimal index, `nodes` a
    /// comma-separated list of `<ip>:<tcp-port>:<rdma-port>`.
    pub fn parse(my_node: &str, nodes: &str) -> Result<TestCluster, String> {
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
        Ok(TestCluster {
            my_node,
            nodes: parsed,
        })
    }

    /// The node this process runs on.
    pub fn me(&self) -> &NodeInfo {
        &self.nodes[self.my_node]
    }

    /// The node with the given index; panics with a hint if the test was
    /// configured with fewer nodes than it uses.
    pub fn node(&self, index: usize) -> &NodeInfo {
        self.nodes.get(index).unwrap_or_else(|| {
            panic!(
                "test uses node {index} but only {} node(s) were deployed",
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

/// How long one node waits between rounds of polling the others.
const SYNCH_POLL_INTERVAL: Duration = Duration::from_millis(10);
/// How long one node waits for another's TCP handshake or reply.
const SYNCH_IO_TIMEOUT: Duration = Duration::from_millis(1000);
/// How long a [`GlobalSynch::synch`] may take by default before the
/// process exits with an error.
const SYNCH_TIMEOUT: Duration = Duration::from_secs(60);

/// A barrier across the deployed nodes, built from the same
/// [`TestCluster`] on every node.
///
/// Each [`Self::synch`] call blocks until every other node has called
/// `synch` at least as many times as this one has: tests hold their
/// phases apart with it (a reader connects only after the owner
/// serves, say).
///
/// The protocol is a distributed clock. `synch` raises this node's
/// clock, then repeatedly exchanges clocks with the other nodes — a
/// poll carries the poller's clock, the reply the polled node's —
/// until every peer has been heard from with a clock at least this
/// high. One exchange informs both sides, and a node can only leave a
/// barrier through exchanges that already left its own clock with
/// every peer: no peer ever still needs to hear from a node that has
/// left. That is what makes it safe to listen only while inside
/// `synch` itself — a refused connection is not an error, only a node
/// that has not entered the barrier yet.
///
/// Every node listens on one agreed port — the smallest node TCP port
/// in the cluster minus one, below every port the metadata channels
/// use — on its own address; [`Self::with_port`] overrides the choice.
/// Nothing binds outside of `synch`.
///
/// A barrier that cannot resolve — a node crashed before entering it,
/// say — would block every node forever; instead a `synch` that runs
/// past its deadline (60 seconds by default) ends the process with an
/// error and a diagnostic naming the nodes it was still waiting for.
/// [`Self::timeout`] overrides the deadline at creation.
pub struct GlobalSynch {
    /// The IPv4 addresses of the other nodes, in cluster order.
    peers: Vec<Ipv4Addr>,
    /// This node's IPv4 address, as the other nodes reach it.
    my_ip: Ipv4Addr,
    /// The port every node's [`Self::synch`] listens on.
    port: u16,
    /// How long one [`Self::synch`] may take before the process exits.
    timeout: Duration,
    /// How many times this node has entered a barrier.
    clock: u64,
    /// The highest clock heard from each peer, by `peers` position.
    known_clocks: Vec<u64>,
}

impl GlobalSynch {
    /// Build the barrier over `cluster`, every node listening on the
    /// smallest node TCP port minus one.
    ///
    /// # Panics
    ///
    /// Panics when the cluster has no nodes, when its smallest TCP
    /// port leaves no port below it, or (via [`Self::with_port`]) when
    /// a node address is not IPv4.
    pub fn new(cluster: &TestCluster) -> Self {
        let smallest = cluster
            .nodes
            .iter()
            .map(|node| node.tcp_port)
            .min()
            .expect("a cluster of at least one node");
        assert!(
            smallest > 1,
            "no port below the smallest node TCP port {smallest}"
        );
        Self::with_port(cluster, smallest - 1)
    }

    /// Build the barrier over `cluster`, every node listening on
    /// `port` instead of the derived one.
    ///
    /// # Panics
    ///
    /// Panics when a node address is not IPv4.
    pub fn with_port(cluster: &TestCluster, port: u16) -> Self {
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
            known_clocks: vec![0; cluster.len() - 1],
        }
    }

    /// The port every node's [`Self::synch`] listens on.
    pub fn port(&self) -> u16 {
        self.port
    }

    /// Override how long a [`Self::synch`] may take: one that does not
    /// resolve within `timeout` ends the process with an error, 60
    /// seconds unless this is called. A consuming builder:
    ///
    /// ```ignore
    /// GlobalSynch::new(&cluster).timeout(Duration::from_secs(5))
    /// ```
    pub fn timeout(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
        self
    }

    /// Enter the next barrier: block until every other node has called
    /// [`Self::synch`] as many times as this one has now.
    ///
    /// The listener lives exactly as long as the call: nodes not in a
    /// barrier of their own refuse the connection, which the poller
    /// reads as "not there yet" and retries until they do. Only a
    /// failure to serve — the port not bindable on this node — is
    /// reported as `Err`. A call that outlives its deadline (see
    /// [`Self::timeout`]) never returns: the process exits with an
    /// error, since a peer that crashed would otherwise hold every
    /// node here forever.
    pub fn synch(&mut self) -> io::Result<()> {
        self.clock += 1;
        let round = self.clock;
        if self.peers.is_empty() {
            return Ok(());
        }
        let listener = TcpListener::bind(SocketAddrV4::new(self.my_ip, self.port))?;
        listener.set_nonblocking(true)?;

        // Answer the peers polling this node on a thread of our own,
        // while this one polls them: a node that answers only between
        // its own polls deadlocks against an equally paced peer — each
        // mid-poll, neither accepting, both until the reply timeout.
        let (seen_tx, seen_rx) = mpsc::channel::<(IpAddr, u64)>();
        let shutdown = Arc::new(AtomicBool::new(false));
        let my_clock = self.clock;
        let responder = {
            let shutdown = Arc::clone(&shutdown);
            thread::spawn(move || {
                // Polling accept: closing a listener does not reliably
                // wake a blocked accept on Linux, so the flag is
                // checked between attempts.
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
            // Record the clocks the responder has heard — their polls
            // are evidence about them, the same evidence a poll of
            // ours would have gathered.
            while let Ok((ip, clock)) = seen_rx.try_recv() {
                if let Some(index) = self.peers.iter().position(|&peer| IpAddr::from(peer) == ip) {
                    self.known_clocks[index] = self.known_clocks[index].max(clock);
                }
            }
            // Then poll every peer this node has not heard from in
            // this round. A failed poll is never an error: the peer
            // has not entered its barrier yet.
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
            // A barrier that cannot resolve — a node crashed before
            // entering it, say — would wait forever; the deadline turns
            // that into a loud, failing exit instead.
            if Instant::now() >= deadline {
                self.bail_out(round);
            }
            thread::sleep(SYNCH_POLL_INTERVAL);
        }

        shutdown.store(true, Ordering::Relaxed);
        let _ = responder.join();
        Ok(())
    }

    /// End the process: this barrier did not resolve within the
    /// timeout, and the peers still missing — with the last clock
    /// heard from each — are the diagnostic of which node it was.
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
             still waiting for: {}. A node may have crashed or be stuck; exiting.",
            self.my_ip,
            self.port,
            self.timeout,
            waiting.join(", ")
        );
        std::process::exit(1)
    }

    /// Poll one peer: send this node's clock and read the peer's.
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

/// Answer one poll: read the poller's clock, report it back to the
/// node's own bookkeeping, and send the node's clock in return.
///
/// Runs on the responder thread of one [`GlobalSynch::synch`] call;
/// `my_clock` is the clock fixed for that round. The poller's clock is
/// reported before the reply is sent, so a poller that has read a
/// reply can rely on its target having already heard its own clock.
fn answer(mut stream: TcpStream, my_clock: u64, seen: &mpsc::Sender<(IpAddr, u64)>) -> io::Result<()> {
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

#[test]
fn parses_the_runner_format() {
    let cluster = TestCluster::parse("1", "192.168.4.1:8100:9100,192.168.4.3:8101:9101").unwrap();
    assert_eq!(cluster.len(), 2);
    assert_eq!(cluster.my_node, 1);
    assert_eq!(cluster.me().ip, "192.168.4.3");
    assert_eq!(cluster.me().tcp_port, 8101);
    assert_eq!(cluster.node(0).rdma_port, 9100);
    assert_eq!(cluster.others().map(|n| n.index).collect::<Vec<_>>(), vec![0]);
    assert_eq!(
        cluster.me().provider_addr(),
        SharedMemoryRegionProviderAddr::new(9101, 8101)
    );
}

#[test]
fn rejects_malformed_input() {
    assert!(TestCluster::parse("0", "").is_err());
    assert!(TestCluster::parse("0", "192.168.4.1:8100").is_err());
    assert!(TestCluster::parse("0", "192.168.4.1:x:9100").is_err());
    assert!(TestCluster::parse("2", "192.168.4.1:8100:9100").is_err());
    assert!(TestCluster::parse("node0", "192.168.4.1:8100:9100").is_err());
}

#[test]
fn synch_blocks_until_every_node_has_called_it() {
    let cluster = TestCluster::parse(
        "0",
        "127.0.0.1:18100:19100,127.0.0.2:18101:19101,127.0.0.3:18102:19102",
    )
    .unwrap();
    let mut mine = GlobalSynch::new(&cluster);
    assert_eq!(mine.port(), 18100 - 1);

    // The other nodes enter the barrier late (127.0.0.2/3 stand in for
    // the other machines: the whole 127/8 range is local), so this
    // node's synch cannot return before the latest of them.
    let began = Instant::now();
    let mut joins = Vec::new();
    for (node, delay) in [(1, 100), (2, 300)] {
        let cluster = TestCluster {
            my_node: node,
            nodes: cluster.nodes.clone(),
        };
        joins.push(thread::spawn(move || {
            thread::sleep(Duration::from_millis(delay));
            let mut synch = GlobalSynch::new(&cluster);
            synch.synch().unwrap();
        }));
    }
    mine.synch().unwrap();
    let elapsed = began.elapsed();
    assert!(
        elapsed >= Duration::from_millis(300),
        "the barrier ended after {elapsed:?}, before the 300ms node entered it"
    );
    for join in joins {
        join.join().unwrap();
    }
}

#[test]
fn synch_holds_each_round_until_every_node_enters_it() {
    let cluster = TestCluster::parse(
        "0",
        "127.0.0.1:18200:19200,127.0.0.2:18201:19201,127.0.0.3:18202:19202",
    )
    .unwrap();
    // A manual port, and one deliberately unlike the derived one.
    let mut mine = GlobalSynch::with_port(&cluster, 18500);
    assert_eq!(mine.port(), 18500);

    // Node 1 runs two back-to-back rounds; node 2 pauses between its
    // rounds, unreachable meanwhile. The second barrier must not close
    // before node 2 enters it — and every round re-binds the listener.
    let node1 = TestCluster {
        my_node: 1,
        nodes: cluster.nodes.clone(),
    };
    let node2 = TestCluster {
        my_node: 2,
        nodes: cluster.nodes.clone(),
    };
    let node2_entered_round2 = Arc::new(Mutex::new(None::<Instant>));

    let join1 = thread::spawn(move || {
        let mut synch = GlobalSynch::with_port(&node1, 18500);
        synch.synch().unwrap();
        synch.synch().unwrap();
    });
    let entered = Arc::clone(&node2_entered_round2);
    let join2 = thread::spawn(move || {
        let mut synch = GlobalSynch::with_port(&node2, 18500);
        synch.synch().unwrap();
        thread::sleep(Duration::from_millis(150));
        *entered.lock().unwrap() = Some(Instant::now());
        synch.synch().unwrap();
    });

    mine.synch().unwrap();
    mine.synch().unwrap();

    let node2_entered = node2_entered_round2
        .lock()
        .unwrap()
        .expect("node 2 entered round 2");
    let left_round2 = Instant::now();
    assert!(
        left_round2 >= node2_entered,
        "the second barrier closed before node 2 entered it"
    );
    join1.join().unwrap();
    join2.join().unwrap();
}

#[test]
fn synch_of_a_lone_node_returns_at_once() {
    let cluster = TestCluster::parse("0", "127.0.0.1:18300:19300").unwrap();
    let mut synch = GlobalSynch::new(&cluster);
    synch.synch().unwrap();
    synch.synch().unwrap();
}

/// Set by the parent run of the test below on its child: the child is
/// the one that hangs in the barrier and must be ended by the deadline.
const ENV_SYNCH_TIMEOUT_CHILD: &str = "RDMALIB_TEST_SYNCH_TIMEOUT_CHILD";

#[test]
fn synch_exits_with_error_when_a_node_never_arrives() {
    if env::var(ENV_SYNCH_TIMEOUT_CHILD).is_ok() {
        // Child role: the other node never enters the barrier, so only
        // the deadline can end this — with the process, not a return.
        let cluster =
            TestCluster::parse("0", "127.0.0.1:18400:19400,127.0.0.2:18401:19401").unwrap();
        let mut synch = GlobalSynch::new(&cluster).timeout(Duration::from_millis(100));
        synch.synch().unwrap();
        unreachable!("the deadline must have exited the process");
    }

    // Parent role: the exit is the feature under test, so it cannot
    // run in-process — rerun this very test as a child process and
    // watch it die.
    let output = Command::new(env::current_exe().unwrap())
        .args([
            "--exact",
            "common::synch_exits_with_error_when_a_node_never_arrives",
            "--nocapture",
        ])
        .env(ENV_SYNCH_TIMEOUT_CHILD, "1")
        .output()
        .unwrap();
    assert_eq!(
        output.status.code(),
        Some(1),
        "the deadline must exit with an error: {output:?}"
    );
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("127.0.0.2"),
        "the diagnostic must name the missing node: {stderr}"
    );
}
