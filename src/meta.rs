//! The metadata exchange between the two sides of the library.
//!
//! The region catalog and the (remote_addr, size, rkey) tuples travel
//! over a plain TCP connection — the control plane — while the data
//! plane stays RDMA. This module owns the wire protocol: the framing
//! (`[len][type][payload]`, little-endian, [`MAX_FRAME`] capped), the
//! messages of the session conversation, and [`Channel`], a TCP
//! stream speaking them. It is deliberately free of RDMA knowledge:
//! the sessions of [`SharedMemoryRegionProvider`](crate::SharedMemoryRegionProvider)
//! and [`RemoteMemoryProvider`](crate::RemoteMemoryProvider) drive
//! it, each side interleaving the RDMA rendezvous where its half of
//! the connection requires it.
//!
//! A session: the reader connects, greets ([`Message::Hello`]), and
//! asks to join a group ([`Message::WantGroup`]) — then connects over
//! RDMA, which the provider accepts into that group's protection
//! domain; the two blocking calls are each other's signal. The
//! provider answers with the catalog ([`Message::Metadata`]) and the
//! group's tuples ([`Message::Tuples`]). The connection stays open:
//! an [`Message::Update`] re-sends both, picking up regions
//! registered since.

use std::io::{self, Read, Write};
use std::net::{TcpStream, ToSocketAddrs};
use std::time::Duration;

/// The protocol version this library speaks; a mismatching greeting
/// ends the session.
pub(crate) const PROTOCOL_VERSION: u16 = 1;

/// Largest accepted frame, header included. Catalogs are small, so a
/// length beyond this is a broken or hostile peer, rejected before
/// its payload is allocated.
const MAX_FRAME: usize = 1 << 20;

/// How long a channel waits for any part of a message: the control
/// plane exchanges small messages, so a peer that stalls this long is
/// gone for practical purposes.
const IO_TIMEOUT: Duration = Duration::from_secs(10);

// Message type codes on the wire; the gap before the next value is
// room for the future (revocation, errors, goodbye).
const MSG_HELLO: u8 = 1;
const MSG_WELCOME: u8 = 2;
const MSG_WANT_GROUP: u8 = 3;
const MSG_UPDATE: u8 = 4;
const MSG_METADATA: u8 = 5;
const MSG_TUPLES: u8 = 6;

/// One message of the metadata exchange.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Message {
    /// Reader → provider: greet, announcing a protocol version.
    Hello {
        version: u16,
    },
    /// Provider → reader: the greeting's reply, echoing the version
    /// the provider speaks.
    Welcome {
        version: u16,
    },
    /// Reader → provider: join `group`, whose tuples the reader
    /// wants; the reader's RDMA connection follows immediately.
    WantGroup {
        group: u32,
    },
    /// Reader → provider: re-send the catalog and the session group's
    /// tuples.
    Update,
    /// Provider → reader: the region catalog, every registered
    /// region.
    Metadata {
        regions: Vec<RegionDesc>,
    },
    /// Provider → reader: the tuples of `group`, one per region.
    Tuples {
        group: u32,
        tuples: Vec<TupleDesc>,
    },
}

/// A region as advertised in the catalog.
///
/// The element layout (`elem_size`, `elem_align`) is the part a
/// reader can check a typed read against; `elem_type` names the type
/// the sharing side registered, for diagnostics only — type identity
/// across separately compiled applications cannot be verified.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct RegionDesc {
    /// Identifier of the region on the sharing machine.
    pub(crate) id: u32,
    /// Human-readable name of the region; need not be unique.
    pub(crate) name: String,
    /// Size of the region's buffer, in bytes.
    pub(crate) size: u64,
    /// `size_of` of the element type the region is shared as.
    pub(crate) elem_size: u32,
    /// `align_of` of the element type the region is shared as.
    pub(crate) elem_align: u32,
    /// `std::any::type_name` of the element type, for diagnostics.
    pub(crate) elem_type: String,
}

/// One (remote_addr, size, rkey) tuple, of the region `region_id`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct TupleDesc {
    /// The region the tuple reaches.
    pub(crate) region_id: u32,
    /// Address of the region in the sharing machine's address space.
    pub(crate) remote_addr: u64,
    /// Size of the registered memory, in bytes.
    pub(crate) size: u64,
    /// Remote key required to access the region with RDMA.
    pub(crate) rkey: u32,
}

impl Message {
    /// The message as one framed wire image: `[len][type][payload]`,
    /// `len` counting the type byte, all integers little-endian.
    pub(crate) fn encode(&self) -> Vec<u8> {
        let mut payload = Vec::new();
        let ty = match self {
            Message::Hello { version } => {
                put_u16(&mut payload, *version);
                MSG_HELLO
            }
            Message::Welcome { version } => {
                put_u16(&mut payload, *version);
                MSG_WELCOME
            }
            Message::WantGroup { group } => {
                put_u32(&mut payload, *group);
                MSG_WANT_GROUP
            }
            Message::Update => MSG_UPDATE,
            Message::Metadata { regions } => {
                put_u32(&mut payload, regions.len() as u32);
                for region in regions {
                    put_u32(&mut payload, region.id);
                    put_str(&mut payload, &region.name);
                    put_u64(&mut payload, region.size);
                    put_u32(&mut payload, region.elem_size);
                    put_u32(&mut payload, region.elem_align);
                    put_str(&mut payload, &region.elem_type);
                }
                MSG_METADATA
            }
            Message::Tuples { group, tuples } => {
                put_u32(&mut payload, *group);
                put_u32(&mut payload, tuples.len() as u32);
                for tuple in tuples {
                    put_u32(&mut payload, tuple.region_id);
                    put_u64(&mut payload, tuple.remote_addr);
                    put_u64(&mut payload, tuple.size);
                    put_u32(&mut payload, tuple.rkey);
                }
                MSG_TUPLES
            }
        };
        let mut frame = Vec::with_capacity(5 + payload.len());
        put_u32(&mut frame, 1 + payload.len() as u32);
        frame.push(ty);
        frame.extend_from_slice(&payload);
        frame
    }
}

/// Decode the message of type `ty` from its `payload`.
///
/// The payload must be consumed exactly: leftover bytes are a
/// protocol violation, as are counts and lengths reaching past the
/// payload (read eagerly, so a bogus count fails on its first element
/// instead of allocating for it).
pub(crate) fn decode(ty: u8, payload: &[u8]) -> io::Result<Message> {
    let mut r = Reader {
        buf: payload,
        pos: 0,
    };
    let msg = match ty {
        MSG_HELLO => Message::Hello { version: r.u16()? },
        MSG_WELCOME => Message::Welcome {
            version: r.u16()?,
        },
        MSG_WANT_GROUP => Message::WantGroup { group: r.u32()? },
        MSG_UPDATE => Message::Update,
        MSG_METADATA => {
            let count = r.u32()? as usize;
            let mut regions = Vec::new();
            for _ in 0..count {
                let id = r.u32()?;
                let name = r.string()?;
                let size = r.u64()?;
                let elem_size = r.u32()?;
                let elem_align = r.u32()?;
                let elem_type = r.string()?;
                regions.push(RegionDesc {
                    id,
                    name,
                    size,
                    elem_size,
                    elem_align,
                    elem_type,
                });
            }
            Message::Metadata { regions }
        }
        MSG_TUPLES => {
            let group = r.u32()?;
            let count = r.u32()? as usize;
            let mut tuples = Vec::new();
            for _ in 0..count {
                let region_id = r.u32()?;
                let remote_addr = r.u64()?;
                let size = r.u64()?;
                let rkey = r.u32()?;
                tuples.push(TupleDesc {
                    region_id,
                    remote_addr,
                    size,
                    rkey,
                });
            }
            Message::Tuples { group, tuples }
        }
        _ => return Err(bad(format!("unknown message type {ty}"))),
    };
    r.done()?;
    Ok(msg)
}

/// Read one frame from `stream` and decode it.
pub(crate) fn read_frame(stream: &mut impl Read) -> io::Result<Message> {
    let mut header = [0u8; 5];
    stream.read_exact(&mut header)?;
    let len = u32::from_le_bytes([header[0], header[1], header[2], header[3]]) as usize;
    if len == 0 || len > MAX_FRAME {
        return Err(bad(format!("frame length {len} is out of range")));
    }
    let mut payload = vec![0u8; len - 1];
    stream.read_exact(&mut payload)?;
    decode(header[4], &payload)
}

/// Encode `msg` and write its frame to `stream`.
pub(crate) fn write_frame(stream: &mut impl Write, msg: &Message) -> io::Result<()> {
    stream.write_all(&msg.encode())
}

/// A TCP connection speaking the metadata protocol.
///
/// Both sides of the library hold one for the lifetime of a session.
/// The reader keeps it open after the initial exchange, so it can
/// re-request the catalog ([`Message::Update`]) — and, in the future,
/// receive provider-side notifications.
#[derive(Debug)]
pub(crate) struct Channel {
    stream: TcpStream,
}

impl Channel {
    /// Adopt an established TCP stream, arming the I/O timeouts.
    pub(crate) fn new(stream: TcpStream) -> io::Result<Self> {
        stream.set_read_timeout(Some(IO_TIMEOUT))?;
        stream.set_write_timeout(Some(IO_TIMEOUT))?;
        Ok(Self { stream })
    }

    /// Connect to the metadata port of a provider.
    pub(crate) fn connect<A: ToSocketAddrs>(addr: A) -> io::Result<Self> {
        Self::new(TcpStream::connect(addr)?)
    }

    /// Send one message.
    pub(crate) fn send(&mut self, msg: &Message) -> io::Result<()> {
        write_frame(&mut self.stream, msg)
    }

    /// Receive one message.
    pub(crate) fn receive(&mut self) -> io::Result<Message> {
        read_frame(&mut self.stream)
    }
}

/// A protocol violation as an `io::Error`.
fn bad(msg: impl Into<String>) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, msg.into())
}

/// A cursor over a received payload.
struct Reader<'a> {
    buf: &'a [u8],
    pos: usize,
}

impl<'a> Reader<'a> {
    /// The next `len` bytes, advancing past them.
    fn take(&mut self, len: usize) -> io::Result<&'a [u8]> {
        if len > self.buf.len() - self.pos {
            return Err(bad("the payload ends mid-message"));
        }
        let bytes = &self.buf[self.pos..self.pos + len];
        self.pos += len;
        Ok(bytes)
    }

    fn u16(&mut self) -> io::Result<u16> {
        let bytes = self.take(2)?;
        Ok(u16::from_le_bytes([bytes[0], bytes[1]]))
    }

    fn u32(&mut self) -> io::Result<u32> {
        let bytes = self.take(4)?;
        Ok(u32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]))
    }

    fn u64(&mut self) -> io::Result<u64> {
        let bytes = self.take(8)?;
        let mut raw = [0u8; 8];
        raw.copy_from_slice(bytes);
        Ok(u64::from_le_bytes(raw))
    }

    fn string(&mut self) -> io::Result<String> {
        let len = self.u32()? as usize;
        let bytes = self.take(len)?;
        std::str::from_utf8(bytes)
            .map(|s| s.to_owned())
            .map_err(|_| bad("a string is not valid UTF-8"))
    }

    /// The payload must end here.
    fn done(&self) -> io::Result<()> {
        if self.pos == self.buf.len() {
            Ok(())
        } else {
            Err(bad("trailing bytes after the payload"))
        }
    }
}

fn put_u16(buf: &mut Vec<u8>, v: u16) {
    buf.extend_from_slice(&v.to_le_bytes());
}

fn put_u32(buf: &mut Vec<u8>, v: u32) {
    buf.extend_from_slice(&v.to_le_bytes());
}

fn put_u64(buf: &mut Vec<u8>, v: u64) {
    buf.extend_from_slice(&v.to_le_bytes());
}

fn put_str(buf: &mut Vec<u8>, s: &str) {
    put_u32(buf, s.len() as u32);
    buf.extend_from_slice(s.as_bytes());
}
