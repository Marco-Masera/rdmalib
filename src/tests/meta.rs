use std::io::{self, Cursor, ErrorKind};
use std::net::TcpListener;
use std::thread;

use crate::meta::{
    decode, read_frame, Channel, Message, RegionDesc, TupleDesc, PROTOCOL_VERSION,
};

fn round_trip(msg: Message) {
    let bytes = msg.encode();
    let back = read_frame(&mut Cursor::new(bytes)).unwrap();
    assert_eq!(back, msg);
}

/// A hand-built frame, for feeding the decoder hostile input.
fn frame(len: u32, ty: u8, payload: &[u8]) -> Vec<u8> {
    let mut buf = Vec::new();
    buf.extend_from_slice(&len.to_le_bytes());
    buf.push(ty);
    buf.extend_from_slice(payload);
    buf
}

fn read(bytes: &[u8]) -> io::Result<Message> {
    read_frame(&mut Cursor::new(bytes.to_vec()))
}

#[test]
fn every_message_round_trips() {
    round_trip(Message::Hello {
        version: PROTOCOL_VERSION,
    });
    round_trip(Message::Welcome { version: 1 });
    round_trip(Message::WantGroup { group: 7 });
    round_trip(Message::Update);
    round_trip(Message::Metadata {
        regions: vec![
            RegionDesc {
                id: 0,
                name: "heap".into(),
                size: 4096,
                elem_size: 1,
                elem_align: 1,
                elem_type: "u8".into(),
            },
            RegionDesc {
                id: 1,
                name: String::new(),
                size: u64::from(u32::MAX) * 3,
                elem_size: 16,
                elem_align: 8,
                elem_type: "app::Reading".into(),
            },
        ],
    });
    round_trip(Message::Tuples {
        group: 7,
        tuples: vec![TupleDesc {
            region_id: 1,
            remote_addr: 0x2000,
            size: 8192,
            rkey: 0xAB,
        }],
    });
}

#[test]
fn frame_layout_is_pinned() {
    // Hello{version: 1}: [len=3][type 1][01 00].
    assert_eq!(
        Message::Hello { version: 1 }.encode(),
        vec![3, 0, 0, 0, 1, 1, 0]
    );

    // The type codes and, for the empty composites, the minimal
    // payload sizes: the count field of Metadata, group plus count of
    // Tuples.
    let cases = [
        (Message::Hello { version: 1 }, 3u32, 1u8),
        (Message::Welcome { version: 1 }, 3, 2),
        (Message::WantGroup { group: 0 }, 5, 3),
        (Message::Update, 1, 4),
        (
            Message::Metadata {
                regions: Vec::new(),
            },
            5,
            5,
        ),
        (
            Message::Tuples {
                group: 0,
                tuples: Vec::new(),
            },
            9,
            6,
        ),
    ];
    for (msg, len, ty) in cases {
        let bytes = msg.encode();
        let got_len = u32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]);
        assert_eq!((got_len, bytes[4]), (len, ty), "{msg:?}");
    }
}

#[test]
fn truncated_streams_end_with_unexpected_eof() {
    let full = Message::Hello { version: 1 }.encode();
    for cut in 0..full.len() {
        assert_eq!(
            read(&full[..cut]).unwrap_err().kind(),
            ErrorKind::UnexpectedEof,
            "cut at {cut}"
        );
    }
    // A header promising more than the stream holds.
    assert_eq!(
        read(&frame(3, 1, &[1])).unwrap_err().kind(),
        ErrorKind::UnexpectedEof
    );
}

#[test]
fn out_of_range_frame_lengths_are_rejected() {
    for len in [0u32, (1 << 20) + 1, u32::MAX] {
        let err = read(&frame(len, 1, &[1, 0])).unwrap_err();
        assert_eq!(err.kind(), ErrorKind::InvalidData, "len {len}");
    }
    // The cap itself passes the length check; its payload then simply
    // fails to arrive.
    assert_eq!(
        read(&frame(1 << 20, 1, &[])).unwrap_err().kind(),
        ErrorKind::UnexpectedEof
    );
}

#[test]
fn unknown_message_types_are_rejected() {
    assert_eq!(
        read(&frame(1, 9, &[])).unwrap_err().kind(),
        ErrorKind::InvalidData
    );
}

#[test]
fn trailing_payload_bytes_are_rejected() {
    // Hello's payload is two bytes; a third is a violation.
    assert_eq!(
        decode(1, &[1, 0, 0]).unwrap_err().kind(),
        ErrorKind::InvalidData
    );
}

#[test]
fn bogus_lengths_fail_fast() {
    // A region count of 2^32 - 1 with no entries behind it: the first
    // entry must fail instead of anything being allocated for the
    // count.
    assert_eq!(
        read(&frame(5, 5, &u32::MAX.to_le_bytes()))
            .unwrap_err()
            .kind(),
        ErrorKind::InvalidData
    );

    // A name length reaching far past the payload.
    let mut payload = Vec::new();
    payload.extend_from_slice(&1u32.to_le_bytes()); // region count
    payload.extend_from_slice(&1u32.to_le_bytes()); // id
    payload.extend_from_slice(&0x4000_0000u32.to_le_bytes()); // name length
    assert_eq!(
        read(&frame(1 + payload.len() as u32, 5, &payload))
            .unwrap_err()
            .kind(),
        ErrorKind::InvalidData
    );

    // A name that is not UTF-8.
    let mut payload = Vec::new();
    payload.extend_from_slice(&1u32.to_le_bytes()); // region count
    payload.extend_from_slice(&1u32.to_le_bytes()); // id
    payload.extend_from_slice(&1u32.to_le_bytes()); // name length
    payload.push(0xFF);
    assert_eq!(
        read(&frame(1 + payload.len() as u32, 5, &payload))
            .unwrap_err()
            .kind(),
        ErrorKind::InvalidData
    );
}

#[test]
fn channels_exchange_messages_over_loopback() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();

    let server = thread::spawn(move || {
        let (stream, _) = listener.accept().unwrap();
        let mut channel = Channel::new(stream).unwrap();
        assert_eq!(
            channel.receive().unwrap(),
            Message::Hello {
                version: PROTOCOL_VERSION
            }
        );
        channel
            .send(&Message::Welcome {
                version: PROTOCOL_VERSION,
            })
            .unwrap();
        assert_eq!(channel.receive().unwrap(), Message::Update);
    });

    let mut client = Channel::connect(addr).unwrap();
    client
        .send(&Message::Hello {
            version: PROTOCOL_VERSION,
        })
        .unwrap();
    assert_eq!(
        client.receive().unwrap(),
        Message::Welcome {
            version: PROTOCOL_VERSION
        }
    );
    client.send(&Message::Update).unwrap();
    server.join().unwrap();
}
