// examples/send_batch.rs
use std::{
    io::{Cursor, Write},
    net::UdpSocket,
    time::{SystemTime, UNIX_EPOCH},
};

fn main() -> std::io::Result<()> {
    // ── 1. connect UDP socket ───────────────────────────────────────────────
    let sock = UdpSocket::bind("0.0.0.0:0")?;
    sock.connect("127.0.0.1:8089")?; // adjust host/port if needed

    // ── 2. build one datagram with 5 points ────────────────────────────────
    const BUF_SIZE: usize = 1024;
    let mut buf = [0u8; BUF_SIZE];
    let mut cur = Cursor::new(&mut buf[..]);

    // simple helper – converts SystemTime → ns epoch
    let now_ns = || {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos() as u64
    };

    for i in 0..5 {
        let start = now_ns();
        let end = start + 5_000; // pretend 5 µs later
        let kind = i % 5; // cycle 0..4

        // measurement: tpcc_txn, tag k=kind
        write!(
            cur,
            "tpcc_txn,k={} start={}i,end={}i {}\n",
            kind, start, end, end
        )
        .unwrap();
    }
    let len = cur.position() as usize;

    // ── 3. transmit datagram ────────────────────────────────────────────────
    println!("sending {}-byte datagram with {} points …", len, 5);
    sock.send(&buf[..len])?;
    println!("done");

    Ok(())
}
