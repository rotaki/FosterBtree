#[cfg(feature = "influxdb_trace")]
pub mod influxdb_trace {
    use std::cell::RefCell;
    use std::{
        io::{Cursor, Write},
        net::UdpSocket,
    };

    use crate::{prelude::urand_int, time::now_ns};

    thread_local! {
        pub static INFLUX_TRACE: RefCell<TxnInflux> = RefCell::new(TxnInflux::new("127.0.0.1", 8089).unwrap());
    }

    const BUF_SIZE: usize = 64 * 1024 - 512; // one UDP datagram on local host

    /// Batches points and ships them via UDP/Telegraf.
    pub struct TxnInflux {
        suffix: String,
        sock: UdpSocket,
        buf: [u8; BUF_SIZE],
        pos: usize,
    }

    impl TxnInflux {
        /// Connects the socket (non-blocking) to `host:port`.
        #[allow(dead_code)]
        fn new(host: &str, port: u16) -> std::io::Result<Self> {
            let suffix = std::env::var("INFLUX_DB_SUFFIX").unwrap_or_else(|_| "".to_string());
            let sock = UdpSocket::bind("0.0.0.0:0")?;
            sock.connect((host, port))?;
            sock.set_nonblocking(true)?;
            Ok(Self {
                suffix,
                sock,
                buf: [0; BUF_SIZE],
                pos: 0,
            })
        }

        /// Appends one transaction record; flushes if buffer/txn-cap/time limit hit.
        #[inline(always)]
        pub fn append_txn(&mut self, _kind: u8) {
            if urand_int(1, 100) <= 1 {
                self.flush_if_needed();

                let mut cur = Cursor::new(&mut self.buf[self.pos..]);

                write!(
                    cur,
                    "t{},k={} v=1i {}\n", // v=1i is a dummy value needed for InfluxDB
                    self.suffix,
                    _kind,
                    now_ns(),
                )
                .unwrap();

                self.pos += cur.position() as usize;
            }
        }

        #[inline(always)]
        pub fn append_diskio<const IS_READ: bool>(&mut self, _container: u8) {
            if IS_READ {
                // once in 10 times for read
                if urand_int(1, 100) <= 1 {
                    self.flush_if_needed();
                    let mut cur = Cursor::new(&mut self.buf[self.pos..]);
                    write!(
                        cur,
                        "d{},c={},o=r v=1i {}\n", // o=r means read
                        self.suffix,
                        _container,
                        now_ns(),
                    )
                    .unwrap();
                    self.pos += cur.position() as usize;
                }
            } else {
                // once in 100 times for write
                if urand_int(1, 100) <= 1 {
                    self.flush_if_needed();
                    let mut cur = Cursor::new(&mut self.buf[self.pos..]);
                    write!(
                        cur,
                        "d{},c={},o=w v=1i {}\n", // o=w means write
                        self.suffix,
                        _container,
                        now_ns(),
                    )
                    .unwrap();
                    self.pos += cur.position() as usize;
                }
            }
        }

        #[inline(always)]
        pub fn append_sec_index_hit_rate(
            &mut self,
            container: u8,
            hint_worked: usize,
            page_hint_failed: usize,
            frame_hint_failed: usize,
        ) {
            if unrand_int(1, 100) <= 1 {
                self.flush_if_needed();
                let mut cur = Cursor::new(&mut self.buf[self.pos..]);
                write!(
                    cur,
                    "s{},c={} h={}i,p={}i,f={}i,v=1i {}\n",
                    self.suffix,
                    container,
                    hint_worked,
                    page_hint_failed,
                    frame_hint_failed,
                    now_ns(),
                )
                .unwrap();
                self.pos += cur.position() as usize;
            }
        }

        #[allow(dead_code)]
        fn flush_if_needed(&mut self) {
            // Soft flush limits
            if self.pos > BUF_SIZE - 128 {
                self.flush();
            }
        }

        /// Explicit flush; ignores EWOULDBLOCK / ENOBUFS so hot path never panics.
        #[allow(dead_code)]
        fn flush(&mut self) {
            if self.pos == 0 {
                return;
            }
            let _ = self.sock.send(&self.buf[..self.pos]); // best-effort
            self.pos = 0;
        }
    }

    impl Drop for TxnInflux {
        fn drop(&mut self) {
            self.flush();
        }
    }

    /* ------------------------------ Usage sketch ------------------------------

    let mut out = TxnInflux::new("127.0.0.1", 8089)?;

    loop {
        // ... run a transaction ...
        out.append(TxnType::Payment, start_ns, commit_ns);
    }

    out.flush();   // after the workload (or rely on Drop)
    --------------------------------------------------------------------------- */
}
