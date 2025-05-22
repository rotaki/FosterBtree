#[inline(always)]
pub fn trace_txn(_kind: u8) {
    #[cfg(feature = "event_tracer")]
    event_tracer::trace_txn(_kind);
}

#[inline(always)]
pub fn trace_diskio(_c_id: u8, _op: char) {
    #[cfg(feature = "event_tracer")]
    event_tracer::trace_diskio(_c_id, _op);
}

#[inline(always)]
pub fn trace_secidx(_c_id: u8, _h: u32, _p: u32, _f: u32) {
    #[cfg(feature = "event_tracer")]
    event_tracer::trace_secidx(_c_id, _h, _p, _f);
}

#[inline(always)]
pub fn trace_lookup() {
    #[cfg(feature = "event_tracer")]
    event_tracer::trace_lookup();
}

#[cfg(feature = "event_tracer")]
mod event_tracer {
    use duckdb::{params, Connection};
    use std::sync::OnceLock;

    use crate::{prelude::urand_int, time::now_ns};

    #[inline(always)]
    pub fn trace_txn(_kind: u8) {
        if urand_int(1, 100) <= 1 {
            EVENT_TRACER.with(|t| {
                t.borrow_mut().txns.push(EventTxn {
                    kind: _kind,
                    ts: now_ns(),
                })
            });
        }
    }

    #[inline(always)]
    pub fn trace_diskio(_c_id: u8, _op: char) {
        if urand_int(1, 100) <= 1 {
            EVENT_TRACER.with(|t| {
                t.borrow_mut().diskio.push(EventDiskio {
                    container: _c_id,
                    op: _op,
                    ts: now_ns(),
                })
            });
        }
    }

    #[inline(always)]
    pub fn trace_secidx(_c_id: u8, _h: u32, _p: u32, _f: u32) {
        if urand_int(1, 100) <= 1 {
            if _h + _p + _f >= 10000 {
                return; // Skip the initial scan
            }
            EVENT_TRACER.with(|t| {
                t.borrow_mut().secidx.push(EventSecIndex {
                    container: _c_id,
                    h: _h,
                    p: _p,
                    f: _f,
                    ts: now_ns(),
                })
            });
        }
    }

    #[inline(always)]
    pub fn trace_lookup() {
        if urand_int(1, 100) <= 1 {
            EVENT_TRACER.with(|t| t.borrow_mut().lookup.push(EventLookup { ts: now_ns() }));
        }
    }

    #[derive(Clone, Copy)]
    pub struct EventTxn {
        pub kind: u8,
        pub ts: u64,
    }
    #[derive(Clone, Copy)]
    pub struct EventDiskio {
        pub container: u8,
        pub op: char,
        pub ts: u64,
    }
    #[derive(Clone, Copy)]
    pub struct EventSecIndex {
        pub container: u8,
        pub h: u32,
        pub p: u32,
        pub f: u32,
        pub ts: u64,
    }

    #[derive(Clone, Copy)]
    pub struct EventLookup {
        pub ts: u64,
    }

    static INIT_SCHEMA: OnceLock<()> = OnceLock::new();

    thread_local! {
        static EVENT_TRACER: std::cell::RefCell<EventTracer> =
            std::cell::RefCell::new(EventTracer::new());
    }

    pub struct EventTracer {
        txns: Vec<EventTxn>,
        diskio: Vec<EventDiskio>,
        secidx: Vec<EventSecIndex>,
        lookup: Vec<EventLookup>,
        conn: Connection,
    }

    impl EventTracer {
        fn new() -> Self {
            let path = std::env::var("DB_PATH").unwrap_or_else(|_| "events.db".into());
            INIT_SCHEMA.get_or_init(|| {
                // this block is executed only once
                let conn = Connection::open(&path).expect("open DuckDB for schema init");
                conn.execute_batch(
                    r#"CREATE TABLE IF NOT EXISTS txns(kind INTEGER, ts BIGINT);
               CREATE TABLE IF NOT EXISTS diskio(container INTEGER, op VARCHAR, ts BIGINT);
               CREATE TABLE IF NOT EXISTS sec_index_hit_rate(
                    container INTEGER, h INTEGER, p INTEGER, f INTEGER, ts BIGINT
               );
               CREATE TABLE IF NOT EXISTS lookup(ts BIGINT);"#,
                )
                .expect("create tables");
                // `conn` is dropped here; other threads can open their own connection normally
            });

            let conn = Connection::open(path).expect("open DuckDB");

            Self {
                txns: Vec::with_capacity(15000),
                diskio: Vec::with_capacity(10000),
                secidx: Vec::with_capacity(5000),
                lookup: Vec::with_capacity(15000),
                conn,
            }
        }

        fn flush(&mut self) {
            if self.txns.is_empty()
                && self.diskio.is_empty()
                && self.secidx.is_empty()
                && self.lookup.is_empty()
            {
                return;
            }

            // Lock only for the actual I/O window
            if !self.txns.is_empty() {
                let mut app = self.conn.appender("txns").unwrap();
                for e in self.txns.drain(..) {
                    app.append_row(params![e.kind, e.ts]).unwrap();
                }
                // app.flush().unwrap();
            }

            if !self.diskio.is_empty() {
                let mut app = self.conn.appender("diskio").unwrap();
                for e in self.diskio.drain(..) {
                    app.append_row(params![e.container, e.op.to_string(), e.ts])
                        .unwrap();
                }
                // app.flush().unwrap();
            }

            if !self.secidx.is_empty() {
                let mut app = self.conn.appender("sec_index_hit_rate").unwrap();
                for e in self.secidx.drain(..) {
                    app.append_row(params![e.container, e.h, e.p, e.f, e.ts])
                        .unwrap();
                }
                // app.flush().unwrap();
            }

            if !self.lookup.is_empty() {
                let mut app = self.conn.appender("lookup").unwrap();
                for e in self.lookup.drain(..) {
                    app.append_row(params![e.ts]).unwrap();
                }
                // app.flush().unwrap();
            }
        }
    }

    /* ---------- 5.  Make sure each thread flushes automatically --------- */

    impl Drop for EventTracer {
        fn drop(&mut self) {
            println!(
                "Flushing event tracer: txns {}, diskio {}, secidx {}, lookup {}",
                self.txns.len(),
                self.diskio.len(),
                self.secidx.len(),
                self.lookup.len()
            );
            self.flush();
        }
    }
}
