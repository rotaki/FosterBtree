use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

#[derive(Clone, Copy)]
pub enum BpMacroOp {
    CreateNewPage,
    GetPageRead,
    GetPageWrite,
}

#[derive(Default)]
struct MacroOpTiming {
    total_ns: AtomicU64,
    min_ns: AtomicU64,
    max_ns: AtomicU64,
    count: AtomicU64,
}

impl MacroOpTiming {
    const fn new() -> Self {
        Self {
            total_ns: AtomicU64::new(0),
            min_ns: AtomicU64::new(u64::MAX),
            max_ns: AtomicU64::new(0),
            count: AtomicU64::new(0),
        }
    }

    fn record(&self, ns: u64) {
        self.total_ns.fetch_add(ns, Ordering::Relaxed);
        self.count.fetch_add(1, Ordering::Relaxed);

        let mut cur_min = self.min_ns.load(Ordering::Relaxed);
        while ns < cur_min {
            match self
                .min_ns
                .compare_exchange(cur_min, ns, Ordering::Relaxed, Ordering::Relaxed)
            {
                Ok(_) => break,
                Err(actual) => cur_min = actual,
            }
        }

        let mut cur_max = self.max_ns.load(Ordering::Relaxed);
        while ns > cur_max {
            match self
                .max_ns
                .compare_exchange(cur_max, ns, Ordering::Relaxed, Ordering::Relaxed)
            {
                Ok(_) => break,
                Err(actual) => cur_max = actual,
            }
        }
    }

    fn reset(&self) {
        self.total_ns.store(0, Ordering::Relaxed);
        self.min_ns.store(u64::MAX, Ordering::Relaxed);
        self.max_ns.store(0, Ordering::Relaxed);
        self.count.store(0, Ordering::Relaxed);
    }

    fn snapshot(&self) -> (u64, u64, u64, u64) {
        (
            self.total_ns.load(Ordering::Relaxed),
            self.min_ns.load(Ordering::Relaxed),
            self.max_ns.load(Ordering::Relaxed),
            self.count.load(Ordering::Relaxed),
        )
    }
}

static CREATE_NEW_PAGE: MacroOpTiming = MacroOpTiming::new();
static GET_PAGE_READ: MacroOpTiming = MacroOpTiming::new();
static GET_PAGE_WRITE: MacroOpTiming = MacroOpTiming::new();

fn timing_for(op: BpMacroOp) -> &'static MacroOpTiming {
    match op {
        BpMacroOp::CreateNewPage => &CREATE_NEW_PAGE,
        BpMacroOp::GetPageRead => &GET_PAGE_READ,
        BpMacroOp::GetPageWrite => &GET_PAGE_WRITE,
    }
}

#[cfg(feature = "bp_macro_profile")]
pub struct ScopedTimer {
    op: BpMacroOp,
    start: Instant,
}

#[cfg(not(feature = "bp_macro_profile"))]
pub struct ScopedTimer;

#[cfg(feature = "bp_macro_profile")]
impl ScopedTimer {
    pub fn new(op: BpMacroOp) -> Self {
        Self {
            op,
            start: Instant::now(),
        }
    }
}

#[cfg(not(feature = "bp_macro_profile"))]
impl ScopedTimer {
    #[inline(always)]
    pub fn new(_op: BpMacroOp) -> Self {
        Self
    }
}

#[cfg(feature = "bp_macro_profile")]
impl Drop for ScopedTimer {
    fn drop(&mut self) {
        timing_for(self.op).record(self.start.elapsed().as_nanos() as u64);
    }
}

pub fn scoped(op: BpMacroOp) -> ScopedTimer {
    ScopedTimer::new(op)
}

pub fn reset() {
    #[cfg(feature = "bp_macro_profile")]
    {
        CREATE_NEW_PAGE.reset();
        GET_PAGE_READ.reset();
        GET_PAGE_WRITE.reset();
    }
}

fn format_ns(ns: f64) -> String {
    if ns >= 1_000_000.0 {
        format!("{:>10.2} ms", ns / 1_000_000.0)
    } else if ns >= 1_000.0 {
        format!("{:>10.2} us", ns / 1_000.0)
    } else {
        format!("{:>10.0} ns", ns)
    }
}

fn render_line(name: &str, timing: &MacroOpTiming) -> Option<String> {
    let (total_ns, min_ns, max_ns, count) = timing.snapshot();
    if count == 0 {
        return None;
    }
    let avg = total_ns as f64 / count as f64;
    Some(format!(
        "{:<24} avg: {}   min: {}   max: {}   cnt: {:>10}",
        name,
        format_ns(avg),
        format_ns(min_ns as f64),
        format_ns(max_ns as f64),
        count,
    ))
}

pub fn report() -> Option<String> {
    #[cfg(feature = "bp_macro_profile")]
    {
        let mut lines = Vec::new();
        if let Some(line) = render_line("CreateNewPage", &CREATE_NEW_PAGE) {
            lines.push(line);
        }
        if let Some(line) = render_line("GetPageForRead", &GET_PAGE_READ) {
            lines.push(line);
        }
        if let Some(line) = render_line("GetPageForWrite", &GET_PAGE_WRITE) {
            lines.push(line);
        }
        if lines.is_empty() {
            None
        } else {
            Some(format!(
                "=== BP Macro Profile ===\n{}\n========================",
                lines.join("\n")
            ))
        }
    }

    #[cfg(not(feature = "bp_macro_profile"))]
    {
        None
    }
}
