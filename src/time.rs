use std::{
    sync::OnceLock,
    time::{Instant, SystemTime, UNIX_EPOCH},
};

#[inline(always)]
pub fn now_ns() -> u64 {
    // (t0, epoch_ns) initialised once
    static BASE: OnceLock<(Instant, u64)> = OnceLock::new();
    let (t0, epoch_ns) = *BASE.get_or_init(|| {
        let realtime = SystemTime::now().duration_since(UNIX_EPOCH).unwrap();
        (Instant::now(), realtime.as_nanos() as u64)
    });

    // time elapsed since t0 as nanoseconds
    let delta = Instant::now().duration_since(t0).as_nanos() as u64;

    epoch_ns + delta
}
