use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Instant;

static mut TIMING: AtomicBool = AtomicBool::new(false);

pub fn set_timing(timing: bool) {
    unsafe {
        TIMING.store(timing, Ordering::Relaxed);
    }
}

pub fn timed<L: std::fmt::Display, T, F: FnOnce() -> T>(label: L, f: F) -> T {
    let start = Instant::now();
    let result = f();
    if unsafe { TIMING.load(Ordering::Relaxed) } {
        println!("{}: {:?}", label, start.elapsed());
    }
    result
}
