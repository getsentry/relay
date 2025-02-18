pub use std::time::Duration;

#[cfg(not(test))]
pub use self::real::*;
#[cfg(test)]
pub use self::test::*;

#[cfg(not(test))]
mod real {
    pub use std::time::Instant;
}

#[cfg(test)]
mod test {
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::time::Duration;

    std::thread_local! {
        static NOW: AtomicU64 = const { AtomicU64::new(0) };
    }

    fn now() -> u64 {
        NOW.with(|now| now.load(Ordering::Relaxed))
    }

    pub fn advance_millis(time: u64) {
        NOW.with(|now| now.fetch_add(time, Ordering::Relaxed));
    }

    pub struct Instant(u64);

    impl Instant {
        pub fn now() -> Self {
            Self(now())
        }

        pub fn elapsed(&self) -> Duration {
            match now() - self.0 {
                0 => Duration::from_nanos(100),
                v => Duration::from_millis(v),
            }
        }
    }
}
