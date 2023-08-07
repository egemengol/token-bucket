/// Taken from https://github.com/antifuchs/governor/blob/b7242de31c8d28b49afb28ccf540683dd31b3a22/governor/src/quota.rs
/// Nanos, depreciated and some docs removed
/// Added a utility function
use std::prelude::v1::*;

use nonzero_ext::nonzero;
use std::num::NonZeroU32;
use std::time::Duration;

/// A rate-limiting quota.
///
/// Quotas are expressed in a positive number of "cells" (the maximum number of positive decisions /
/// allowed items until the rate limiter needs to replenish) and the amount of time for the rate
/// limiter to replenish a single cell.
///
/// Neither the number of cells nor the replenishment unit of time may be zero.
///
/// # Burst sizes
/// There are multiple ways of expressing the same quota: a quota given as `Quota::per_second(1)`
/// allows, on average, the same number of cells through as a quota given as `Quota::per_minute(60)`.
/// However, the quota of `Quota::per_minute(60)` has a burst size of 60 cells, meaning it is
/// possible to accomodate 60 cells in one go, after which the equivalent of a minute of inactivity
/// is required for the burst allowance to be fully restored.
///
/// Burst size gets really important when you construct a rate limiter that should allow multiple
/// elements through at one time (using [`RateLimiter.check_n`](struct.RateLimiter.html#method.check_n)
/// and its related functions): Only
/// at most as many cells can be let through in one call as are given as the burst size.
///
/// In other words, the burst size is the maximum number of cells that the rate limiter will ever
/// allow through without replenishing them.
#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub struct Quota {
    pub(crate) max_burst: NonZeroU32,
    pub(crate) replenish_1_per: Duration,
}

/// Constructors for Quotas
impl Quota {
    /// Construct a quota for a number of cells per second. The given number of cells is also
    /// assumed to be the maximum burst size.
    pub const fn per_second(max_burst: NonZeroU32) -> Quota {
        let replenish_interval_micros =
            Duration::from_secs(1).as_micros() / (max_burst.get() as u128);
        Quota {
            max_burst,
            replenish_1_per: Duration::from_micros(replenish_interval_micros as u64),
        }
    }

    /// Construct a quota for a number of cells per 60-second period. The given number of cells is
    /// also assumed to be the maximum burst size.
    pub const fn per_minute(max_burst: NonZeroU32) -> Quota {
        let replenish_interval_micros =
            Duration::from_secs(60).as_micros() / (max_burst.get() as u128);
        Quota {
            max_burst,
            replenish_1_per: Duration::from_micros(replenish_interval_micros as u64),
        }
    }

    /// Construct a quota for a number of cells per 60-minute (3600-second) period. The given number
    /// of cells is also assumed to be the maximum burst size.
    pub const fn per_hour(max_burst: NonZeroU32) -> Quota {
        let replenish_interval_micros =
            Duration::from_secs(60 * 60).as_micros() / (max_burst.get() as u128);
        Quota {
            max_burst,
            replenish_1_per: Duration::from_micros(replenish_interval_micros as u64),
        }
    }

    /// Construct a quota that replenishes one cell in a given
    /// interval.
    ///
    /// This constructor is meant to replace [`::new`](#method.new),
    /// in cases where a longer refresh period than 1 cell/hour is
    /// necessary.
    ///
    /// If the time interval is zero, returns `None`.
    /// ```
    pub fn with_period(replenish_1_per: Duration) -> Option<Quota> {
        if replenish_1_per.as_micros() == 0 {
            None
        } else {
            Some(Quota {
                max_burst: nonzero!(1u32),
                replenish_1_per,
            })
        }
    }

    /// Adjusts the maximum burst size for a quota to construct a rate limiter with a capacity
    /// for at most the given number of cells.
    pub const fn allow_burst(self, max_burst: NonZeroU32) -> Quota {
        Quota { max_burst, ..self }
    }
}

/// Retrieving information about a quota
impl Quota {
    /// The time it takes for a rate limiter with an exhausted burst budget to replenish
    /// a single element.
    pub const fn replenish_interval(&self) -> Duration {
        self.replenish_1_per
    }

    /// The maximum number of cells that can be allowed in one burst.
    pub const fn burst_size(&self) -> NonZeroU32 {
        self.max_burst
    }

    /// The time it takes to replenish the entire maximum burst size.
    pub const fn burst_size_replenished_in(&self) -> Duration {
        let fill_in_micros = self.replenish_1_per.as_micros() * self.max_burst.get() as u128;
        Duration::from_micros(fill_in_micros as u64)
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use nonzero_ext::nonzero;

    #[test]
    fn time_multiples() {
        let hourly = Quota::per_hour(nonzero!(1u32));
        let minutely = Quota::per_minute(nonzero!(1u32));
        let secondly = Quota::per_second(nonzero!(1u32));

        assert_eq!(
            hourly.replenish_interval() / 60,
            minutely.replenish_interval()
        );
        assert_eq!(
            minutely.replenish_interval() / 60,
            secondly.replenish_interval()
        );
    }
}
