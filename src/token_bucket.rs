use std::time::Instant;

use log::debug;

use crate::quota::Quota;

pub type NotUntil = Instant;

#[derive(Debug, Clone)]
pub struct TokenBucket {
    quota: Quota,
    tokens: u32,
    last_update: Instant,
}

impl TokenBucket {
    pub fn new(quota: Quota) -> Self {
        Self {
            quota,
            tokens: 0,
            last_update: Instant::now(),
        }
    }
    pub fn check_n(&self, n: u32) -> Result<(), NotUntil> {
        if n <= self.tokens {
            return Ok(());
        }

        let need_tokens = n - self.tokens;
        let need_dur = self.quota.replenish_1_per * need_tokens;
        if self.last_update.elapsed() < need_dur {
            return Err(self.last_update + need_dur);
        }
        Ok(())
    }

    pub fn try_take_n(&mut self, n: u32) -> Result<(), NotUntil> {
        let earned_tokens = (self.last_update.elapsed().as_micros()
            / self.quota.replenish_1_per.as_micros()) as u32;
        debug!(
            "earned_tokens: {} for quota: {:?} in duration: {:?}",
            earned_tokens,
            self.quota,
            self.last_update.elapsed()
        );
        self.tokens = std::cmp::min(self.tokens + earned_tokens, self.quota.max_burst.get());
        debug!("tokens: {}", self.tokens);
        self.last_update = Instant::now();
        self.check_n(n).and_then(|_| {
            self.tokens -= n;
            Ok(())
        })
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use nonzero_ext::nonzero;

    #[test]
    fn basic_token_bucket_test() {
        let mut bucket = TokenBucket::new(Quota::per_second(nonzero!(4u32)));
        assert_eq!(bucket.check_n(1), Ok(()));
        assert_eq!(bucket.try_take_n(1), Ok(()));
        assert_eq!(bucket.check_n(3), Ok(()));
        assert_eq!(bucket.try_take_n(3), Ok(()));
        assert!(bucket.check_n(1).is_err());
        assert!(bucket.try_take_n(1).is_err());
        std::thread::sleep(std::time::Duration::from_millis(500)); // will have 2
        assert!(bucket.check_n(3).is_err());
        assert!(bucket.try_take_n(3).is_err());
        assert_eq!(bucket.check_n(1), Ok(()));
        assert_eq!(bucket.try_take_n(1), Ok(()));
        assert_eq!(bucket.check_n(1), Ok(()));
        assert_eq!(bucket.try_take_n(1), Ok(()));
        assert!(bucket.check_n(1).is_err());
        assert!(bucket.try_take_n(1).is_err());
        assert_eq!(bucket.check_n(0), Ok(()));
        assert_eq!(bucket.try_take_n(0), Ok(()));
        assert_eq!(bucket.check_n(0), Ok(()));
        assert_eq!(bucket.try_take_n(0), Ok(()));
    }

    #[test]
    fn empty_test() {
        let mut bucket = TokenBucket::new(Quota::per_second(nonzero!(2u32)));
        assert_eq!(bucket.check_n(2), Ok(()));
        assert_eq!(bucket.try_take_n(2), Ok(()));
        assert_eq!(bucket.tokens, 0);
        assert!(bucket.check_n(1).is_err());
    }
}
