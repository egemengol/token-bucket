use std::time::Instant;

use crate::quota::Quota;

type NotUntil = Instant;

pub struct TokenBucket {
    quota: Quota,
    tokens: u32,
    last_update: Instant,
}

impl TokenBucket {
    pub fn new(quota: Quota) -> Self {
        Self {
            quota,
            tokens: quota.burst_size().get(),
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
        self.tokens = std::cmp::min(self.tokens + earned_tokens, self.quota.max_burst.get());
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
}
