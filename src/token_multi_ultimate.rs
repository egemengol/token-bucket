use std::time::Duration;
use std::{collections::HashMap, time::Instant};

use rand::distributions::{Distribution, Uniform};
use rand::thread_rng;

use futures_timer::Delay;

use crate::{
    quota::Quota,
    token_bucket::{NotUntil, TokenBucket},
};

use once_cell::sync::Lazy;

static JITTER_DIST: Lazy<Uniform<u64>> = Lazy::new(|| Uniform::new(0, 10));

pub struct TokenBucketUltimate(HashMap<String, TokenBucket>);

impl TokenBucketUltimate {
    pub fn new() -> Self {
        Self(HashMap::new())
    }

    pub fn insert(&mut self, key: String, quota: Quota) {
        self.0.insert(key, TokenBucket::new(quota));
    }

    pub fn check_n(&self, pairs: &[(&str, u32)]) -> Result<(), NotUntil> {
        pairs
            .iter()
            .map(|&(key, n)| {
                self.0
                    .get(key)
                    .expect("Do not use a key that is not inserted")
                    .check_n(n)
            })
            .fold(Ok(()), |a, b| a.and(b))
    }

    pub fn try_take_n(&mut self, pairs: &[(&str, u32)]) -> Result<(), NotUntil> {
        let mut buckets_new = self.0.clone();
        let res = pairs
            .iter()
            .try_for_each(|&(key, n)| buckets_new.get_mut(key).unwrap().try_take_n(n));

        res.and_then(|_| {
            self.0 = buckets_new;
            Ok(())
        })
    }

    pub async fn take_n(&mut self, pairs: &[(&str, u32)]) {
        loop {
            println!("Looping...");
            match self.try_take_n(pairs) {
                Ok(_) => return,
                Err(not_until) => {
                    let jitter = JITTER_DIST.sample(&mut thread_rng());
                    let jitter_dur = Duration::from_micros(jitter);
                    println!("not_until: {:?}, now: {:?}", not_until, Instant::now());
                    let delay = Delay::new(not_until.duration_since(Instant::now()) + jitter_dur);
                    println!("delay: {:?}", delay);
                    delay.await;
                    println!("delay done");
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use nonzero_ext::nonzero;

    use super::*;

    pub fn ultimate() -> TokenBucketUltimate {
        let mut ultimate = TokenBucketUltimate::new();
        ultimate.insert("a".to_string(), Quota::per_second(nonzero!(10u32)));
        ultimate.insert("b".to_string(), Quota::per_second(nonzero!(20u32)));
        ultimate
    }

    #[tokio::test]
    async fn test_check() {
        let ultimate = ultimate();
        assert_eq!(ultimate.check_n(&[("a", 5), ("b", 10)]), Ok(()));
        assert!(ultimate.check_n(&[("a", 5), ("b", 30)]).is_err());
        assert!(ultimate.check_n(&[("a", 15), ("b", 10)]).is_err());
        assert_eq!(ultimate.check_n(&[("a", 5)]), Ok(()));
        assert_eq!(ultimate.check_n(&[]), Ok(()));
    }

    #[tokio::test]
    async fn test_take_sync() {
        let mut ultimate = ultimate();
        assert_eq!(ultimate.try_take_n(&[("a", 5), ("b", 10)]), Ok(()));
        assert_eq!(ultimate.try_take_n(&[("a", 5)]), Ok(()));
        assert!(ultimate.try_take_n(&[("a", 5)]).is_err());
        assert_eq!(ultimate.try_take_n(&[("b", 10)]), Ok(()));
        assert!(ultimate.try_take_n(&[("b", 5)]).is_err());
        tokio::time::sleep(Duration::from_secs(1)).await;
        assert_eq!(ultimate.try_take_n(&[("a", 10), ("b", 20)]), Ok(()));
    }

    #[tokio::test]
    async fn test_take_async() {
        let mut ultimate = ultimate();
        let start = Instant::now();
        ultimate.take_n(&[("a", 5), ("b", 10)]).await;
        ultimate.take_n(&[("a", 5), ("b", 10)]).await;
        assert!(start.elapsed() < Duration::from_millis(10));

        let mid = Instant::now();
        ultimate.take_n(&[("b", 10)]).await;
        assert!(mid.elapsed() > Duration::from_millis(450));
    }
    use futures::executor::block_on;
    use std::sync::Arc;
    use std::sync::Mutex;
    use std::thread;
    use tokio::time::Duration;

    #[tokio::test]
    async fn test_threaded() {
        let ultimate = Arc::new(Mutex::new(ultimate()));
        let num_threads = 3;
        let num_requests = 2;
        let request_weight = 10;
        let mut handles = Vec::new();

        let start = Instant::now();
        for _ in 0..num_threads {
            let ultimate = ultimate.clone();
            let handle = thread::spawn(move || {
                block_on(async {
                    for _ in 0..num_requests {
                        ultimate
                            .lock()
                            .unwrap()
                            .take_n(&[("b", request_weight)])
                            .await;
                    }
                })
            });
            handles.push(handle);
        }

        for handle in handles {
            handle.join().unwrap();
        }
        // b has 20 per second
        // 3 consumers, tring to consume 10 per second twice
        // should take around 2 seconds
        assert!(start.elapsed() > Duration::from_secs(2));
        assert!(start.elapsed() < Duration::from_secs_f32(2.5));
    }
}
