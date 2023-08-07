use crate::{
    quota::Quota,
    token_bucket::{NotUntil, TokenBucket},
};
use futures_timer::Delay;
use log::debug;
use once_cell::sync::Lazy;
use rand::distributions::{Distribution, Uniform};
use rand::thread_rng;
use std::hash::Hash;
use std::time::Duration;
use std::{collections::HashMap, time::Instant};
use strum::IntoEnumIterator;

static JITTER_DIST: Lazy<Uniform<u64>> = Lazy::new(|| Uniform::new(0, 10));

pub trait QuotasTrait {
    fn get_quota(&self) -> Quota;
}

pub struct TokenBucketUltimate<T: QuotasTrait + IntoEnumIterator>(HashMap<T, TokenBucket>);

impl<T: QuotasTrait + IntoEnumIterator + Hash + Eq + Clone> TokenBucketUltimate<T> {
    pub fn new() -> Self {
        let mut map = HashMap::new();
        for variant in T::iter() {
            let quota = variant.get_quota();
            map.insert(variant, TokenBucket::new(quota));
        }
        Self(map)
    }

    pub fn check_n(&self, pairs: &[(T, u32)]) -> Result<(), NotUntil> {
        pairs
            .iter()
            .map(|(key, n)| {
                self.0
                    .get(key)
                    .expect("Do not use a key that is not inserted")
                    .check_n(*n)
            })
            .fold(Ok(()), |a, b| a.and(b))
    }

    pub fn try_take_n(&mut self, pairs: &[(T, u32)]) -> Result<(), NotUntil> {
        let mut buckets_new = self.0.clone();
        let res = pairs
            .iter()
            .try_for_each(|(key, n)| buckets_new.get_mut(key).unwrap().try_take_n(*n));

        res.and_then(|_| {
            self.0 = buckets_new;
            Ok(())
        })
    }

    pub async fn take_n(&mut self, pairs: &[(T, u32)]) {
        loop {
            match self.try_take_n(pairs) {
                Ok(_) => return,
                Err(not_until) => {
                    let jitter = JITTER_DIST.sample(&mut thread_rng());
                    let jitter_dur = Duration::from_millis(jitter);
                    let delay_dur = not_until.duration_since(Instant::now()) + jitter_dur;
                    Delay::new(delay_dur).await;
                    debug!("delay_dur: {:?}", delay_dur);
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use futures::executor::block_on;
    use nonzero_ext::nonzero;
    use std::sync::Arc;
    use std::sync::Mutex;
    use std::thread;
    use tokio::time::Duration;

    use test_log::test;

    use super::*;

    #[derive(EnumIter, Hash, Eq, PartialEq, Clone, Copy, Debug)]
    pub enum Quotas {
        Ten,
        Twenty,
    }
    impl QuotasTrait for Quotas {
        fn get_quota(&self) -> Quota {
            match self {
                Quotas::Ten => Quota::per_second(nonzero!(10u32)),
                Quotas::Twenty => Quota::per_second(nonzero!(20u32)),
            }
        }
    }

    pub fn ultimate() -> TokenBucketUltimate<Quotas> {
        TokenBucketUltimate::new()
    }

    #[tokio::test]
    async fn test_check() {
        let ultimate = ultimate();
        assert_eq!(
            ultimate.check_n(&[(Quotas::Ten, 5), (Quotas::Twenty, 10)]),
            Ok(())
        );
        assert!(ultimate
            .check_n(&[(Quotas::Ten, 5), (Quotas::Twenty, 30)])
            .is_err());
        assert!(ultimate
            .check_n(&[(Quotas::Ten, 15), (Quotas::Twenty, 10)])
            .is_err());
        assert_eq!(ultimate.check_n(&[(Quotas::Ten, 5)]), Ok(()));
        assert_eq!(ultimate.check_n(&[]), Ok(()));
    }

    #[tokio::test]
    async fn test_take_sync() {
        let mut ultimate = ultimate();
        assert_eq!(
            ultimate.try_take_n(&[(Quotas::Ten, 5), (Quotas::Twenty, 10)]),
            Ok(())
        );
        assert_eq!(ultimate.try_take_n(&[(Quotas::Ten, 5)]), Ok(()));
        assert!(ultimate.try_take_n(&[(Quotas::Ten, 5)]).is_err());
        assert_eq!(ultimate.try_take_n(&[(Quotas::Twenty, 10)]), Ok(()));
        assert!(ultimate.try_take_n(&[(Quotas::Twenty, 5)]).is_err());
        tokio::time::sleep(Duration::from_secs(1)).await;
        assert_eq!(
            ultimate.try_take_n(&[(Quotas::Ten, 10), (Quotas::Twenty, 20)]),
            Ok(())
        );
    }

    #[tokio::test]
    async fn test_take_async() {
        let mut ultimate = ultimate();
        let start = Instant::now();
        ultimate
            .take_n(&[(Quotas::Ten, 5), (Quotas::Twenty, 10)])
            .await;
        ultimate
            .take_n(&[(Quotas::Ten, 5), (Quotas::Twenty, 10)])
            .await;
        assert!(start.elapsed() < Duration::from_millis(10));

        let mid = Instant::now();
        ultimate.take_n(&[(Quotas::Twenty, 10)]).await;
        assert!(mid.elapsed() > Duration::from_millis(450));
    }

    #[test(tokio::test)]
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
                            .take_n(&[(Quotas::Twenty, request_weight)])
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
