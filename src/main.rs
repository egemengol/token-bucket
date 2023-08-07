// #[macro_use]
extern crate log;
use std::{
    sync::{Arc, Mutex},
    thread,
    time::{Duration, Instant},
};

use futures::executor::block_on;
use log::warn;
use nonzero_ext::nonzero;
use strum_macros::EnumIter;
use token_bucket::{
    quota::Quota, token_multi_ultimate_enum::QuotasTrait,
    token_multi_ultimate_enum::TokenBucketUltimate,
};

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

async fn test_threaded() {
    let ultimate = Arc::new(Mutex::new(TokenBucketUltimate::new()));
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

#[tokio::main]
async fn main() {
    env_logger::init();
    test_threaded().await;
}
