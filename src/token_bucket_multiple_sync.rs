use crate::{
    quota::Quota,
    token_bucket::{NotUntil, TokenBucket},
};

#[derive(Debug, Clone)]
pub struct TokenBucketMultipleSync<const N: usize> {
    buckets: [TokenBucket; N],
}

impl<const N: usize> TokenBucketMultipleSync<N> {
    pub fn new(quotas: [Quota; N]) -> Self {
        if N <= 1 {
            panic!("TokenBucketMultipleSync needs at least two buckets");
        }
        let buckets = quotas.map(TokenBucket::new);
        Self { buckets }
    }
}

impl<const N: usize> TokenBucketMultipleSync<N> {
    pub fn check_ns(&self, ns: [u32; N]) -> Result<(), NotUntil> {
        // reduce the self.buckets.iter() with result::and
        self.buckets
            .iter()
            .zip(ns)
            .map(|(bucket, n)| bucket.check_n(n))
            .reduce(|a, b| a.and(b))
            .expect("Always has at least two buckets")
    }

    pub fn try_take_ns(&mut self, ns: [u32; N]) -> Result<(), NotUntil> {
        let mut buckets_new = self.buckets.clone();
        let res = buckets_new
            .iter_mut()
            .zip(ns)
            .try_for_each(|(bucket, n)| bucket.try_take_n(n));

        res.and_then(|_| {
            self.buckets = buckets_new;
            Ok(())
        })
    }
}
#[cfg(test)]
mod test {
    use super::*;
    use nonzero_ext::nonzero;

    #[test]
    fn test_new() {
        let bucket = TokenBucketMultipleSync::new([
            Quota::per_second(nonzero!(4u32)),
            Quota::per_second(nonzero!(2u32)),
        ]);
        assert_eq!(bucket.buckets.len(), 2);
    }

    #[test]
    #[should_panic(expected = "TokenBucketMultipleSync needs at least two buckets")]
    fn test_new_panic() {
        TokenBucketMultipleSync::new([Quota::per_second(nonzero!(4u32))]);
    }

    #[test]
    fn test_check_ns_ok() {
        let bucket = TokenBucketMultipleSync::new([
            Quota::per_second(nonzero!(4u32)),
            Quota::per_second(nonzero!(2u32)),
        ]);
        assert_eq!(bucket.check_ns([1, 1]), Ok(()));
    }

    #[test]
    fn test_check_ns_err() {
        let bucket = TokenBucketMultipleSync::new([
            Quota::per_second(nonzero!(4u32)),
            Quota::per_second(nonzero!(2u32)),
        ]);
        assert!(bucket.check_ns([5, 3]).is_err());
    }

    #[test]
    fn test_try_take_ns_ok() {
        let mut bucket = TokenBucketMultipleSync::new([
            Quota::per_second(nonzero!(4u32)),
            Quota::per_second(nonzero!(2u32)),
        ]);
        assert_eq!(bucket.try_take_ns([1, 1]), Ok(()));
    }

    #[test]
    fn test_try_take_ns_err() {
        let mut bucket = TokenBucketMultipleSync::new([
            Quota::per_second(nonzero!(4u32)),
            Quota::per_second(nonzero!(2u32)),
        ]);
        assert!(bucket.try_take_ns([5, 3]).is_err());
    }

    #[test]
    fn test_try_take_ns_multiple_buckets() {
        let mut bucket = TokenBucketMultipleSync::new([
            Quota::per_second(nonzero!(4u32)),
            Quota::per_second(nonzero!(2u32)),
        ]);
        assert_eq!(bucket.try_take_ns([1, 1]), Ok(()));
        assert_eq!(bucket.try_take_ns([1, 1]), Ok(()));
        assert!(bucket.try_take_ns([1, 1]).is_err());
    }

    #[test]
    fn test_try_take_ns_multiple_buckets_concurrent() {
        let mut bucket = TokenBucketMultipleSync::new([
            Quota::per_second(nonzero!(4u32)),
            Quota::per_second(nonzero!(3u32)),
        ]);
        assert_eq!(bucket.try_take_ns([3, 1]), Ok(()));
        assert!(bucket.try_take_ns([2, 1]).is_err());
        assert_eq!(bucket.try_take_ns([1, 1]), Ok(()));
        assert!(bucket.try_take_ns([1, 0]).is_err());
    }
}
