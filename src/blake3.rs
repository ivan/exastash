//! BLAKE3-related helpers

use std::pin::Pin;
use std::sync::Arc;
use parking_lot::Mutex;
use pin_project::pin_project;
use futures::{ready, stream::Stream, task::{Context, Poll}};

#[pin_project]
pub(crate) struct Blake3HashingStream<S> {
    #[pin]
    stream: S,
    b3sum: Arc<Mutex<blake3::Hasher>>,
}

impl<S> Blake3HashingStream<S> {
    pub fn new(stream: S, b3sum: Arc<Mutex<blake3::Hasher>>) -> Blake3HashingStream<S> {
        Blake3HashingStream { stream, b3sum }
    }

    /// Returns an `Arc` which can be derefenced to get the blake3 Hasher
    #[inline]
    pub fn b3sum(&self) -> Arc<Mutex<blake3::Hasher>> {
        self.b3sum.clone()
    }
}

impl<S, O, E> Stream for Blake3HashingStream<S>
where
    O: AsRef<[u8]>,
    S: Stream<Item = Result<O, E>>,
{
    type Item = Result<O, E>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let b3sum = self.b3sum();
        if let Some(res) = ready!(self.project().stream.poll_next(cx)) {
            if let Ok(bytes) = &res {
                b3sum.lock().update(bytes.as_ref());
            }
            Poll::Ready(Some(res))
        } else {
            Poll::Ready(None)
        }
    }
}

pub(crate) fn b3sum_bytes(bytes: &[u8]) -> blake3::Hash {
    let mut b3sum = blake3::Hasher::new();
    b3sum.update(bytes);
    blake3::Hasher::finalize(&b3sum)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_b3sum_bytes() {
        assert_eq!(
            b3sum_bytes(b"hello world").as_bytes().as_ref(),
            &hex::decode("d74981efa70a0c880b8d8c1985d075dbcbf679b99a5f9914e5aaf96b831a9e24").unwrap()[..]
        );
    }
}
