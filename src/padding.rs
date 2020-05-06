use std::pin::Pin;
use tokio::io::AsyncRead;
use std::task::{Context, Poll};

macro_rules! ready {
    ($e:expr) => (
        match $e {
            ::std::task::Poll::Ready(v) => v,
            ::std::task::Poll::Pending => return ::std::task::Poll::Pending,
        }
    )
}

pub(crate) struct PaddingRemover<'a, R: ?Sized> {
    inner: &'a mut R,
    keep_bytes: u64,
    total_bytes_read: u64,
}

impl<'a, R> PaddingRemover<'a, R>
where 
    R: AsyncRead + Unpin + ?Sized
{
    fn new(inner: &'a mut R, keep_bytes: u64) -> PaddingRemover<'a, R> {
        PaddingRemover { inner, keep_bytes, total_bytes_read: 0 }
    }
}

impl<'a, R> AsyncRead for PaddingRemover<'a, R>
where 
    R: AsyncRead + Unpin + ?Sized,
{
    fn poll_read(mut self: Pin<&mut Self>, cx: &mut Context<'_>, buf: &mut [u8]) -> Poll<std::io::Result<usize>> {
        let underyling_poll = Pin::new(&mut *self.inner).poll_read(cx, buf);
        let mut bytes_read = ready!(underyling_poll)?;
        let mut total_bytes_read = self.total_bytes_read + bytes_read as u64;
        if total_bytes_read > self.keep_bytes {
            let overflow_bytes = total_bytes_read - self.keep_bytes;
            total_bytes_read -= overflow_bytes;
            bytes_read -= overflow_bytes as usize;
        }
        self.total_bytes_read = total_bytes_read;
        Poll::Ready(Ok(bytes_read))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_keep_bytes_over_input_size() {
        let buf = vec![0u8, 1, 2, 3, 4, 5, 6];
        let mut input = buf.as_ref();
        let mut padding = PaddingRemover::new(&mut input, 10);
        let mut out = vec![];
        tokio::io::copy(&mut padding, &mut out).await.unwrap();
        assert_eq!(out, vec![0, 1, 2, 3, 4, 5, 6]);
    }

    #[tokio::test]
    async fn test_keep_bytes_under_input_size() {
        let buf = vec![0u8, 1, 2, 3, 4, 5, 6];
        let mut input = buf.as_ref();
        let mut padding = PaddingRemover::new(&mut input, 4);
        let mut out = vec![];
        tokio::io::copy(&mut padding, &mut out).await.unwrap();
        assert_eq!(out, vec![0, 1, 2, 3]);

        // Now test something that doesn't deliver all the bytes at once
        let mut repeat = tokio::io::repeat(255);
        let mut padding = PaddingRemover::new(&mut repeat, 5);
        let mut out = vec![];
        tokio::io::copy(&mut padding, &mut out).await.unwrap();
        assert_eq!(out, vec![255; 5]);
    }

    #[tokio::test]
    async fn test_keep_bytes_0() {
        let buf = vec![0u8, 1, 2, 3, 4, 5, 6];
        let mut input = buf.as_ref();
        let mut padding = PaddingRemover::new(&mut input, 0);
        let mut out = vec![];
        tokio::io::copy(&mut padding, &mut out).await.unwrap();
        assert_eq!(out, Vec::<u8>::new());
    }
}
