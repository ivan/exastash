use anyhow::Result;
use bytes::Bytes;
use tokio_util::compat::FuturesAsyncReadCompatExt;
use futures::stream::{Stream, TryStreamExt};
use std::pin::Pin;
use std::task::{Context, Poll};
use crate::db::storage::{Storage, inline, gdrive, internetarchive};

struct SingleItemStream {
    content: Vec<u8>,
    done: bool,
}

impl SingleItemStream {
    fn new(content: Vec<u8>) -> Self {
        SingleItemStream { content, done: false }
    }
}

impl Stream for SingleItemStream {
    type Item = Result<Bytes>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if self.done {
            return Poll::Ready(None);
        }
        self.done = true;
        // TODO: avoid a clone here?
        Poll::Ready(Some(Ok(Bytes::from(self.content.clone()))))
    }
}

pub(crate) fn read(storage: &Storage) -> Result<impl tokio::io::AsyncRead> {
    Ok(match storage {
        Storage::Inline(inline::Storage { content, .. }) => {
            SingleItemStream::new(content.clone())
                .map_err(|e| futures::io::Error::new(futures::io::ErrorKind::Other, e))
                .into_async_read()
                .compat()
        }
        Storage::Gdrive(gdrive::Storage { .. }) => {
            unimplemented!()
        }
        Storage::InternetArchive(internetarchive::Storage { .. }) => {
            unimplemented!()
        }
    })
}
