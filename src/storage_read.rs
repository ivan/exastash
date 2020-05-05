//! Functions to read content from storage

use anyhow::{Result, Error};
use tokio_util::compat::FuturesAsyncReadCompatExt;
use futures::stream::{self,  TryStreamExt};
use crate::db::storage::{Storage, inline, gdrive, internetarchive};

/// Returns an AsyncRead with the content of a file
pub fn read(storage: &Storage) -> Result<Box<dyn tokio::io::AsyncRead + Unpin>> {
    Ok(Box::new(match storage {
        Storage::Inline(inline::Storage { content, .. }) => {
            let stream = stream::iter::<_>(vec![Ok(content.clone())]);
            stream
                .map_err(|e: Error| futures::io::Error::new(futures::io::ErrorKind::Other, e))
                .into_async_read()
                .compat()
        }
        Storage::Gdrive(gdrive::Storage { .. }) => {
            // TODO:
            // concat streams of all gdrive files
            // run through GcmDecoder
            // run through RightTruncate (or add "stop at byte" to GcmDecoder - probably faster)
            unimplemented!()
        }
        Storage::InternetArchive(internetarchive::Storage { .. }) => {
            unimplemented!()
        }
    }))
}
