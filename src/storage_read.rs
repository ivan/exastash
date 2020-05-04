use anyhow::{Result, Error};
use tokio_util::compat::FuturesAsyncReadCompatExt;
use futures::stream::{self,  TryStreamExt};
use crate::db::storage::{Storage, inline, gdrive, internetarchive};

pub(crate) fn read(storage: &Storage) -> Result<Box<dyn tokio::io::AsyncRead>> {
    Ok(Box::new(match storage {
        Storage::Inline(inline::Storage { content, .. }) => {
            let stream = stream::iter::<_>(vec![Ok(content.clone())]);
            stream
                .map_err(|e: Error| futures::io::Error::new(futures::io::ErrorKind::Other, e))
                .into_async_read()
                .compat()
        }
        Storage::Gdrive(gdrive::Storage { .. }) => {
            unimplemented!()
        }
        Storage::InternetArchive(internetarchive::Storage { .. }) => {
            unimplemented!()
        }
    }))
}
