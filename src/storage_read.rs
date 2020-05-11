//! Functions to read content from storage

use anyhow::{Result, Error, ensure};
use tracing::info;
use tokio_util::compat::FuturesAsyncReadCompatExt;
use futures::stream::{self, TryStreamExt};
use tokio::io::AsyncReadExt;
use crate::db::storage::{Storage, inline, gdrive, internetarchive};
use crate::gdrive::stream_gdrive_file_on_domain;
use crate::crypto::{GcmDecoder, gcm_create_key};
use crate::db::inode;
use tokio_util::codec::FramedRead;

fn get_aes_gcm_length(content_length: u64, block_size: usize) -> u64 {
    // We want division to round up here, so fix it up by incrementing when needed
    let mut number_of_tags = content_length / block_size as u64;
    if content_length % block_size as u64 != 0 {
        number_of_tags += 1;
    }
    let length_of_tags = 16 * number_of_tags;
    content_length + length_of_tags
}

/// Returns an AsyncRead with the content of a file
pub async fn read(file: &inode::File, storage: &Storage) -> Result<Box<dyn tokio::io::AsyncRead + Unpin>> {
    info!(id = file.id, "Reading file");
    Ok(match storage {
        Storage::Inline(inline::Storage { content, .. }) => {
            ensure!(
                content.len() as i64 == file.size,
                "length of inline storage for file id={} is {} but file size is {}", file.id, content.len(), file.size
            );
            let stream = stream::iter::<_>(vec![Ok(content.clone())]);
            let read = stream
                .map_err(|e: Error| futures::io::Error::new(futures::io::ErrorKind::Other, e))
                .into_async_read()
                .compat();
            Box::new(read)
        }
        Storage::Gdrive(gdrive::Storage { file_id, gsuite_domain, gdrive_files, cipher, cipher_key }) => {
            // Block size for all of our AES-128-GCM files
            let block_size = 65536 - 16;

            // TODO: read all files instead of just first
            let gdrive_file_id = &gdrive_files.get(0).unwrap().id;
            let encrypted_read = stream_gdrive_file_on_domain(gdrive_file_id, gsuite_domain).await?;
            let aes_gcm_length = get_aes_gcm_length(file.size as u64, block_size);
            let truncated_read = encrypted_read.take(aes_gcm_length);

            let key = gcm_create_key(*cipher_key)?;
            let first_block_number = 0;
            let decoder = GcmDecoder::new(block_size, key, first_block_number);
            let frame_reader = FramedRead::new(truncated_read, decoder);
            let decrypted_read = frame_reader
                .map_err(|e: Error| futures::io::Error::new(futures::io::ErrorKind::Other, e))
                .into_async_read()
                .compat();
            // TODO: on EOF, make sure we got the expected number of bytes 
            Box::new(decrypted_read)
        }
        Storage::InternetArchive(internetarchive::Storage { .. }) => {
            unimplemented!()
        }
    })
}
