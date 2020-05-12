//! Functions to read content from storage

use anyhow::{Result, Error, ensure};
use tracing::info;
use tokio_util::compat::FuturesAsyncReadCompatExt;
use futures::stream::{self, StreamExt, TryStreamExt};
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
    info!(id = file.id, "reading file");
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
            let whole_block_size = 65536;
            // Block size for all of our AES-128-GCM files
            let block_size = whole_block_size - 16;
            let aes_gcm_length = get_aes_gcm_length(file.size as u64, block_size);

            let mut streams = vec![];
            let mut gcm_stream_bytes: u64 = 0;
            for gdrive_file in gdrive_files {
                let gdrive_file_id = &*gdrive_file.id;
                info!(id = gdrive_file_id, size = gdrive_file.size, "streaming gdrive file");
                let encrypted_read = stream_gdrive_file_on_domain(&gdrive_file.id, *gsuite_domain).await?;

                // We need to truncate the random padding off the gdrive file itself, to avoid
                // AES-GCM decryption failure.
                //
                // aes_gcm_length tells us when to stop for the size of the entire stream,
                // but we actually need to truncate an individual gdrive file.
                let last_gcm_stream_bytes = gcm_stream_bytes;
                gcm_stream_bytes += gdrive_file.size as u64;
                // This will be too large except for the last gdrive file in the sequence, but
                // there is no harm.
                let keep_bytes = aes_gcm_length - last_gcm_stream_bytes;
                let truncated_read = encrypted_read.take(keep_bytes);
    
                let key = gcm_create_key(*cipher_key)?;
                let first_block_number = last_gcm_stream_bytes / whole_block_size as u64;
                let decoder = GcmDecoder::new(block_size, key, first_block_number);
                let frame_reader = FramedRead::new(truncated_read, decoder);
                streams.push(frame_reader);
            }

            let concat_stream = stream::iter(streams).flatten();
            let decrypted_read = concat_stream
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
