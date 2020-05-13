//! Functions to read content from storage

use std::pin::Pin;
use anyhow::{Result, Error, ensure};
use bytes::{Bytes, BytesMut, Buf, BufMut};
use tracing::info;
use futures::stream::{self, Stream};
use futures_async_stream::stream;
use tokio::io::AsyncReadExt;
use tokio_util::codec::FramedRead;
use crate::db::inode;
use crate::db::storage::{Storage, inline, gdrive, internetarchive};
use crate::gdrive::stream_gdrive_file_on_domain;
use crate::crypto::{GcmDecoder, gcm_create_key};

type Aes128Ctr = ctr::Ctr128<aes::Aes128>;

// fn stream_gdrive_ctr_chunks(file: &inode::File, storage: &gdrive::Storage) -> Pin<Box<dyn Stream<Item = Result<Bytes, Error>>>> {
//     let file = file.clone();
//     let storage = storage.clone();

//     #[stream]
//     async move {
//         for gdrive_file in storage.gdrive_files {
//             info!(id = &*gdrive_file.id, size = gdrive_file.size, "streaming gdrive file");
//             let encrypted_read = stream_gdrive_file_on_domain(&gdrive_file.id, storage.gsuite_domain).await;
//             let encrypted_read = match encrypted_read {
//                 Err(e) => {
//                     yield Err(e.into());
//                     break;
//                 }
//                 Ok(v) => v,
//             };

//             #[for_await]
//             for frame in frame_reader {
//                 yield frame;
//             }
//             // TODO: on EOF, make sure we got the expected number of bytes
//         }
//     }
// }

fn get_aes_gcm_length(content_length: u64, block_size: usize) -> u64 {
    // We want division to round up here, so fix it up by incrementing when needed
    let mut number_of_tags = content_length / block_size as u64;
    if content_length % block_size as u64 != 0 {
        number_of_tags += 1;
    }
    let length_of_tags = 16 * number_of_tags;
    content_length + length_of_tags
}

fn stream_gdrive_gcm_chunks(file: &inode::File, storage: &gdrive::Storage) -> Pin<Box<dyn Stream<Item = Result<Bytes, Error>>>> {
    let file = file.clone();
    let storage = storage.clone();

    Box::pin(
        #[stream]
        async move {
            let whole_block_size = 65536;
            // Block size for all of our AES-128-GCM files
            let block_size = whole_block_size - 16;
            let aes_gcm_length = get_aes_gcm_length(file.size as u64, block_size);

            let mut gcm_stream_bytes = 0;
            for gdrive_file in storage.gdrive_files {
                info!(id = &*gdrive_file.id, size = gdrive_file.size, "streaming gdrive file");
                let encrypted_read = stream_gdrive_file_on_domain(&gdrive_file.id, storage.gsuite_domain).await;
                let encrypted_read = match encrypted_read {
                    Err(e) => {
                        yield Err(e.into());
                        break;
                    }
                    Ok(v) => v,
                };

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

                let key = gcm_create_key(storage.cipher_key).unwrap();
                let first_block_number = last_gcm_stream_bytes / whole_block_size as u64;
                let decoder = GcmDecoder::new(block_size, key, first_block_number);
                let frame_reader = FramedRead::new(truncated_read, decoder);
                #[for_await]
                for frame in frame_reader {
                    yield frame;
                }
                // TODO: on EOF, make sure we got the expected number of bytes
            }
        }
    )
}

fn stream_gdrive_files(file: &inode::File, storage: &gdrive::Storage) -> Pin<Box<dyn Stream<Item = Result<Bytes, Error>>>> {
    match storage.cipher {
        gdrive::Cipher::Aes128Gcm => stream_gdrive_gcm_chunks(file, storage),
        // Old files that we no longer produce
        //gdrive::Cipher::Aes128Ctr => stream_gdrive_ctr_chunks(file, storage),
        gdrive::Cipher::Aes128Ctr => unreachable!(),
    }
}

/// Return the content of a file as a pinned boxed Stream on which caller can call .into_async_read()
pub async fn read(file: &inode::File, storage: &Storage) -> Result<Pin<Box<dyn Stream<Item = Result<Bytes, Error>>>>> {
    info!(id = file.id, "reading file");
    Ok(match storage {
        Storage::Inline(inline::Storage { content, .. }) => {
            ensure!(
                content.len() as i64 == file.size,
                "length of inline storage for file id={} is {} but file size is {}", file.id, content.len(), file.size
            );
            let mut bytes = BytesMut::new();
            bytes.put(&content[..]);
            Box::pin(stream::iter::<_>(vec![Ok(bytes.to_bytes())]))
        }
        Storage::Gdrive(gdrive_storage) => {
            stream_gdrive_files(file, gdrive_storage)
        }
        Storage::InternetArchive(internetarchive::Storage { .. }) => {
            unimplemented!()
        }
    })
}
