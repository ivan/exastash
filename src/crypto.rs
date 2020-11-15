use anyhow::{anyhow, bail, ensure, Result, Error};
use byteorder::{BigEndian, WriteBytesExt};
use ring::aead::{LessSafeKey, Nonce, Aad, Tag, UnboundKey, AES_128_GCM};
use bytes::{Bytes, BytesMut, Buf, BufMut};
use tokio_util::codec::{Decoder, Encoder};

#[inline(always)]
pub(crate) fn write_gcm_iv_for_block_number(buf: &mut [u8; 12], block_number: u64) {
    (&mut buf[4..]).write_u64::<BigEndian>(block_number).unwrap();
}

pub(crate) fn gcm_create_key(bytes: [u8; 16]) -> Result<LessSafeKey> {
    let key = LessSafeKey::new(
        UnboundKey::new(&AES_128_GCM, &bytes)
            .map_err(|_| anyhow!("ring failed to create key"))?
    );
    Ok(key)
}

#[inline]
fn gcm_encrypt_block(key: &LessSafeKey, block_number: u64, in_out: &mut [u8]) -> Result<Tag> {
    let mut iv = [0; 12];
    write_gcm_iv_for_block_number(&mut iv, block_number);
    let nonce = Nonce::assume_unique_for_key(iv);
    let tag = key
        .seal_in_place_separate_tag(nonce, Aad::empty(), in_out)
        .map_err(|_| anyhow!("AES-GCM encryption failed with unexpected internal error"))?;
    Ok(tag)
}

#[inline]
fn gcm_decrypt_block(key: &LessSafeKey, block_number: u64, in_out: &mut [u8], tag: &Tag) -> Result<()> {
    let mut iv = [0; 12];
    write_gcm_iv_for_block_number(&mut iv, block_number);
    let nonce = Nonce::assume_unique_for_key(iv);
    key
        .open_in_place_separate_tag(nonce, Aad::empty(), in_out, tag)
        .map_err(|_| anyhow!("AES-GCM decryption failed, likely bad tag or data"))?;
    Ok(())
}

const GCM_TAG_LENGTH: usize = 16;

/// Decodes an AsyncRead to a stream of GCM blocks, one `Bytes` per GCM block
#[derive(Debug)]
pub(crate) struct GcmDecoder {
    block_size: usize,
    key: LessSafeKey,
    block_number: u64,
}

impl GcmDecoder {
    pub(crate) fn new(block_size: usize, key: LessSafeKey, first_block_number: u64) -> Self {
        assert!(block_size > 0, "block size must be > 0");
        GcmDecoder { block_size, key, block_number: first_block_number }
    }
}

impl Decoder for GcmDecoder {
    type Item = Bytes;
    type Error = Error;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        let tag_plus_data_length = self.block_size + GCM_TAG_LENGTH;
        if src.len() < tag_plus_data_length {
            return Ok(None);
        }
        let tag = src.split_to(GCM_TAG_LENGTH);
        let mut data = src.split_to(self.block_size);
        src.reserve(tag_plus_data_length);
        let tag = Tag::new(tag.as_ref()).unwrap();
        gcm_decrypt_block(&self.key, self.block_number, &mut data, &tag)?;
        self.block_number += 1;
        Ok(Some(data.to_bytes()))
    }

    // Last block is not necessarily full-sized
    fn decode_eof(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        if src.is_empty() {
            return Ok(None)
        }
        if src.len() < GCM_TAG_LENGTH {
            bail!("AES-GCM stream ended in the middle of a tag");
        }
        // We shouldn't have a tag unless there was at least one byte of data
        if src.len() == GCM_TAG_LENGTH {
            bail!("AES-GCM stream ended after a tag followed by no data");
        }
        let tag = src.split_to(GCM_TAG_LENGTH);
        let mut data = src;
        // data should be shorter than block_size, else it would have been handled in decode()
        assert!(data.len() < self.block_size);
        let tag = Tag::new(tag.as_ref()).unwrap();
        gcm_decrypt_block(&self.key, self.block_number, &mut data, &tag)?;
        self.block_number += 1;
        Ok(Some(data.to_bytes()))
    }
}
/// Encodes a stream of GCM blocks (one `Bytes` per block) to a stream of bytes.
///
/// All `Bytes` must be of length block_size, except for the last `Bytes` which
/// may be shorter.
#[derive(Debug)]
pub(crate) struct GcmEncoder {
    block_size: usize,
    key: LessSafeKey,
    block_number: u64,
    finalized: bool,
}

impl GcmEncoder {
    pub(crate) fn new(block_size: usize, key: LessSafeKey, first_block_number: u64) -> Self {
        assert!(block_size > 0, "block size must be > 0");
        GcmEncoder { block_size, key, block_number: first_block_number, finalized: false }
    }
}

impl Encoder<Bytes> for GcmEncoder {
    type Error = Error;

    fn encode(&mut self, item: Bytes, dst: &mut BytesMut) -> Result<(), Self::Error> {
        ensure!(item.len() <= self.block_size, "AES-GCM block must be shorter or same length as block size {}, was {}", self.block_size, item.len());
        ensure!(item.len() > 0, "AES-GCM block must not be 0 bytes");
        if self.finalized {
            bail!("cannot encode another AES-GCM block after encoding a block shorter than the block size");
        }
        if item.len() < self.block_size {
            self.finalized = true;
        }
        let mut in_out = BytesMut::from(item.as_ref());
        let tag = gcm_encrypt_block(&self.key, self.block_number, &mut in_out)?;
        self.block_number += 1;
        dst.put_slice(tag.as_ref());
        dst.put_slice(in_out.as_ref());
        Ok(())
    }
}

/// Decodes an AsyncRead to a stream of Bytes of fixed length,
/// except for the last chunk which may be shorter.
#[derive(Debug)]
pub(crate) struct FixedReadSizeDecoder {
    chunk_size: usize,
}

impl FixedReadSizeDecoder {
    pub(crate) fn new(chunk_size: usize) -> Self {
        assert!(chunk_size > 0, "chunk size must be > 0");
        FixedReadSizeDecoder { chunk_size }
    }
}

impl Decoder for FixedReadSizeDecoder {
    type Item = Bytes;
    type Error = std::io::Error;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        if src.len() < self.chunk_size {
            return Ok(None);
        }
        let mut data = src.split_to(self.chunk_size);
        src.reserve(self.chunk_size);
        Ok(Some(data.to_bytes()))
    }

    // Last chunk is not necessarily full-sized
    fn decode_eof(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        if src.is_empty() {
            return Ok(None)
        }
        Ok(Some(src.to_bytes()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio_util::codec::{FramedRead, FramedWrite};
    use futures::stream::{self, StreamExt};

    fn iv_for_block_number(block_number: u64) -> Vec<u8> {
        let mut iv = [0; 12];
        write_gcm_iv_for_block_number(&mut iv, block_number);
        iv.to_vec()
    }

    #[test]
	fn test_write_gcm_iv_for_block_number() {
        assert_eq!(iv_for_block_number(0),                 b"\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00");
        assert_eq!(iv_for_block_number(1),                 b"\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x01");
        assert_eq!(iv_for_block_number(100),               b"\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x64");
        assert_eq!(iv_for_block_number(2_u64.pow(53) - 1), b"\x00\x00\x00\x00\x00\x1f\xff\xff\xff\xff\xff\xff");
        assert_eq!(iv_for_block_number(u64::MAX),          b"\x00\x00\x00\x00\xff\xff\xff\xff\xff\xff\xff\xff");
    }

    #[test]
	fn test_gcm_encrypt_block() -> Result<()> {
        let key = gcm_create_key([0; 16])?;
        let mut in_out = vec![0; 10];
        let block_number = 0;
        let tag = gcm_encrypt_block(&key, block_number, &mut in_out)?;
        assert_eq!(tag.as_ref(), [216, 233, 87, 141, 195, 160, 86, 118, 56, 169, 213, 238, 142, 121, 81, 181]);
        assert_eq!(in_out, [3, 136, 218, 206, 96, 182, 163, 146, 243, 40]);
        Ok(())
    }

    #[test]
	fn test_gcm_decrypt_block() -> Result<()> {
        let key = gcm_create_key([0; 16])?;
        let block_number = 0;
        let tag = Tag::new(&[216, 233, 87, 141, 195, 160, 86, 118, 56, 169, 213, 238, 142, 121, 81, 181]).expect("tag of wrong length?");
        let mut in_out = vec![3, 136, 218, 206, 96, 182, 163, 146, 243, 40];
        gcm_decrypt_block(&key, block_number, &mut in_out, &tag)?;
        assert_eq!(in_out, [0; 10]);
        Ok(())
    }

    #[tokio::test]
    async fn test_gcmencoder_gcmdecoder() -> Result<()> {
        let block_sequences = [
            vec![
                Bytes::from_static(b"hellowo"),
                Bytes::from_static(b"testone"),
                Bytes::from_static(b"testtwo"),
            ],
            // Shorter final block is OK
            vec![
                Bytes::from_static(b"hellowo"),
                Bytes::from_static(b"testone"),
                Bytes::from_static(b"short"),
            ],
            // 1-byte final block is OK
            vec![
                Bytes::from_static(b"hellowo"),
                Bytes::from_static(b"testone"),
                Bytes::from_static(b"1"),
            ],
        ];

        for blocks in &block_sequences {
            let block_size = 7;
            let key_bytes = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15];
            let encoder = GcmEncoder::new(block_size, gcm_create_key(key_bytes)?, 0);
            let decoder = GcmDecoder::new(block_size, gcm_create_key(key_bytes)?, 0);
            let blocks_s = stream::iter(blocks.clone()).map(Ok);

            let mut frame_data = vec![];
            let frame_writer = FramedWrite::new(&mut frame_data, encoder);
            blocks_s.forward(frame_writer).await?;

            let mut out: Vec<Bytes> = vec![];
            let mut frame_reader = FramedRead::new(frame_data.as_ref(), decoder);
            while let Some(result) = frame_reader.next().await {
                out.push(result.unwrap());
            }
            assert_eq!(out, *blocks);
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_gcmencoder_cannot_encode_zero_sized_block() -> Result<()> {
        let block_size = 7;
        let key_bytes = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15];
        let mut encoder = GcmEncoder::new(block_size, gcm_create_key(key_bytes)?, 0);
        let mut dst = BytesMut::new();

        let result = encoder.encode(Bytes::from_static(b""), &mut dst);
        assert_eq!(result.err().expect("expected an error").to_string(), "AES-GCM block must not be 0 bytes");

        Ok(())
    }

    #[tokio::test]
    async fn test_gcmencoder_cannot_encode_oversized_block() -> Result<()> {
        let block_size = 7;
        let key_bytes = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15];
        let mut encoder = GcmEncoder::new(block_size, gcm_create_key(key_bytes)?, 0);
        let mut dst = BytesMut::new();

        let result = encoder.encode(Bytes::from_static(b"too long"), &mut dst);
        assert_eq!(result.err().expect("expected an error").to_string(), "AES-GCM block must be shorter or same length as block size 7, was 8");

        Ok(())
    }

    #[tokio::test]
    async fn test_gcmdecoder_bad_tag() -> Result<()> {
        let block_size = 7;
        let key_bytes = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15];
        let decoder = GcmDecoder::new(block_size, gcm_create_key(key_bytes)?, 0);
        let buf = vec![0; 16 + 7];
        let mut frame_reader = FramedRead::new(buf.as_ref(), decoder);

        let result = frame_reader.next().await.expect("Some");
        assert_eq!(result.err().expect("expected an error").to_string(), "AES-GCM decryption failed, likely bad tag or data");

        Ok(())
    }

    #[tokio::test]
    async fn test_gcmdecoder_eof_tag_but_no_data() -> Result<()> {
        let block_size = 7;
        let key_bytes = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15];
        let decoder = GcmDecoder::new(block_size, gcm_create_key(key_bytes)?, 0);
        let buf = vec![0; 16];
        let mut frame_reader = FramedRead::new(buf.as_ref(), decoder);

        let result = frame_reader.next().await.expect("Some");
        assert_eq!(result.err().expect("expected an error").to_string(), "AES-GCM stream ended after a tag followed by no data");

        Ok(())
    }

    #[tokio::test]
    async fn test_gcmdecoder_eof_middle_of_tag() -> Result<()> {
        let block_size = 7;
        let key_bytes = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15];
        let decoder = GcmDecoder::new(block_size, gcm_create_key(key_bytes)?, 0);
        let buf = vec![0; 15];
        let mut frame_reader = FramedRead::new(buf.as_ref(), decoder);

        let result = frame_reader.next().await.expect("Some");
        assert_eq!(result.err().expect("expected an error").to_string(), "AES-GCM stream ended in the middle of a tag");

        Ok(())
    }
}
