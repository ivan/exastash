use anyhow::{anyhow, Result};
use byteorder::{BigEndian, WriteBytesExt};
use ring::aead::{LessSafeKey, Nonce, Aad, Tag};

#[inline(always)]
pub(crate) fn write_gcm_iv_for_block_number(buf: &mut [u8; 12], block_number: u64) {
    (&mut buf[4..]).write_u64::<BigEndian>(block_number).unwrap();
}

#[inline]
fn gcm_encrypt_block(key: &LessSafeKey, block_number: u64, in_out: &mut [u8]) -> Result<Tag> {
    let mut iv = [0; 12];
    write_gcm_iv_for_block_number(&mut iv, block_number);
    let nonce = Nonce::assume_unique_for_key(iv);
    let tag = key.seal_in_place_separate_tag(nonce, Aad::empty(), in_out).map_err(|_| anyhow!("Failed to seal"))?;
    Ok(tag)
}

#[inline]
fn gcm_decrypt_block(key: &LessSafeKey, block_number: u64, in_out: &mut [u8], tag: &Tag) -> Result<()> {
    let mut iv = [0; 12];
    write_gcm_iv_for_block_number(&mut iv, block_number);
    let nonce = Nonce::assume_unique_for_key(iv);
    key.open_in_place_separate_tag(nonce, Aad::empty(), in_out, tag).map_err(|_| anyhow!("Failed to open"))?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use ring::aead::{UnboundKey, AES_128_GCM};

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
        let key = LessSafeKey::new(
            UnboundKey::new(&AES_128_GCM, &[0; 16])
                .map_err(|_| anyhow!("ring failed to create key"))?
        );
        let mut in_out = vec![0; 10];
        let block_number = 0;
        let tag = gcm_encrypt_block(&key, block_number, &mut in_out)?;
        assert_eq!(tag.as_ref(), [216, 233, 87, 141, 195, 160, 86, 118, 56, 169, 213, 238, 142, 121, 81, 181]);
        assert_eq!(in_out, [3, 136, 218, 206, 96, 182, 163, 146, 243, 40]);
        Ok(())
    }
    
    #[test]
	fn test_gcm_decrypt_block() -> Result<()> {
        let key = LessSafeKey::new(
            UnboundKey::new(&AES_128_GCM, &[0; 16])
                .map_err(|_| anyhow!("ring failed to create key"))?
        );
        let block_number = 0;
        let tag = Tag::new(&[216, 233, 87, 141, 195, 160, 86, 118, 56, 169, 213, 238, 142, 121, 81, 181]).expect("tag of wrong length?");
        let mut in_out = vec![3, 136, 218, 206, 96, 182, 163, 146, 243, 40];
        gcm_decrypt_block(&key, block_number, &mut in_out, &tag)?;
        assert_eq!(in_out, [0; 10]);
        Ok(())
    }

}
