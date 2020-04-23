use anyhow::{anyhow, Result};
use byteorder::{BigEndian, WriteBytesExt};
use ring::aead::{LessSafeKey, Nonce, Aad, Tag};

pub(crate) fn iv_for_block_number(block_number: u64) -> Box<[u8; 12]> {
    let mut iv = Box::new([0; 12]);
    (&mut iv[4..]).write_u64::<BigEndian>(block_number).unwrap();
    iv
}

fn gcm_encrypt_block(key: &LessSafeKey, block_number: u64, in_out: &mut [u8]) -> Result<Tag> {
    let nonce = Nonce::assume_unique_for_key(*iv_for_block_number(block_number));
    let tag = key.seal_in_place_separate_tag(nonce, Aad::empty(), in_out).map_err(|_| anyhow!("Failed to seal"))?;
    Ok(tag)
}

#[cfg(test)]
mod tests {
    use super::*;
    use ring::aead::{UnboundKey, AES_128_GCM};

    #[test]
	fn test_iv_for_block_number() {
        assert_eq!(iv_for_block_number(0).to_vec(),                 b"\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00");
        assert_eq!(iv_for_block_number(1).to_vec(),                 b"\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x01");
        assert_eq!(iv_for_block_number(100).to_vec(),               b"\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x64");
        assert_eq!(iv_for_block_number(2_u64.pow(53) - 1).to_vec(), b"\x00\x00\x00\x00\x00\x1f\xff\xff\xff\xff\xff\xff");
        assert_eq!(iv_for_block_number(u64::MAX).to_vec(),          b"\x00\x00\x00\x00\xff\xff\xff\xff\xff\xff\xff\xff");
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
}
