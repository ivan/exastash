use byteorder::{BigEndian, WriteBytesExt};

pub(crate) fn iv_for_block_number(block_number: u64) -> Vec<u8> {
    let mut iv = vec![0; 12];
    (&mut iv[4..]).write_u64::<BigEndian>(block_number).unwrap();
    iv
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
	fn test_iv_for_block_number() {
        assert_eq!(iv_for_block_number(0),                 b"\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00");
        assert_eq!(iv_for_block_number(1),                 b"\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x01");
        assert_eq!(iv_for_block_number(100),               b"\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x64");
        assert_eq!(iv_for_block_number(2_u64.pow(53) - 1), b"\x00\x00\x00\x00\x00\x1f\xff\xff\xff\xff\xff\xff");
        assert_eq!(iv_for_block_number(u64::MAX),          b"\x00\x00\x00\x00\xff\xff\xff\xff\xff\xff\xff\xff");
    }
}
