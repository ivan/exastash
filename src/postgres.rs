//! Some wrappers implementing ToSql/FromSql to get things in and of postgres.

use bytes::{BytesMut, BufMut};
use byteorder::{BigEndian, ReadBytesExt};
use postgres_types::{Type, FromSql, ToSql, IsNull, accepts, to_sql_checked};
use postgres_protocol::types;

/// 16 bytes stored in a PostgreSQL uuid
#[derive(Debug)]
pub(crate) struct SixteenBytes {
    pub bytes: [u8; 16]
}

impl<'a> FromSql<'a> for SixteenBytes {
    fn from_sql(_: &Type, raw: &[u8]) -> std::result::Result<SixteenBytes, Box<dyn std::error::Error + Sync + Send>> {
        let bytes = types::uuid_from_sql(raw)?;
        Ok(SixteenBytes { bytes })
    }

    accepts!(UUID);
}

impl ToSql for SixteenBytes {
    fn to_sql(&self, _: &Type, w: &mut BytesMut) -> Result<IsNull, Box<dyn std::error::Error + Sync + Send>> {
        types::uuid_to_sql(self.bytes, w);
        Ok(IsNull::No)
    }

    accepts!(UUID);
    to_sql_checked!();
}


/// u32 stored in a PostgreSQL int
#[derive(Debug)]
pub(crate) struct UnsignedInt4 {
    pub value: u32
}

impl<'a> FromSql<'a> for UnsignedInt4 {
    fn from_sql(_: &Type, mut raw: &[u8]) -> std::result::Result<UnsignedInt4, Box<dyn std::error::Error + Sync + Send>> {
        if raw.len() != 4 {
            return Err("expected to get 4 bytes".into());
        }
        let value = raw.read_u32::<BigEndian>()?;
        Ok(UnsignedInt4 { value })
    }

    accepts!(INT4);
}

impl ToSql for UnsignedInt4 {
    fn to_sql(&self, _: &Type, w: &mut BytesMut) -> Result<IsNull, Box<dyn std::error::Error + Sync + Send>> {
        w.put_u32(self.value);
        Ok(IsNull::No)
    }

    accepts!(INT4);
    to_sql_checked!();
}
