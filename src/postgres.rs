//! Some wrappers implementing ToSql/FromSql to get things in and of postgres.

use bytes::{BytesMut, BufMut};
use byteorder::{BigEndian, ReadBytesExt};
use postgres_types::{Type, FromSql, ToSql, IsNull, accepts, to_sql_checked};
use postgres_protocol::types;

/// MD5 hash stored in a PostgreSQL uuid
#[derive(Debug)]
pub(crate) struct Md5 {
    pub bytes: [u8; 16]
}

impl<'a> FromSql<'a> for Md5 {
    fn from_sql(_: &Type, raw: &[u8]) -> std::result::Result<Md5, Box<dyn std::error::Error + Sync + Send>> {
        let bytes = types::uuid_from_sql(raw)?;
        Ok(Md5 { bytes })
    }

    accepts!(UUID);
}

impl ToSql for Md5 {
    fn to_sql(&self, _: &Type, w: &mut BytesMut) -> Result<IsNull, Box<dyn std::error::Error + Sync + Send>> {
        types::uuid_to_sql(self.bytes, w);
        Ok(IsNull::No)
    }

    accepts!(UUID);
    to_sql_checked!();
}


/// CRC32C stored in a PostgreSQL int
#[derive(Debug)]
pub(crate) struct Crc32c {
    pub v: u32
}

impl<'a> FromSql<'a> for Crc32c {
    fn from_sql(_: &Type, mut raw: &[u8]) -> std::result::Result<Crc32c, Box<dyn std::error::Error + Sync + Send>> {
        if raw.len() != 4 {
            return Err("invalid message length: crc32c size mismatch".into());
        }
        let v = raw.read_u32::<BigEndian>()?;
        Ok(Crc32c { v })
    }

    accepts!(INT4);
}

impl ToSql for Crc32c {
    fn to_sql(&self, _: &Type, w: &mut BytesMut) -> Result<IsNull, Box<dyn std::error::Error + Sync + Send>> {
        w.put_u32(self.v);
        Ok(IsNull::No)
    }

    accepts!(INT4);
    to_sql_checked!();
}
