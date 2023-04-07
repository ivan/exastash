//! Utility functions

use std::env;
use std::fmt;
use std::path::{Path, PathBuf};
use std::path::Component;
use std::pin::Pin;
use std::sync::Arc;
use std::str::FromStr;
use std::sync::atomic::{AtomicU64, Ordering};
use serde::{de, Deserialize, Deserializer};
use anyhow::{anyhow, Result};
use chrono::{DateTime, Utc, Timelike};
use bytes::{Bytes, BytesMut, Buf};
use tokio_util::codec::Decoder;
use smol_str::SmolStr;
use pin_project::pin_project;
use futures::task::{Context, Poll};
use tokio::io::{AsyncRead, ReadBuf};

pub(crate) fn env_var(var: &str) -> Result<String> {
    use anyhow::Context;
    env::var(var).with_context(|| anyhow!("Could not get variable {:?} from environment", var))
}

/// Get this machine's hostname
pub fn get_hostname() -> String {
    let os_string = gethostname::gethostname();
    let hostname = os_string.to_str().expect("hostname on this machine was not valid UTF-8");
    hostname.to_owned()
}

/// Make a `chrono::DateTime` merely microsecond-precise, making it suitable for
/// round-tripping through PostgreSQL's timestamptz.
pub fn without_nanos<T: chrono::TimeZone>(dt: DateTime<T>) -> DateTime<T> {
    let new_nanos = 1000 * (dt.timestamp_subsec_nanos() / 1000);
    assert_eq!(new_nanos % 1000, 0);
    dt.with_nanosecond(new_nanos).unwrap()
}

/// Assert that a `chrono::DateTime` is merely microsecond-precise,
/// having 000 for nanoseconds.
#[inline]
pub fn assert_without_nanos<T: chrono::TimeZone>(dt: DateTime<T>) {
    assert_eq!(dt.timestamp_subsec_nanos() % 1000, 0, "DateTime with unexpected nanoseconds");
}

/// `chrono::Utc::now()` but merely microsecond-precise, making it suitable for
/// round-tripping through PostgreSQL's timestamptz.
#[inline]
pub fn now_no_nanos() -> DateTime<Utc> {
    without_nanos(Utc::now())
}

// Copied from https://github.com/qryxip/snowchains/blob/dcd76c1dbb87eea239ba17f28b44ee11fdd3fd80/src/macros.rs

/// Return a Lazy<Regex> for the given regexp string
#[macro_export]
macro_rules! lazy_regex {
    ($expr:expr) => {{
        static REGEX: ::once_cell::sync::Lazy<::regex::Regex> =
            ::once_cell::sync::Lazy::new(|| ::regex::Regex::new($expr).unwrap());
        &REGEX
    }};
    ($expr:expr,) => {
        lazy_regex!($expr)
    };
}

// Copied from https://github.com/rust-lang/cargo/blob/af64bd644982cc43b231fb39d7e19f697ec8680d/src/cargo/util/paths.rs#L61

/// Like `std::path::Path::canonicalize`, but don't actually check for
/// the existence of anything on the filesystem
pub fn normalize_path(path: &Path) -> PathBuf {
    let mut components = path.components().peekable();
    let mut ret = if let Some(c @ Component::Prefix(..)) = components.peek().cloned() {
        components.next();
        PathBuf::from(c.as_os_str())
    } else {
        PathBuf::new()
    };

    for component in components {
        match component {
            Component::Prefix(..) => unreachable!(),
            Component::RootDir => {
                ret.push(component.as_os_str());
            }
            Component::CurDir => {}
            Component::ParentDir => {
                ret.pop();
            }
            Component::Normal(c) => {
                ret.push(c);
            }
        }
    }
    ret
}

/// Convert an absolute path to a Vec of String path components
pub(crate) fn utf8_path_to_components(path: &str) -> Vec<String> {
    assert!(path.starts_with('/'));
    let mut parts: Vec<String> = path
        .split('/')
        .skip(1)
        .map(String::from)
        .collect();
    if parts.get(0).unwrap() == "" {
        parts.pop();
    }
    parts
}

// For use with custom_debug_derive::Debug + #[debug(with = "elide")]
#[inline]
pub(crate) fn elide<T>(_: &T, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    write!(f, "...")
}


// Based on https://github.com/Totobird-Creations/Lrinser-Laser-Etcher-Edition-2/blob/81efa5a7355ba3df38e756f62c177d391d8e9060/src/helper.rs
//
/// Commaify a number by the British English rules (not the American English
/// rules because we don't want the '1' in 1000 to align with a comma in 10,000).
pub fn commaify_i64(number: i64) -> String {
    // Fast path
    if number > -1000 && number < 1000 {
        return number.to_string();
    }
    let string = number.abs().to_string();
    let mut result = String::with_capacity(5);
    let chars = string.chars().rev().collect::<Vec<char>>();
    for (i, char) in chars.into_iter().enumerate() {
        if i != 0 && i % 3 == 0 {
            result.push(',');
        }
        result.push(char);
    }
    if number < 0 {
        result.push('-');
    }
    result.chars().rev().collect::<String>()
}


/// Error indicating failure to parse strictly a natural number
#[derive(thiserror::Error, Debug, Clone)]
#[error("could not parse as natural number without leading 0 or +")]
pub struct ParseNaturalNumberError;

/// Parse strictly, forbidding leading '0' or '+'
#[inline]
fn parse_natural_number<T: FromStr>(s: &str) -> Result<T, ParseNaturalNumberError> {
    if s.starts_with('0') || s.starts_with('+') {
        return Err(ParseNaturalNumberError);
    }
    s.parse::<T>().map_err(|_| ParseNaturalNumberError)
}

fn serde_parse_natural_number<'de, D, T: FromStr>(de: D) -> Result<T, D::Error>
where
    D: Deserializer<'de>,
    T: FromStr,
    T::Err: fmt::Display,
{
    let s = SmolStr::deserialize(de)?;
    parse_natural_number(&s).map_err(de::Error::custom)
}

/// Strictly-parsed natural number, forbidding leading '0' or '+'
#[derive(Debug, Deserialize)]
pub(crate) struct NatNum<T: FromStr> (
    #[serde(default, deserialize_with = "serde_parse_natural_number")]
    pub T
) where T::Err: fmt::Display;



/// Decodes an AsyncRead to a stream of Bytes of fixed length,
/// except for the last chunk which may be shorter.
#[expect(missing_copy_implementations)]
#[derive(Debug)]
pub struct FixedReadSizeDecoder {
    /// Length of each Bytes
    chunk_size: usize,
}

impl FixedReadSizeDecoder {
    /// Create a new `FixedReadSizeDecoder`
    pub fn new(chunk_size: usize) -> Self {
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
        Ok(Some(data.copy_to_bytes(data.remaining())))
    }

    // Last chunk is not necessarily full-sized
    fn decode_eof(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        if src.is_empty() {
            return Ok(None);
        }
        Ok(Some(src.copy_to_bytes(src.remaining())))
    }
}



#[pin_project]
pub(crate) struct ByteCountingReader<A: AsyncRead> {
    #[pin]
    inner: A,
    length: Arc<AtomicU64>,
}

impl<A: AsyncRead> ByteCountingReader<A> {
    pub fn new(inner: A) -> ByteCountingReader<A> {
        let length = Arc::new(AtomicU64::new(0));
        ByteCountingReader { inner, length }
    }

    /// Returns an `Arc<AtomicU64>`  of the number of bytes read so far.
    #[inline]
    pub fn length(&self) -> Arc<AtomicU64> {
        self.length.clone()
    }
}

impl<R> AsyncRead for ByteCountingReader<R>
where
    R: AsyncRead,
{
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        let length = self.length();
        let already_filled = buf.filled().len() as u64;
        let inner_poll = self.project().inner.poll_read(cx, buf);
        if let Poll::Ready(Ok(_)) = inner_poll {
            let bytes_read = buf.filled().len() as u64 - already_filled;
            length.fetch_add(bytes_read, Ordering::SeqCst);
        }
        inner_poll
    }
}



#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_without_nanos() {
        let dt000_000 = chrono::DateTime::parse_from_rfc3339("1996-12-19T16:39:57.000000000+00:00").unwrap();
        let dt001_000 = chrono::DateTime::parse_from_rfc3339("1996-12-19T16:39:57.000001000+00:00").unwrap();
        let dt001_999 = chrono::DateTime::parse_from_rfc3339("1996-12-19T16:39:57.000001999+00:00").unwrap();
        assert_ne!(dt000_000, dt001_000);
        assert_ne!(dt001_000, dt001_999);

        assert_eq!(without_nanos(dt001_999), dt001_000);
        assert_eq!(without_nanos(dt001_000), dt001_000);
    }

    #[test]
    fn test_commaify_i64() {
        assert_eq!(commaify_i64(0), "0".to_string());
        assert_eq!(commaify_i64(1), "1".to_string());
        assert_eq!(commaify_i64(-1), "-1".to_string());
        assert_eq!(commaify_i64(999), "999".to_string());
        assert_eq!(commaify_i64(-999), "-999".to_string());
        assert_eq!(commaify_i64(1000), "1,000".to_string());
        assert_eq!(commaify_i64(-1000), "-1,000".to_string());
        assert_eq!(commaify_i64(9999), "9,999".to_string());
        assert_eq!(commaify_i64(-9999), "-9,999".to_string());
        assert_eq!(commaify_i64(10000), "10,000".to_string());
        assert_eq!(commaify_i64(-10000), "-10,000".to_string());
        assert_eq!(commaify_i64(-1000000000000002), "-1,000,000,000,000,002".to_string());
    }
}
