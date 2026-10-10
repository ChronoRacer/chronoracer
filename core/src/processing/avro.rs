//! Avro binary primitives for the fixed sensor data schema
//! ([`core/avro/sensor_data.avsc`](../../avro/sensor_data.avsc)).

use std::fmt;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DecodeError {
    MissingPayload,
    Truncated,
    VarintOverflow,
    IntOutOfRange,
    NegativeLength,
    InvalidUtf8,
    NegativeTimestamp,
    NegativeDelta,
    TimestampOverflow,
    ItemCountExceedsInput,
    BlockSizeMismatch,
}

impl fmt::Display for DecodeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::MissingPayload => "The Avro payload was empty or missing expected data",
            Self::Truncated => "Avro payload ended before the datum was complete",
            Self::VarintOverflow => "Avro varint does not fit in 64 bits",
            Self::IntOutOfRange => "Avro int does not fit in 32 bits",
            Self::NegativeLength => "Avro length or block size is negative",
            Self::InvalidUtf8 => "Avro string is not valid UTF-8",
            Self::NegativeTimestamp => "Sensor data timestamp is negative",
            Self::NegativeDelta => "Sensor reading delta is negative",
            Self::TimestampOverflow => "Sensor reading timestamp overflows u64",
            Self::ItemCountExceedsInput => {
                "Avro array block declares more items than the payload can hold"
            }
            Self::BlockSizeMismatch => "Avro array block byte size does not match its items",
        })
    }
}

impl std::error::Error for DecodeError {}

/// Cursor over one Avro binary datum. Every read is bounds-checked and never allocates.
pub struct AvroReader<'a> {
    buf: &'a [u8],
}

impl<'a> AvroReader<'a> {
    #[must_use]
    pub fn new(buf: &'a [u8]) -> Self {
        Self { buf }
    }

    #[must_use]
    pub fn remaining(&self) -> usize {
        self.buf.len()
    }

    fn take(&mut self, len: usize) -> Result<&'a [u8], DecodeError> {
        if len > self.buf.len() {
            return Err(DecodeError::Truncated);
        }
        let (head, tail) = self.buf.split_at(len);
        self.buf = tail;
        Ok(head)
    }

    /// Zigzag varint. Non-canonical (zero-padded) encodings are accepted, like `apache-avro` does.
    #[allow(clippy::cast_possible_wrap)]
    pub fn read_long(&mut self) -> Result<i64, DecodeError> {
        let mut acc = 0u64;
        for shift in (0..=63).step_by(7) {
            let byte = self.take(1)?[0];
            // The tenth byte carries the single top bit and cannot continue.
            if shift == 63 && byte > 1 {
                return Err(DecodeError::VarintOverflow);
            }
            acc |= u64::from(byte & 0x7f) << shift;
            if byte & 0x80 == 0 {
                return Ok(((acc >> 1) as i64) ^ -((acc & 1) as i64));
            }
        }
        Err(DecodeError::VarintOverflow)
    }

    pub fn read_int(&mut self) -> Result<i32, DecodeError> {
        i32::try_from(self.read_long()?).map_err(|_| DecodeError::IntOutOfRange)
    }

    pub fn read_double(&mut self) -> Result<f64, DecodeError> {
        let bytes = self.take(8)?;
        Ok(f64::from_le_bytes(
            bytes.try_into().expect("take(8) returns 8 bytes"),
        ))
    }

    /// Borrowed string; the length is checked against the remaining input before any use.
    pub fn read_str(&mut self) -> Result<&'a str, DecodeError> {
        let len = usize::try_from(self.read_long()?).map_err(|_| DecodeError::NegativeLength)?;
        std::str::from_utf8(self.take(len)?).map_err(|_| DecodeError::InvalidUtf8)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn long(bytes: &[u8]) -> Result<i64, DecodeError> {
        AvroReader::new(bytes).read_long()
    }

    #[test]
    fn read_long_decodes_zigzag() {
        assert_eq!(long(&[0x00]), Ok(0));
        assert_eq!(long(&[0x01]), Ok(-1));
        assert_eq!(long(&[0x02]), Ok(1));
        assert_eq!(long(&[0x03]), Ok(-2));
        assert_eq!(long(&[0x80, 0x01]), Ok(64));
        assert_eq!(
            long(&[0xfe, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x01]),
            Ok(i64::MAX)
        );
        assert_eq!(
            long(&[0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x01]),
            Ok(i64::MIN)
        );
    }

    #[test]
    fn read_long_rejects_overflow_and_truncation() {
        assert_eq!(long(&[]), Err(DecodeError::Truncated));
        assert_eq!(long(&[0x80]), Err(DecodeError::Truncated));
        assert_eq!(long(&[0x80; 9]), Err(DecodeError::Truncated));
        // Tenth byte with bits above the 64th, or a continuation, or an eleventh byte.
        assert_eq!(
            long(&[0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x02]),
            Err(DecodeError::VarintOverflow)
        );
        assert_eq!(long(&[0xff; 10]), Err(DecodeError::VarintOverflow));
        assert_eq!(long(&[0x80; 11]), Err(DecodeError::VarintOverflow));
    }

    #[test]
    fn read_int_rejects_values_outside_i32() {
        // zigzag(2^31) = 2^32 = varint 80 80 80 80 10
        assert_eq!(
            AvroReader::new(&[0x80, 0x80, 0x80, 0x80, 0x10]).read_int(),
            Err(DecodeError::IntOutOfRange)
        );
        // zigzag(i32::MAX) = 0xFFFF_FFFE
        assert_eq!(
            AvroReader::new(&[0xfe, 0xff, 0xff, 0xff, 0x0f]).read_int(),
            Ok(i32::MAX)
        );
    }

    #[test]
    fn read_str_checks_length_and_utf8() {
        assert_eq!(
            AvroReader::new(&[0x06, b'a', b'b', b'c']).read_str(),
            Ok("abc")
        );
        assert_eq!(AvroReader::new(&[0x00]).read_str(), Ok(""));
        assert_eq!(
            AvroReader::new(&[0x06, b'a', b'b']).read_str(),
            Err(DecodeError::Truncated)
        );
        assert_eq!(
            AvroReader::new(&[0x01]).read_str(),
            Err(DecodeError::NegativeLength)
        );
        assert_eq!(
            AvroReader::new(&[0x04, 0xff, 0xfe]).read_str(),
            Err(DecodeError::InvalidUtf8)
        );
        // Length far beyond the input must not allocate or panic.
        assert_eq!(
            AvroReader::new(&[0xfe, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x01])
                .read_str(),
            Err(DecodeError::Truncated)
        );
    }

    #[test]
    fn read_double_needs_eight_bytes() {
        assert_eq!(
            AvroReader::new(&1.5f64.to_le_bytes()).read_double(),
            Ok(1.5)
        );
        assert_eq!(
            AvroReader::new(&[0; 7]).read_double(),
            Err(DecodeError::Truncated)
        );
    }
}
