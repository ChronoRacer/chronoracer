use crate::messaging::partial_message::SourceOffset;
use crate::processing::avro::{AvroReader, DecodeError};
use std::collections::HashSet;
use std::sync::Arc;

/// Sensor id limit in bytes. `compact_str` inlines `len <= size_of::<String>()` (i.e. hashset does not make pointer hop)
pub const MAX_SENSOR_ID_BYTES: usize = std::mem::size_of::<String>();
const _: () = assert!(MAX_SENSOR_ID_BYTES == 24);

/// Smallest encoded reading: a one-byte `delta` varint and an eight-byte `value`.
const MIN_READING_BYTES: usize = 1 + 8;

/// Distinct ids one interner keeps. The sensor set is small; past the cap the table is cleared so
/// a stream of unique ids cannot grow it.
const MAX_INTERNED_SENSOR_IDS: usize = 1024;

#[derive(Debug, PartialEq)]
pub struct AbsoluteSensorReading {
    pub value: f64,
    pub timestamp: u64,
}

#[derive(Debug)]
pub struct UnwoundSensorDataMessage {
    pub sensor_id: Arc<str>,
    pub readings: Box<[AbsoluteSensorReading]>,
    /// The Kafka message these readings came from: the offset that settles once they are written.
    pub source: SourceOffset,
    /// Kafka record timestamp of that message, for the persist latency metric.
    pub created_ms: Option<i64>,
}

/// Shares one `Arc<str>` per distinct sensor id. Not `Sync`: keep one per decoding thread.
#[derive(Default)]
pub struct SensorIdInterner {
    ids: HashSet<Arc<str>>,
}

impl SensorIdInterner {
    pub fn intern(&mut self, id: &str) -> Arc<str> {
        if let Some(existing) = self.ids.get(id) {
            return Arc::clone(existing);
        }
        if self.ids.len() >= MAX_INTERNED_SENSOR_IDS {
            self.ids.clear();
        }
        let id: Arc<str> = Arc::from(id);
        self.ids.insert(Arc::clone(&id));
        id
    }
}

impl UnwoundSensorDataMessage {
    /// Decodes one Avro datum of [`core/avro/sensor_data.avsc`](../../avro/sensor_data.avsc) straight
    /// into absolute timestamps. Bytes after the datum are ignored (as `apache-avro` did); every
    /// other malformation is an error. Compatibility with `apache-avro` is pinned by the
    /// differential tests below.
    pub fn decode(
        payload: Option<&[u8]>,
        source: SourceOffset,
        created_ms: Option<i64>,
        sensor_ids: &mut SensorIdInterner,
    ) -> Result<Self, DecodeError> {
        let mut reader = AvroReader::new(payload.ok_or(DecodeError::MissingPayload)?);
        let sensor_id = sensor_ids.intern(reader.read_str()?);
        let timestamp =
            u64::try_from(reader.read_long()?).map_err(|_| DecodeError::NegativeTimestamp)?;

        let mut readings = Vec::new();
        loop {
            let count = reader.read_long()?;
            if count == 0 {
                break;
            }
            // A negative count is followed by the block's byte size.
            let expected_end = if count < 0 {
                let size = usize::try_from(reader.read_long()?)
                    .map_err(|_| DecodeError::NegativeLength)?;
                reader
                    .remaining()
                    .checked_sub(size)
                    .ok_or(DecodeError::Truncated)
                    .map(Some)?
            } else {
                None
            };
            // Bounds the allocation by the input, whatever the block claims.
            let count = usize::try_from(count.unsigned_abs())
                .ok()
                .filter(|count| *count <= reader.remaining() / MIN_READING_BYTES)
                .ok_or(DecodeError::ItemCountExceedsInput)?;
            if readings.is_empty() {
                readings.reserve_exact(count);
            } else {
                readings.reserve(count);
            }

            for _ in 0..count {
                let delta =
                    u32::try_from(reader.read_int()?).map_err(|_| DecodeError::NegativeDelta)?;
                let value = reader.read_double()?;
                readings.push(AbsoluteSensorReading {
                    value,
                    timestamp: timestamp
                        .checked_add(u64::from(delta))
                        .ok_or(DecodeError::TimestampOverflow)?,
                });
            }
            if expected_end.is_some_and(|end| end != reader.remaining()) {
                return Err(DecodeError::BlockSizeMismatch);
            }
        }
        Ok(Self {
            sensor_id,
            readings: readings.into_boxed_slice(),
            source,
            created_ms,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use apache_avro::Schema;
    use apache_avro::reader::datum::GenericDatumReader;
    use apache_avro::types::Value;
    use apache_avro::writer::datum::GenericDatumWriter;
    use serde::Deserialize;
    use std::io::Cursor;

    fn sensor_data_schema() -> Schema {
        let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("avro")
            .join("sensor_data.avsc");
        Schema::parse_str(&std::fs::read_to_string(path).expect("schema file"))
            .expect("sensor data schema should parse")
    }

    /// Bit-exact view of a decoded message (`f64` NaN is not `==`).
    type Plain = (String, Vec<(u64, u64)>);

    fn plain(message: &UnwoundSensorDataMessage) -> Plain {
        (
            message.sensor_id.to_string(),
            message
                .readings
                .iter()
                .map(|reading| (reading.timestamp, reading.value.to_bits()))
                .collect(),
        )
    }

    fn decode(bytes: &[u8]) -> Result<Plain, DecodeError> {
        UnwoundSensorDataMessage::decode(
            Some(bytes),
            SourceOffset::default(),
            None,
            &mut SensorIdInterner::default(),
        )
        .map(|message| plain(&message))
    }

    /// The previous implementation: generic `apache-avro` value tree, then serde into the
    /// unsigned fields. `None` means it rejected the payload.
    fn apache_avro_oracle(schema: &Schema, bytes: &[u8]) -> Option<Plain> {
        #[derive(Deserialize)]
        struct Reading {
            value: f64,
            delta: u32,
        }
        #[derive(Deserialize)]
        struct Message {
            sensor_id: String,
            timestamp: u64,
            readings: Vec<Reading>,
        }
        let reader = GenericDatumReader::builder(schema).build().ok()?;
        let value = reader.read_value(&mut Cursor::new(bytes)).ok()?;
        let message = apache_avro::from_value::<Message>(&value).ok()?;
        let readings = message
            .readings
            .iter()
            .map(|reading| {
                message
                    .timestamp
                    .checked_add(u64::from(reading.delta))
                    .map(|timestamp| (timestamp, reading.value.to_bits()))
            })
            .collect::<Option<Vec<_>>>()?;
        Some((message.sensor_id, readings))
    }

    fn apache_avro_encode(
        schema: &Schema,
        sensor_id: &str,
        timestamp: i64,
        readings: &[(i32, f64)],
    ) -> Vec<u8> {
        let record = Value::Record(vec![
            (
                "sensor_id".to_string(),
                Value::String(sensor_id.to_string()),
            ),
            ("timestamp".to_string(), Value::TimestampNanos(timestamp)),
            (
                "readings".to_string(),
                Value::Array(
                    readings
                        .iter()
                        .map(|(delta, value)| {
                            Value::Record(vec![
                                ("delta".to_string(), Value::Int(*delta)),
                                ("value".to_string(), Value::Double(*value)),
                            ])
                        })
                        .collect(),
                ),
            ),
        ]);
        GenericDatumWriter::builder(schema)
            .build()
            .expect("sensor data writer should build")
            .write_value_to_vec(record)
            .expect("Failed to serialize to raw datum")
    }

    #[allow(clippy::cast_possible_truncation)]
    fn put_long(out: &mut Vec<u8>, value: i64) {
        #[allow(clippy::cast_sign_loss)]
        let mut zigzag = ((value << 1) ^ (value >> 63)) as u64;
        while zigzag >= 0x80 {
            out.push((zigzag & 0x7f) as u8 | 0x80);
            zigzag >>= 7;
        }
        out.push(zigzag as u8);
    }

    fn put_reading(out: &mut Vec<u8>, (delta, value): (i32, f64)) {
        put_long(out, i64::from(delta));
        out.extend_from_slice(&value.to_le_bytes());
    }

    fn put_header(out: &mut Vec<u8>, sensor_id: &str, timestamp: i64) {
        put_long(out, i64::try_from(sensor_id.len()).unwrap());
        out.extend_from_slice(sensor_id.as_bytes());
        put_long(out, timestamp);
    }

    enum Block<'a> {
        /// `count` then items.
        Counted(&'a [(i32, f64)]),
        /// `-count`, byte size, then items.
        Sized(&'a [(i32, f64)]),
    }

    fn hand_encode(sensor_id: &str, timestamp: i64, blocks: &[Block]) -> Vec<u8> {
        let mut out = Vec::new();
        put_header(&mut out, sensor_id, timestamp);
        for block in blocks {
            match block {
                Block::Counted(items) => {
                    put_long(&mut out, i64::try_from(items.len()).unwrap());
                    items.iter().for_each(|item| put_reading(&mut out, *item));
                }
                Block::Sized(items) => {
                    let mut body = Vec::new();
                    items.iter().for_each(|item| put_reading(&mut body, *item));
                    put_long(&mut out, -i64::try_from(items.len()).unwrap());
                    put_long(&mut out, i64::try_from(body.len()).unwrap());
                    out.extend_from_slice(&body);
                }
            }
        }
        put_long(&mut out, 0);
        out
    }

    struct Rng(u64);

    impl Rng {
        fn next(&mut self) -> u64 {
            self.0 ^= self.0 << 13;
            self.0 ^= self.0 >> 7;
            self.0 ^= self.0 << 17;
            self.0
        }

        fn below(&mut self, bound: u64) -> u64 {
            self.next() % bound
        }

        fn pick<T: Copy>(&mut self, items: &[T]) -> T {
            items[usize::try_from(self.below(items.len() as u64)).unwrap()]
        }

        fn timestamp(&mut self) -> i64 {
            match self.below(4) {
                0 => self.pick(&[0, 1, i64::MAX, i64::MAX - 1]),
                1 => i64::try_from(self.next() >> 1).unwrap(),
                2 => i64::try_from(self.below(1 << 40)).unwrap(),
                _ => 1_700_000_000_000_000_000 + i64::try_from(self.below(1 << 30)).unwrap(),
            }
        }

        #[allow(clippy::cast_precision_loss)]
        fn reading(&mut self) -> (i32, f64) {
            let delta = match self.below(4) {
                0 => self.pick(&[0, 1, 63, 64, 8191, 8192, i32::MAX]),
                1 => i32::try_from(self.below(2_000_000_001)).unwrap(),
                _ => i32::try_from(self.below(1 << 31)).unwrap(),
            };
            let value = match self.below(4) {
                0 => self.pick(&[
                    0.0,
                    -0.0,
                    f64::NAN,
                    f64::INFINITY,
                    f64::NEG_INFINITY,
                    f64::MIN,
                    f64::MAX,
                    f64::MIN_POSITIVE,
                    5e-324,
                ]),
                1 => f64::from_bits(self.next()),
                _ => (self.next() >> 11) as f64 / 1024.0 - 1.0e6,
            };
            (delta, value)
        }

        fn readings(&mut self, count: usize) -> Vec<(i32, f64)> {
            (0..count).map(|_| self.reading()).collect()
        }

        /// Between 0 and `max_count - 1` readings.
        fn some_readings(&mut self, max_count: u64) -> Vec<(i32, f64)> {
            let count = usize::try_from(self.below(max_count)).unwrap();
            self.readings(count)
        }
    }

    const SENSOR_IDS: [&str; 6] = [
        "",
        "a",
        "sensor-42",
        "abcdefghijklmnopqrstuvwx",
        "czujnik-ł-ś-ż",
        // Two-byte length varint.
        "0123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789",
    ];

    #[test]
    fn decodes_the_documented_example() {
        let schema = sensor_data_schema();
        let bytes = apache_avro_encode(
            &schema,
            "sensor-42",
            1_000_000_000,
            &[(12, 21.5), (345, -4.25)],
        );
        let message = UnwoundSensorDataMessage::decode(
            Some(&bytes),
            SourceOffset::default(),
            None,
            &mut SensorIdInterner::default(),
        )
        .expect("valid sensor data should decode");

        assert_eq!(message.sensor_id.as_ref(), "sensor-42");
        assert_eq!(
            message.readings,
            vec![
                AbsoluteSensorReading {
                    value: 21.5,
                    timestamp: 1_000_000_012,
                },
                AbsoluteSensorReading {
                    value: -4.25,
                    timestamp: 1_000_000_345,
                },
            ]
            .into_boxed_slice()
        );
    }

    #[test]
    fn decodes_an_empty_message_and_preserves_sensor_id() {
        let bytes = apache_avro_encode(&sensor_data_schema(), "empty-sensor", i64::MAX, &[]);
        let message = UnwoundSensorDataMessage::decode(
            Some(&bytes),
            SourceOffset::default(),
            None,
            &mut SensorIdInterner::default(),
        )
        .unwrap();

        assert_eq!(message.sensor_id.as_ref(), "empty-sensor");
        assert!(message.readings.is_empty());
    }

    #[test]
    fn max_delta_is_unwound_without_loss() {
        let bytes = apache_avro_encode(
            &sensor_data_schema(),
            "boundary-sensor",
            i64::MAX,
            &[(i32::MAX, f64::MIN)],
        );
        let message = UnwoundSensorDataMessage::decode(
            Some(&bytes),
            SourceOffset::default(),
            None,
            &mut SensorIdInterner::default(),
        )
        .unwrap();

        assert_eq!(message.readings[0].value, f64::MIN);
        assert_eq!(
            message.readings[0].timestamp,
            u64::try_from(i64::MAX).unwrap() + u64::try_from(i32::MAX).unwrap()
        );
    }

    #[test]
    fn rejects_missing_payload() {
        let error = UnwoundSensorDataMessage::decode(
            None,
            SourceOffset::default(),
            None,
            &mut SensorIdInterner::default(),
        )
        .expect_err("missing payload should fail");

        assert_eq!(
            error.to_string(),
            "The Avro payload was empty or missing expected data"
        );
    }

    #[test]
    #[allow(clippy::too_many_lines)]
    fn rejects_malformed_payloads() {
        let schema = sensor_data_schema();
        let valid = apache_avro_encode(&schema, "s", 10, &[(1, 1.0)]);
        let header = |timestamp: i64| {
            let mut out = Vec::new();
            put_header(&mut out, "s", timestamp);
            out
        };
        let with_tail = |mut prefix: Vec<u8>, tail: &[u8]| {
            prefix.extend_from_slice(tail);
            prefix
        };
        let huge = [0xfe, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x01]; // i64::MAX
        let min = [0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x01]; // i64::MIN

        let cases: Vec<(&str, Vec<u8>, DecodeError)> = vec![
            ("empty", vec![], DecodeError::Truncated),
            ("garbage", vec![0xff, 0xff, 0xff], DecodeError::Truncated),
            (
                "varint longer than 10 bytes",
                vec![0x80; 12],
                DecodeError::VarintOverflow,
            ),
            (
                "varint tenth byte overflows",
                vec![0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x7f],
                DecodeError::VarintOverflow,
            ),
            (
                "negative string length",
                vec![0x01],
                DecodeError::NegativeLength,
            ),
            ("huge string length", huge.to_vec(), DecodeError::Truncated),
            (
                "string length past the end",
                vec![0x0a, b'a'],
                DecodeError::Truncated,
            ),
            (
                "invalid utf-8 sensor id",
                vec![0x04, 0xc3, 0x28, 0x00],
                DecodeError::InvalidUtf8,
            ),
            (
                "missing timestamp",
                vec![0x02, b's'],
                DecodeError::Truncated,
            ),
            (
                "negative timestamp",
                with_tail(header(-1), &[0x00]),
                DecodeError::NegativeTimestamp,
            ),
            (
                "i64::MIN timestamp",
                with_tail(vec![0x02, b's'], &min),
                DecodeError::NegativeTimestamp,
            ),
            ("missing readings", header(10), DecodeError::Truncated),
            (
                "missing array terminator",
                valid[..valid.len() - 1].to_vec(),
                DecodeError::Truncated,
            ),
            (
                "array count beyond input",
                with_tail(header(10), &[0xc8, 0x01, 0x02]),
                DecodeError::ItemCountExceedsInput,
            ),
            (
                "huge array count",
                with_tail(header(10), &huge),
                DecodeError::ItemCountExceedsInput,
            ),
            (
                "i64::MIN array count",
                with_tail(header(10), &min),
                DecodeError::Truncated,
            ),
            (
                "i64::MIN array count with i64::MIN block size",
                with_tail(with_tail(header(10), &min), &min),
                DecodeError::NegativeLength,
            ),
            (
                "negative block size",
                with_tail(header(10), &[0x01, 0x01]),
                DecodeError::NegativeLength,
            ),
            (
                "block size past the end",
                with_tail(header(10), &[0x01, 0x40]),
                DecodeError::Truncated,
            ),
            (
                "negative delta",
                hand_encode("s", 10, &[Block::Counted(&[(-1, 1.0)])]),
                DecodeError::NegativeDelta,
            ),
            (
                "delta past i32",
                with_tail(
                    header(10),
                    &[0x02, 0x80, 0x80, 0x80, 0x80, 0x10, 0, 0, 0, 0, 0, 0, 0, 0],
                ),
                DecodeError::IntOutOfRange,
            ),
            (
                "truncated second reading",
                // Item one has a two-byte delta, so the 18 remaining bytes cannot hold item two.
                with_tail(header(10), &[&[0x04, 0x80, 0x01][..], &[0; 16]].concat()),
                DecodeError::Truncated,
            ),
        ];

        for (name, bytes, expected) in cases {
            assert_eq!(decode(&bytes), Err(expected), "{name}");
            assert_eq!(apache_avro_oracle(&schema, &bytes), None, "{name}: oracle");
        }
    }

    /// `apache-avro` ignores the block byte size; a size that disagrees with the items is corrupt.
    #[test]
    fn rejects_block_size_that_disagrees_with_items() {
        let mut too_small = Vec::new();
        put_header(&mut too_small, "s", 10);
        too_small.extend_from_slice(&[0x01, 0x02]); // -1 item, 1 byte of items
        put_reading(&mut too_small, (1, 1.0));
        put_long(&mut too_small, 0);

        let mut too_large = Vec::new();
        put_header(&mut too_large, "s", 10);
        too_large.extend_from_slice(&[0x01, 0x40]); // -1 item, 32 bytes of items
        put_reading(&mut too_large, (1, 1.0));
        too_large.extend_from_slice(&[0; 40]);

        for bytes in [too_small, too_large] {
            assert_eq!(decode(&bytes), Err(DecodeError::BlockSizeMismatch));
        }
    }

    #[test]
    fn every_truncation_of_a_valid_payload_is_rejected() {
        let schema = sensor_data_schema();
        let mut rng = Rng(0x9e37_79b9_7f4a_7c15);
        for sensor_id in SENSOR_IDS {
            let readings = rng.readings(5);
            let timestamp = rng.timestamp();
            for bytes in [
                apache_avro_encode(&schema, sensor_id, timestamp, &readings),
                hand_encode(
                    sensor_id,
                    timestamp,
                    &[Block::Sized(&readings[..2]), Block::Sized(&readings[2..])],
                ),
            ] {
                assert!(decode(&bytes).is_ok());
                for end in 0..bytes.len() {
                    assert!(decode(&bytes[..end]).is_err(), "prefix of {end} bytes");
                }
            }
        }
    }

    #[test]
    fn trailing_bytes_are_ignored_like_apache_avro() {
        let schema = sensor_data_schema();
        let mut bytes = apache_avro_encode(&schema, "s", 5, &[(1, 2.0)]);
        let expected = decode(&bytes).unwrap();
        bytes.extend_from_slice(b"trailing garbage");

        assert_eq!(decode(&bytes), Ok(expected.clone()));
        assert_eq!(apache_avro_oracle(&schema, &bytes), Some(expected));
    }

    #[test]
    fn decode_carries_the_source_offset_and_timestamp_into_the_message() {
        use crate::messaging::partial_message::MessageOrigin;
        let source = SourceOffset {
            origin: MessageOrigin::SensorData,
            partition: 3,
            offset: 1234,
        };
        let bytes = hand_encode("s", 7, &[Block::Counted(&[(1, 1.5)])]);

        let message = UnwoundSensorDataMessage::decode(
            Some(&bytes),
            source,
            Some(1_700_000_000_000),
            &mut SensorIdInterner::default(),
        )
        .unwrap();

        assert_eq!(message.source, source);
        assert_eq!(message.created_ms, Some(1_700_000_000_000));
    }

    #[test]
    fn matches_apache_avro_writer_output() {
        let schema = sensor_data_schema();
        let mut rng = Rng(0x2545_f491_4f6c_dd1d);
        for round in 0..400 {
            let sensor_id = rng.pick(&SENSOR_IDS);
            let count = match round % 8 {
                0 => 0,
                1 => 1,
                2 => 63,
                3 => 64,
                4 => 65,
                5 => 200,
                _ => usize::try_from(rng.below(40)).unwrap(),
            };
            let timestamp = rng.timestamp();
            let readings = rng.readings(count);
            let bytes = apache_avro_encode(&schema, sensor_id, timestamp, &readings);

            let expected = apache_avro_oracle(&schema, &bytes).expect("oracle accepts");
            assert_eq!(decode(&bytes), Ok(expected), "round {round}");
        }
    }

    #[test]
    fn matches_apache_avro_on_multiple_and_negative_count_blocks() {
        let schema = sensor_data_schema();
        let mut rng = Rng(0x1234_5678_9abc_def1);
        for round in 0..200 {
            let sensor_id = rng.pick(&SENSOR_IDS);
            let timestamp = rng.timestamp();
            let all = rng.some_readings(30);
            let mut blocks = Vec::new();
            let mut rest = all.as_slice();
            while !rest.is_empty() {
                let take = usize::try_from(rng.below(rest.len() as u64)).unwrap() + 1;
                let (chunk, tail) = rest.split_at(take);
                blocks.push(if rng.below(2) == 0 {
                    Block::Counted(chunk)
                } else {
                    Block::Sized(chunk)
                });
                rest = tail;
            }
            let bytes = hand_encode(sensor_id, timestamp, &blocks);

            let expected = apache_avro_oracle(&schema, &bytes).expect("oracle accepts");
            assert_eq!(decode(&bytes), Ok(expected), "round {round}");
        }
    }

    #[test]
    fn matches_apache_avro_on_negative_timestamp_and_delta() {
        let schema = sensor_data_schema();
        for (timestamp, delta) in [(-1, 0), (i64::MIN, 0), (0, -1), (5, i32::MIN), (7, 3)] {
            let bytes = apache_avro_encode(&schema, "s", timestamp, &[(delta, 1.0)]);
            let oracle = apache_avro_oracle(&schema, &bytes);
            assert_eq!(decode(&bytes).ok(), oracle, "ts={timestamp} delta={delta}");
            assert_eq!(oracle.is_some(), timestamp >= 0 && delta >= 0);
        }
    }

    #[test]
    fn corrupted_payloads_never_panic_and_never_disagree_with_apache_avro() {
        let schema = sensor_data_schema();
        let mut rng = Rng(0xdead_beef_cafe_f00d);
        for round in 0..3000 {
            let readings = rng.some_readings(6);
            let mut bytes = apache_avro_encode(
                &schema,
                rng.pick(&SENSOR_IDS[..5]),
                rng.timestamp(),
                &readings,
            );
            for _ in 0..=rng.below(3) {
                let at = usize::try_from(rng.below(bytes.len() as u64)).unwrap();
                #[allow(clippy::cast_possible_truncation)]
                {
                    bytes[at] = rng.next() as u8;
                }
            }
            if let Ok(decoded) = decode(&bytes) {
                assert_eq!(
                    apache_avro_oracle(&schema, &bytes),
                    Some(decoded),
                    "round {round}: accepted by Core only"
                );
            }
        }
    }

    #[test]
    fn interner_shares_ids_and_stays_bounded() {
        let mut interner = SensorIdInterner::default();
        let first = interner.intern("sensor-1");
        assert!(Arc::ptr_eq(&first, &interner.intern("sensor-1")));
        assert!(!Arc::ptr_eq(&first, &interner.intern("sensor-2")));

        for index in 0..(MAX_INTERNED_SENSOR_IDS * 3) {
            interner.intern(&format!("unique-{index}"));
            assert!(interner.ids.len() <= MAX_INTERNED_SENSOR_IDS);
        }
        // Ids handed out before a clear stay valid.
        assert_eq!(first.as_ref(), "sensor-1");
    }
}
