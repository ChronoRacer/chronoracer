use crate::processing::avro::TryFromAvro;
use anyhow::{Result, anyhow};
use apache_avro::reader::datum::GenericDatumReader;
use serde::Deserialize;
use std::io::Cursor;

#[derive(Deserialize, Debug)]
pub struct RelativeSensorReading {
    pub value: f64,
    pub delta: u32,
}

#[derive(Deserialize, Debug, PartialEq)]
pub struct AbsoluteSensorReading {
    pub value: f64,
    pub timestamp: u64,
}

impl RelativeSensorReading {
    fn to_absolute(&self, base_timestamp: u64) -> AbsoluteSensorReading {
        AbsoluteSensorReading {
            value: self.value,
            timestamp: base_timestamp + u64::from(self.delta),
        }
    }
}

#[derive(Deserialize, Debug)]
pub struct SensorDataMessage {
    pub sensor_id: String,
    pub timestamp: u64,
    pub readings: Vec<RelativeSensorReading>,
}

#[derive(Debug)]
pub struct UnwoundSensorDataMessage {
    pub sensor_id: String,
    pub readings: Box<[AbsoluteSensorReading]>,
}

impl From<SensorDataMessage> for UnwoundSensorDataMessage {
    fn from(message: SensorDataMessage) -> Self {
        Self {
            sensor_id: message.sensor_id,
            readings: message
                .readings
                .into_iter()
                .map(|reading| reading.to_absolute(message.timestamp))
                .collect::<Box<[AbsoluteSensorReading]>>(),
        }
    }
}

impl TryFromAvro for SensorDataMessage {
    fn try_from_avro(reader: &GenericDatumReader<'_>, bytes: Option<&[u8]>) -> Result<Self> {
        let bytes =
            bytes.ok_or_else(|| anyhow!("The Avro payload was empty or missing expected data"))?;
        let mut cursor = Cursor::new(bytes);
        let value = reader.read_value(&mut cursor)?;
        Ok(apache_avro::from_value::<SensorDataMessage>(&value)?)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::processing::avro::{AvroSchema, SchemaRegistry};
    use apache_avro::Schema;
    use apache_avro::types::Value;
    use apache_avro::writer::datum::GenericDatumWriter;

    fn sensor_data_schema() -> Schema {
        AvroSchema::point_env_var_to_workspace();
        SchemaRegistry::load_and_parse_from_env(AvroSchema::SensorData)
            .expect("sensor data schema should load")
    }

    fn encode_sensor_data(schema: &Schema) -> Vec<u8> {
        let value = Value::Record(vec![
            (
                "sensor_id".to_string(),
                Value::String("sensor-42".to_string()),
            ),
            (
                "timestamp".to_string(),
                Value::TimestampNanos(1_000_000_000),
            ),
            (
                "readings".to_string(),
                Value::Array(vec![
                    Value::Record(vec![
                        ("delta".to_string(), Value::Int(12)),
                        ("value".to_string(), Value::Double(21.5)),
                    ]),
                    Value::Record(vec![
                        ("delta".to_string(), Value::Int(345)),
                        ("value".to_string(), Value::Double(-4.25)),
                    ]),
                ]),
            ),
        ]);
        let writer = GenericDatumWriter::builder(schema)
            .build()
            .expect("sensor data reader should build");
        writer.write_value_to_vec(value).expect("Failed to serialize to raw datum")
    }

    #[test]
    fn sensor_data_message_unwind() {
        let sensor_id = "test";
        let message = SensorDataMessage {
            sensor_id: sensor_id.to_string(),
            timestamp: 100000,
            readings: vec![
                RelativeSensorReading {
                    value: 1.0,
                    delta: 2,
                },
                RelativeSensorReading {
                    value: 2.0,
                    delta: 3,
                },
                RelativeSensorReading {
                    value: 3.0,
                    delta: 4,
                },
            ],
        };
        let unwound_message = UnwoundSensorDataMessage::from(message);
        let expected_unwound_readings = vec![
            AbsoluteSensorReading {
                value: 1.0,
                timestamp: 100_002,
            },
            AbsoluteSensorReading {
                value: 2.0,
                timestamp: 100_003,
            },
            AbsoluteSensorReading {
                value: 3.0,
                timestamp: 100_004,
            },
        ];
        assert_eq!(
            unwound_message.readings,
            expected_unwound_readings.into_boxed_slice()
        );
        assert_eq!(unwound_message.sensor_id, sensor_id);
    }

    #[test]
    fn relative_sensor_reading_to_absolute() {
        let delta = 7;
        let relative_reading = RelativeSensorReading {
            value: 10.0,
            delta: delta as u32,
        };
        let base_timestamp = 100000;
        let absolute_reading = relative_reading.to_absolute(base_timestamp);
        assert_eq!(absolute_reading.value, 10.0);
        assert_eq!(absolute_reading.timestamp, base_timestamp + delta as u64);
    }

    #[test]
    fn sensor_data_message_deserializes_from_avro() {
        let schema = sensor_data_schema();
        let reader = GenericDatumReader::builder(&schema)
            .build()
            .expect("sensor data reader should build");
        let message = SensorDataMessage::try_from_avro(&reader, Some(&encode_sensor_data(&schema)))
            .expect("valid sensor data should deserialize");

        assert_eq!(message.sensor_id, "sensor-42");
        assert_eq!(message.timestamp, 1_000_000_000);
        assert_eq!(message.readings.len(), 2);
        assert_eq!(message.readings[0].value, 21.5);
        assert_eq!(message.readings[0].delta, 12);
        assert_eq!(message.readings[1].value, -4.25);
        assert_eq!(message.readings[1].delta, 345);
    }

    #[test]
    fn sensor_data_message_rejects_missing_payload() {
        let schema = sensor_data_schema();
        let reader = GenericDatumReader::builder(&schema)
            .build()
            .expect("sensor data reader should build");

        let error = SensorDataMessage::try_from_avro(&reader, None)
            .expect_err("missing payload should fail");

        assert_eq!(
            error.to_string(),
            "The Avro payload was empty or missing expected data"
        );
    }

    #[test]
    fn sensor_data_message_rejects_malformed_payload() {
        let schema = sensor_data_schema();
        let reader = GenericDatumReader::builder(&schema)
            .build()
            .expect("sensor data reader should build");

        let result = SensorDataMessage::try_from_avro(&reader, Some(&[0xff, 0xff, 0xff]));

        assert!(result.is_err());
    }

    #[test]
    fn unwinding_an_empty_message_preserves_sensor_id() {
        let message = SensorDataMessage {
            sensor_id: "empty-sensor".to_string(),
            timestamp: u64::MAX,
            readings: Vec::new(),
        };

        let unwound = UnwoundSensorDataMessage::from(message);

        assert_eq!(unwound.sensor_id, "empty-sensor");
        assert!(unwound.readings.is_empty());
    }

    #[test]
    fn relative_reading_with_max_delta_is_unwound_without_loss() {
        let message = SensorDataMessage {
            sensor_id: "boundary-sensor".to_string(),
            timestamp: 0,
            readings: vec![RelativeSensorReading {
                value: f64::MIN,
                delta: u32::MAX,
            }],
        };

        let unwound = UnwoundSensorDataMessage::from(message);

        assert_eq!(unwound.readings[0].value, f64::MIN);
        assert_eq!(unwound.readings[0].timestamp, u64::from(u32::MAX));
    }
}
