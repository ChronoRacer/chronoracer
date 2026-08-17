use crate::processing::avro::{AvroErrorExtended, TryFromAvro};
use apache_avro::Schema;
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
    fn to_absolute(self: &Self, base_timestamp: &u64) -> AbsoluteSensorReading {
        AbsoluteSensorReading {
            value: self.value,
            timestamp: base_timestamp + self.delta as u64,
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
                .map(|reading| RelativeSensorReading::to_absolute(&reading, &message.timestamp))
                .collect::<Box<[AbsoluteSensorReading]>>(),
        }
    }
}

impl TryFromAvro for SensorDataMessage {
    fn try_from_avro(schema: &Schema, bytes: Option<&[u8]>) -> Result<Self, AvroErrorExtended> {
        if let Some(avro_value) = bytes {
            let mut cursor = Cursor::new(avro_value);
            let value = apache_avro::from_avro_datum(schema, &mut cursor, None)?;
            return Ok(apache_avro::from_value::<SensorDataMessage>(&value)?);
        }
        Err(AvroErrorExtended::EmptyPayload)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn SensorDataMessage_unwind() {
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
                timestamp: 100002,
            },
            AbsoluteSensorReading {
                value: 2.0,
                timestamp: 100003,
            },
            AbsoluteSensorReading {
                value: 3.0,
                timestamp: 100004,
            },
        ];
        assert_eq!(unwound_message.readings, expected_unwound_readings.into_boxed_slice());
        assert_eq!(unwound_message.sensor_id, sensor_id);
    }

    #[test]
    fn RelativeSensorReading_to_absolute() {
        let delta = 7;
        let relative_reading = RelativeSensorReading {
            value: 10.0,
            delta: delta as u32,
        };
        let base_timestamp = 100000;
        let absolute_reading = relative_reading.to_absolute(&base_timestamp);
        assert_eq!(absolute_reading.value, 10.0);
        assert_eq!(absolute_reading.timestamp, base_timestamp + delta as u64);
    }
}
