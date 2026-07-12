use crate::messaging::avro::TryFromAvro;
use apache_avro::Schema;
use serde::Deserialize;
use thiserror::Error;

#[derive(Deserialize, Debug)]
pub struct RelativeSensorReading {
    pub value: f64,
    pub delta: i16,
}

#[derive(Deserialize, Debug, PartialEq)]
pub struct AbsoluteSensorReading {
    pub value: f64,
    pub timestamp: i64,
}

impl RelativeSensorReading {
    fn to_absolute(self: &Self, base_timestamp: &i64) -> AbsoluteSensorReading {
        AbsoluteSensorReading {
            value: self.value,
            timestamp: base_timestamp + self.delta as i64,
        }
    }
}

#[derive(Deserialize, Debug)]
pub struct SensorDataMessage {
    pub sensor_id: String,
    pub timestamp: i64,
    pub readings: Vec<RelativeSensorReading>,
}

#[derive(Debug)]
pub struct UnwoundSensorDataMessage {
    pub sensor_id: String,
    pub readings: Vec<AbsoluteSensorReading>,
}

impl From<SensorDataMessage> for UnwoundSensorDataMessage {
    fn from(message: SensorDataMessage) -> Self {
        Self {
            sensor_id: message.sensor_id,
            readings: message
                .readings
                .into_iter()
                .map(|reading| RelativeSensorReading::to_absolute(&reading, &message.timestamp))
                .collect(),
        }
    }
}

#[derive(Error, Debug)]
pub enum SensorDataError {
    #[error("Avro error: {0}")]
    Avro(#[from] apache_avro::Error),

    #[error("The Avro payload was empty or missing expected data")]
    EmptyPayload,
}

impl TryFromAvro for SensorDataMessage {
    type Error = SensorDataError;

    fn try_from_avro(schema: &Schema, bytes: Option<&[u8]>) -> Result<Self, Self::Error> {
        if let Some(avro_value) = bytes {
            let mut reader = apache_avro::Reader::with_schema(schema, avro_value)?;
            if let Some(value_result) = reader.next() {
                return Ok(apache_avro::from_value::<SensorDataMessage>(
                    &value_result?,
                )?);
            }
        }
        Err(SensorDataError::EmptyPayload)
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
        assert_eq!(unwound_message.readings, expected_unwound_readings);
        assert_eq!(unwound_message.sensor_id, sensor_id);
    }
}
