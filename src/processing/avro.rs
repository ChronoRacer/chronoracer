use apache_avro::Schema;
use thiserror::Error;

pub struct SchemaRegistry {}

pub enum AvroSchema {
    SensorData,
}

impl AvroSchema {
    fn env_var(&self) -> &'static str {
        match *self {
            AvroSchema::SensorData => "AVRO_MESSAGE_SCHEMA_PATH",
        }
    }
}

impl SchemaRegistry {
    pub fn load_and_parse_from_env(
        schema_type: AvroSchema,
    ) -> Result<Schema, Box<dyn std::error::Error>> {
        let env_var = schema_type.env_var();
        let schema_path = std::env::var(env_var)
            .map_err(|_| format!("Environment variable '{}' not set", env_var))?;
        let schema_string = std::fs::read_to_string(schema_path)?;
        Schema::parse_str(&schema_string).map_err(|err| { err.into() })
    }
}

#[derive(Error, Debug)]
pub enum AvroErrorExtended {
    #[error("Avro error: {0}")]
    Avro(#[from] apache_avro::Error),

    #[error("The Avro payload was empty or missing expected data")]
    EmptyPayload,
}

pub trait TryFromAvro: Sized {
    fn try_from_avro(schema: &Schema, avro_value: Option<&[u8]>) -> Result<Self, AvroErrorExtended>;
}

#[cfg(test)]
mod tests {
    use crate::processing::avro::{AvroSchema, SchemaRegistry};
    use std::env;
    use std::path::PathBuf;

    #[test]
    fn load_and_parse_from_env_fails_on_missing_env_var() {
        unsafe {
            env::remove_var(AvroSchema::SensorData.env_var());
        }
        let loaded = SchemaRegistry::load_and_parse_from_env(AvroSchema::SensorData);
        assert!(loaded.is_err());
        assert!(
            loaded
                .unwrap_err()
                .to_string()
                .contains("Environment variable")
        );
    }

    #[test]
    fn load_and_parse_from_env_schema_loads_correctly() {
        let manifest_dir = env!("CARGO_MANIFEST_DIR");
        let file_path = PathBuf::from(manifest_dir)
            .join("avro")
            .join("sensor_data.avsc");
        unsafe {
            env::set_var(
                AvroSchema::SensorData.env_var(),
                file_path.to_str().unwrap(),
            );
        }
        let loaded = SchemaRegistry::load_and_parse_from_env(AvroSchema::SensorData);
        assert!(loaded.is_ok());
    }
}
