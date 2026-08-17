use anyhow::{Context, Result};
use apache_avro::Schema;
use std::env;
use std::path::PathBuf;

pub struct SchemaRegistry {}

#[derive(Copy, Clone)]
pub enum AvroSchema {
    SensorData,
}

impl AvroSchema {
    pub fn env_var(&self) -> &'static str {
        match *self {
            AvroSchema::SensorData => "AVRO_MESSAGE_SCHEMA_PATH",
        }
    }

    pub fn point_env_var_to_workspace() {
        let manifest_dir = env!("CARGO_MANIFEST_DIR");
        let file_path = PathBuf::from(manifest_dir)
            .join("avro")
            .join("sensor_data.avsc");
        unsafe {
            env::set_var(
                AvroSchema::SensorData.env_var(),
                file_path.to_string_lossy().as_ref(),
            );
        }
    }
}

impl SchemaRegistry {
    pub fn load_and_parse_from_env(schema_type: AvroSchema) -> Result<Schema> {
        let env_var = schema_type.env_var();
        let schema_path = std::env::var(env_var)
            .with_context(|| format!("Environment variable '{env_var}' not set"))?;
        let schema_string =
            std::fs::read_to_string(schema_path).context("Failed to read Avro schema")?;
        Schema::parse_str(&schema_string).context("Failed to parse Avro schema")
    }
}

pub trait TryFromAvro: Sized {
    fn try_from_avro(schema: &Schema, avro_value: Option<&[u8]>) -> Result<Self>;
}

#[cfg(test)]
mod tests {
    use crate::processing::avro::{AvroSchema, SchemaRegistry};
    use std::env;

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
        AvroSchema::point_env_var_to_workspace();
        let loaded = SchemaRegistry::load_and_parse_from_env(AvroSchema::SensorData);
        assert!(loaded.is_ok());
    }
}
