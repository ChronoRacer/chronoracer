#![warn(clippy::pedantic)]

use crate::configuration::CoreConfig;

mod configuration;
pub mod messaging;

fn main() {
    let config = CoreConfig::load_from_env()
        .expect("Failed to load configuration. Config file path not set.");
}
