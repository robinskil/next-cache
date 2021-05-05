use std::{fs::File, io::Read};

use serde::{Deserialize, Serialize};

use crate::configurator::ConfigurationError;

use super::benchmark_result::BenchmarkResult;

#[derive(Debug, Deserialize, Serialize)]
pub struct BenchmarkConfiguration {
    pub host: String,
    pub port: u16,
    pub key_length: u8,
    pub value_length: usize,
    pub total_requests: usize,
    pub num_concurrent_clients: usize,
}

impl BenchmarkConfiguration {
    pub fn load_from_file() -> Result<BenchmarkConfiguration, ConfigurationError> {
        match File::open("benchmark_config.json") {
            Ok(mut file) => {
                let mut buffer = String::new();
                file.read_to_string(&mut buffer).unwrap();
                let config_parse: Result<BenchmarkConfiguration, serde_json::Error> =
                    serde_json::from_str(buffer.as_str());
                match config_parse {
                    Ok(config) => Ok(config),
                    Err(err) => Err(ConfigurationError::FailureParsingJson(format!(
                        "{}{}",
                        String::from("Failed parsing json at line: "),
                        err.line()
                    ))),
                }
            }
            Err(_) => Err(ConfigurationError::MissingFile(
                "Missing 'benchmark_config.json' file",
            )),
        }
    }
}
