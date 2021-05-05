use std::{fs::File, io::Read, net::IpAddr};

use dashmap::DashMap;
use futures_executor::{ThreadPool, ThreadPoolBuilder};
use serde::{Deserialize, Serialize};

use crate::request_handler::RequestHandler;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Auth {
    pub username: String,
    pub password: String,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct Configuration {
    pub max_threads: usize,
    pub default_cache_size: usize,
    pub port: usize,
    pub address: IpAddr,
    pub auth: Option<Auth>,
    pub buffer_size: Option<usize>,
}

pub enum ConfigurationError {
    MissingFile(&'static str),
    FailureParsingJson(String),
}

impl Configuration {
    pub fn load_from_file() -> Result<Configuration, ConfigurationError> {
        match File::open("config.json") {
            Ok(mut file) => {
                let mut buffer = String::new();
                file.read_to_string(&mut buffer).unwrap();
                let config_parse: Result<Configuration, serde_json::Error> =
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
                "Missing 'config.json' file",
            )),
        }
    }

    pub fn create_thread_pool(&self) -> Result<ThreadPool, std::io::Error> {
        let mut builder = ThreadPoolBuilder::new();
        builder.pool_size(self.max_threads);
        builder.create()
    }

    pub fn create_hash_map(&self) -> DashMap<String, Vec<u8>> {
        DashMap::with_capacity(self.default_cache_size)
    }

    pub async fn create_request_handler(&self) -> Result<RequestHandler, &'static str> {
        match self.address {
            IpAddr::V4(v4_address) => RequestHandler::new(v4_address, self.port).await,
            IpAddr::V6(_) => panic!("V6 address is currently not supported"),
        }
    }
}
