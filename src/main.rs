use std::time::Duration;

use async_std::task;
use configurator::{Configuration, ConfigurationError};
use futures::FutureExt;

mod cache_handler;
mod configurator;
mod request_handler;
mod request_parser;

#[async_std::main]
async fn main() {
    match Configuration::load_from_file() {
        Ok(config) => {
            let app_thread_pool = config.create_thread_pool().unwrap();
            let main_handle = async move {
                let handler = config.create_request_handler().await.unwrap();
                handler.start_handling(config.create_hash_map()).await;
            }
            .boxed();
            app_thread_pool.spawn_ok(main_handle);
            loop {
                task::sleep(Duration::from_secs(1)).await
            }
        }
        Err(err) => match err {
            ConfigurationError::FailureParsingJson(parse_err) => println!("{}", parse_err),
            ConfigurationError::MissingFile(err) => println!("{}", err),
        },
    }
}
