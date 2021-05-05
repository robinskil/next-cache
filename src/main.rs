use std::time::Duration;

use async_std::task;
use configurator::{Configuration, ConfigurationError};
use futures::FutureExt;

mod cache_handler;
mod configurator;
mod request_handler;
mod request_parser;
extern crate clap;
mod benchmark;
use benchmark::{
    benchmark_configuration::{self, BenchmarkConfiguration},
    benchmark_runner::BenchmarkRunner,
};
use clap::{App, Arg, ArgMatches, SubCommand};

#[async_std::main]
async fn main() {
    let matches = load_arguments();
    if matches.is_present("benchmark") {
        match BenchmarkConfiguration::load_from_file() {
            Ok(config) => {
                let benchmarker = BenchmarkRunner::new(config);
                println!("{:?}", benchmarker.run_benchmark());
            }
            Err(err) => match err {
                ConfigurationError::FailureParsingJson(parse_err) => println!("{}", parse_err),
                ConfigurationError::MissingFile(err) => println!("{}", err),
            },
        }
    } else {
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
}

fn load_arguments() -> ArgMatches<'static> {
    App::new("Next Cache")
        .version("0.1")
        .author("Robin Kooyman <robin.kooyman.work@gmail.com>")
        .about("Next generation in-memory cache process.")
        .arg(
            Arg::with_name("benchmark")
                .short("b")
                .help("Starts a benchmark using the benchmark_config.json."),
        )
        .get_matches()
}
