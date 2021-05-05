use std::time::Duration;

#[derive(Debug)]
pub struct BenchmarkResult {
    pub push_requests_per_second: u64,
    pub push_benchmark_time_to_complete: Duration,
    pub get_requests_per_second: u64,
    pub get_benchmark_time_to_complete: Duration,
    pub cache_entries_inserted: usize,
    pub cache_entries_retrieved: usize,
}
