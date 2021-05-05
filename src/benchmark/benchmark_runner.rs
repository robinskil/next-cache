use super::{benchmark_configuration::BenchmarkConfiguration, benchmark_result::BenchmarkResult};
use byteorder::{ByteOrder, LittleEndian, ReadBytesExt};
use rand::distributions::Alphanumeric;
use rand::{thread_rng, Rng};
use std::{
    io::{Read, Write},
    net::TcpStream,
    sync::{atomic::AtomicU32, Arc, Mutex},
    thread::{self, JoinHandle, Thread},
    time::Duration,
    usize,
};
use stopwatch::Stopwatch;

pub struct BenchmarkRunner {
    pub configuration: BenchmarkConfiguration,
    pub request_count: usize,
    pub stream: TcpStream,
}

impl BenchmarkRunner {
    pub fn new(configuration: BenchmarkConfiguration) -> Self {
        BenchmarkRunner {
            request_count: configuration.total_requests,
            stream: TcpStream::connect((configuration.host.clone(), configuration.port)).unwrap(),
            configuration: configuration,
        }
    }

    pub fn generate_random_string(length: usize) -> String {
        thread_rng()
            .sample_iter(&Alphanumeric)
            .take(length)
            .map(char::from)
            .collect()
    }

    pub fn run_benchmark(&self) -> BenchmarkResult {
        let mut push_threads = Vec::with_capacity(self.configuration.num_concurrent_clients);
        let mut get_threads = Vec::with_capacity(self.configuration.num_concurrent_clients);
        let mut request_keys = Vec::with_capacity(self.configuration.total_requests);
        let mut push_requests = Vec::with_capacity(self.configuration.total_requests);
        let mut get_requests: Vec<Vec<u8>> = Vec::with_capacity(self.configuration.total_requests);
        for _ in 0..request_keys.capacity() {
            request_keys.push(BenchmarkRunner::generate_random_string(
                self.configuration.key_length as usize,
            ));
        }
        for _ in 0..request_keys.len() {
            push_requests.push(BenchmarkRunner::create_push_request_buffer(
                &request_keys.pop().unwrap().into_bytes(),
                self.configuration.value_length,
            ));
            // get_requests.push(BenchmarkRunner::create_get_request_buffer(
            //     &request_keys.pop().unwrap().into_bytes(),
            // ));
        }
        println!("{}", push_requests.len());
        println!("{}", get_requests.len());
        let arced_data = Arc::new(push_requests);
        let mut sw = Stopwatch::start_new();
        for thread_id in 0..self.configuration.num_concurrent_clients {
            let cloned_stream = self.stream.try_clone().unwrap();
            let data = arced_data.clone();
            let slice_size =
                self.configuration.total_requests / self.configuration.num_concurrent_clients;
            push_threads.push(thread::spawn(move || {
                BenchmarkRunner::run_push_client(cloned_stream, data, thread_id, slice_size);
            }))
        }
        for thread in push_threads {
            thread.join();
        }
        let push_elapsed_ms = sw.elapsed_ms();
        sw.restart();
        for thread_id in 0..self.configuration.num_concurrent_clients {
            let cloned_stream = self.stream.try_clone().unwrap();
            let data = arced_data.clone();
            let slice_size =
                self.configuration.total_requests / self.configuration.num_concurrent_clients;
            get_threads.push(thread::spawn(move || {
                BenchmarkRunner::run_push_client(cloned_stream, data, thread_id, slice_size);
            }))
        }
        for thread in get_threads {
            thread.join();
        }
        let get_elapsed_ms = sw.elapsed_ms();
        BenchmarkResult {
            cache_entries_inserted: self.configuration.total_requests,
            cache_entries_retrieved: self.configuration.total_requests,
            get_benchmark_time_to_complete: Duration::from_millis(get_elapsed_ms as u64),
            push_benchmark_time_to_complete: Duration::from_millis(push_elapsed_ms as u64),
            get_requests_per_second: self.configuration.total_requests as u64
                / (get_elapsed_ms / 1000) as u64,
            push_requests_per_second: self.configuration.total_requests as u64
                / (push_elapsed_ms / 1000) as u64,
        }
    }

    fn create_push_request_buffer(key: &[u8], value_length: usize) -> Vec<u8> {
        const REQUEST_TYPE: &[u8] = b"PUSH";
        let mut buffer = vec![0; 4 + REQUEST_TYPE.len() + 1 + key.len() + value_length];
        let buffer_len = buffer.len();
        LittleEndian::write_u32_into(&[buffer_len as u32], &mut buffer[0..4]);
        buffer[4..8].copy_from_slice(REQUEST_TYPE);
        buffer[8] = key.len() as u8;
        buffer[9..key.len() + 9].copy_from_slice(key);
        buffer[key.len() + 9..buffer_len].copy_from_slice(&vec![5u8; value_length]);
        buffer
    }

    fn create_get_request_buffer(key: &[u8]) -> Vec<u8> {
        const REQUEST_TYPE: &[u8] = b"GET";
        let mut buffer = vec![0; 4 + REQUEST_TYPE.len() + 1 + key.len()];
        let buffer_len = buffer.len();
        LittleEndian::write_u32_into(&[buffer_len as u32], &mut buffer[0..4]);
        buffer[4..8].copy_from_slice(REQUEST_TYPE);
        buffer[8] = key.len() as u8;
        buffer[9..buffer_len].copy_from_slice(key);
        buffer
    }

    fn run_get_client(
        mut stream: TcpStream,
        requests: Arc<Vec<Vec<u8>>>,
        thread_id: usize,
        slice_size: usize,
        value_length: usize,
    ) {
        let mut expected_buffer = vec![0u8; 1 + value_length];
        for req_id in slice_size * thread_id..slice_size * (thread_id + 1) {
            let request_buffer = requests[req_id].as_slice();
            stream.write(request_buffer).unwrap();
            stream.flush().unwrap();
            stream.read(&mut expected_buffer).unwrap();
        }
    }

    fn run_push_client(
        mut stream: TcpStream,
        requests: Arc<Vec<Vec<u8>>>,
        thread_id: usize,
        slice_size: usize,
    ) {
        let mut stream = TcpStream::connect(("192.168.178.101", 7777)).unwrap();
        let mut expected_buffer = vec![0u8; 2];
        for req_id in slice_size * thread_id..slice_size * (thread_id + 1) {
            let request_buffer = requests[req_id].as_slice();
            stream.write(request_buffer).unwrap();
            stream.flush().expect("Error flushing");
            stream.read(&mut expected_buffer).unwrap();
        }
        println!("Finished");
    }
}
