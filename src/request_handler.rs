use std::{net::Ipv4Addr, sync::Arc};

use async_std::task::spawn;
use async_std::{
    io::BufReader,
    net::{TcpListener, TcpStream},
};

use dashmap::DashMap;
use futures::{AsyncWriteExt, StreamExt};

use crate::cache_handler::{self, CacheInteractionResult};
use crate::{configurator::Auth, request_parser::Request};
use async_std::io::{Read, Write};
use std::marker::Unpin;
pub struct RequestHandler {
    pub listener: TcpListener,
    pub authentication: Option<Auth>,
}

impl RequestHandler {
    pub async fn new(address: Ipv4Addr, port: usize) -> Result<RequestHandler, &'static str> {
        println!("Next Cache is running at {}:{}", address, port);
        let listener = TcpListener::bind(format!("{}:{}", address, port).as_str()).await;
        match listener {
            Ok(verified_listener) => Ok(RequestHandler {
                listener: verified_listener,
                authentication: None,
            }),
            _ => Err("Could not create TCP listener"),
        }
    }
    pub async fn start_handling(self, memory_cache: DashMap<String, Vec<u8>>) {
        let cache = Arc::new(memory_cache);
        while let Some(stream) = self.listener.incoming().next().await {
            let stream = stream.ok().unwrap();
            let clone = cache.clone();
            let auth_cloned_ref = self.authentication.clone();
            async move {
                spawn(handle_connection(stream, clone, auth_cloned_ref));
            }
            .await;
        }
    }
}

async fn handle_connection(
    mut stream: impl Read + Write + Unpin,
    memory_cache: Arc<DashMap<String, Vec<u8>>>,
    _: Option<Auth>,
) -> Result<(), InternalRequestHandlingError> {
    loop {
        match Request::from_stream(&mut stream).await {
            Request::Error => {
                stream.close().await;
                return Err(InternalRequestHandlingError::TcpConnectionUnsafelyClosed);
            }
            Request::Get(get_req) => {
                let cache_interaction_result =
                    cache_handler::get_cache_line(memory_cache.clone(), get_req.key);
                respond(&mut stream, cache_interaction_result).await;
            }
            Request::Push(push_req) => {
                let cache_interaction_result = cache_handler::insert_cache_line(
                    memory_cache.clone(),
                    push_req.key,
                    push_req.value,
                );
                respond(&mut stream, cache_interaction_result).await;
            }
            Request::UnknownRequest => {}
            Request::Closed => {
                stream.close().await;
                return Err(InternalRequestHandlingError::TcpConnectionUnsafelyClosed);
            }
        }
    }
}

async fn respond(
    mut stream: impl Read + Write + Unpin,
    cache_interaction_result: CacheInteractionResult,
) -> Result<(), InternalRequestHandlingError> {
    let buffer = create_response_buffer(cache_interaction_result);
    let res = stream.write(&buffer).await;
    match res {
        Ok(_) => match stream.flush().await {
            Ok(_) => Ok(()),
            Err(_) => Err(InternalRequestHandlingError::FailedFlushingResponse),
        },
        Err(_) => Err(InternalRequestHandlingError::FailedWritingResponse),
    }
}

#[allow(dead_code)]
async fn verify_connection(mut stream: impl Read + Write + Unpin, auth: Option<Auth>) -> bool {
    match auth {
        Some(auth_info) => true,
        None => true,
    }
}

fn create_response_buffer(cache_interaction_result: CacheInteractionResult) -> Vec<u8> {
    match cache_interaction_result {
        CacheInteractionResult::Found(cached_byte_stream) => {
            let mut buffer = vec![0u8; cached_byte_stream.len() + 1];
            let buffer_len = buffer.len();
            buffer[0] = ClientRequestResult::SuccesfulRead as u8;
            buffer[1..buffer_len].copy_from_slice(&cached_byte_stream);
            buffer
        }
        CacheInteractionResult::NoEntryFound => {
            vec![ClientRequestResult::NoEntryFound as u8]
        }
        CacheInteractionResult::InsertSucces => {
            vec![ClientRequestResult::SuccesfulEntry as u8]
        }
        CacheInteractionResult::AnotherCacheEntryExists => {
            vec![ClientRequestResult::AnotherCacheEntryExists as u8]
        }
    }
}

enum InternalRequestHandlingError {
    FailedWritingResponse,
    FailedFlushingResponse,
    FailedReadingTcpStream,
    TcpConnectionUnsafelyClosed,
}
#[repr(u8)]
pub enum ClientRequestResult {
    SuccesfulRead = 1,
    SuccesfulEntry = 2,
    SuccesfulDelete = 3,

    NoEntryFound = 101,
    AnotherCacheEntryExists = 102,
    FailedDelete = 103,
    UnknownAction = 104,
}

#[cfg(test)]
mod tests {

    use super::*;
    use async_std::task;
    use byteorder::{ByteOrder, LittleEndian, ReadBytesExt};
    use futures::io::Error;
    use futures::task::{Context, Poll};
    use std::sync::Mutex;
    use std::{cmp::min, time::Duration};
    use std::{pin::Pin, thread};

    #[derive(Debug, Clone)]
    struct MockTcpStream {
        read_data: Arc<Mutex<Vec<u8>>>,
        write_data: Arc<Mutex<Vec<u8>>>,
    }

    impl Read for MockTcpStream {
        fn poll_read(
            self: Pin<&mut Self>,
            _: &mut Context,
            buf: &mut [u8],
        ) -> Poll<Result<usize, Error>> {
            let mut wait_for_data = true;
            while wait_for_data {
                let mut read_buffer = self.read_data.lock().unwrap();
                if read_buffer.len() != 0 {
                    wait_for_data = false;
                }
                thread::sleep(Duration::from_millis(100));
            }
            let mut read_buffer = self.read_data.lock().unwrap();
            let size: usize = min(read_buffer.len(), buf.len());
            buf[..size].copy_from_slice(&read_buffer[..size]);
            *read_buffer = Vec::new();
            Poll::Ready(Ok(size))
        }
    }

    impl Write for MockTcpStream {
        fn poll_write(
            mut self: Pin<&mut Self>,
            _: &mut Context,
            buf: &[u8],
        ) -> Poll<Result<usize, Error>> {
            let mut write_buffer = self.write_data.lock().unwrap();
            *write_buffer = Vec::from(buf);
            return Poll::Ready(Ok(buf.len()));
        }
        fn poll_flush(self: Pin<&mut Self>, _: &mut Context) -> Poll<Result<(), Error>> {
            Poll::Ready(Ok(()))
        }
        fn poll_close(self: Pin<&mut Self>, _: &mut Context) -> Poll<Result<(), Error>> {
            Poll::Ready(Ok(()))
        }
    }

    use std::marker::Unpin;
    impl Unpin for MockTcpStream {}

    fn create_valid_get_request() -> Vec<u8> {
        const REQUEST_TYPE: &[u8] = b"GET";
        const KEY: &[u8] = b"testkey";
        let mut buffer = [0; 4 + REQUEST_TYPE.len() + 1 + KEY.len()];
        let buffer_len = buffer.len();
        LittleEndian::write_u32_into(&[15], &mut buffer[0..4]);
        buffer[4..7].copy_from_slice(REQUEST_TYPE);
        buffer[7] = KEY.len() as u8;
        buffer[8..buffer_len].copy_from_slice(KEY);
        buffer.to_vec()
    }

    fn create_valid_push_request() -> Vec<u8> {
        const REQUESTTYPE: &[u8] = b"PUSH";
        const VALUE: &[u8] = b"cachedvalue";
        const KEY: &[u8] = b"testkey";
        let mut buffer = [0; 4 + REQUESTTYPE.len() + 1 + KEY.len() + VALUE.len()];
        let buffer_len = buffer.len();
        LittleEndian::write_u32_into(&[buffer_len as u32], &mut buffer[0..4]);
        buffer[4..8].copy_from_slice(REQUESTTYPE);
        buffer[8] = KEY.len() as u8;
        buffer[9..KEY.len() + 9].copy_from_slice(KEY);
        buffer[KEY.len() + 9..buffer_len].copy_from_slice(VALUE);
        buffer.to_vec()
    }

    #[async_std::test]
    async fn test_response_valid_push_request() {
        let mut stream = MockTcpStream {
            read_data: Arc::new(Mutex::new(create_valid_push_request())),
            write_data: Arc::new(Mutex::new(Vec::new())),
        };
        let mut handle = spawn(handle_connection(
            stream.clone(),
            Arc::new(DashMap::new()),
            None,
        ));
        task::sleep(Duration::from_secs(1)).await;
        drop(handle);
        let res = stream.write_data.clone();
        {
            let respond_buffer = res.lock().unwrap();
            assert_eq!(
                respond_buffer.clone(),
                [ClientRequestResult::SuccesfulEntry as u8]
            );
        }
    }

    #[async_std::test]
    async fn test_response_valid_push_request_already_exists() {
        let mut stream = MockTcpStream {
            read_data: Arc::new(Mutex::new(create_valid_push_request())),
            write_data: Arc::new(Mutex::new(Vec::new())),
        };
        let handle = spawn(handle_connection(
            stream.clone(),
            Arc::new(DashMap::new()),
            None,
        ));
        task::sleep(Duration::from_secs(1)).await;
        let res = stream.write_data.clone();
        {
            let respond_buffer = res.lock().unwrap();
            assert_eq!(
                respond_buffer.clone(),
                [ClientRequestResult::SuccesfulEntry as u8]
            );
        }
        let read_handle = stream.read_data.clone();
        {
            let mut handle = read_handle.lock().unwrap();
            *handle = create_valid_push_request();
        }
        task::sleep(Duration::from_secs(1)).await;
        drop(handle);
        {
            let respond_buffer = res.lock().unwrap();
            assert_eq!(
                respond_buffer.clone(),
                [ClientRequestResult::AnotherCacheEntryExists as u8]
            );
        }
    }

    #[async_std::test]
    async fn test_response_valid_get_request_no_entry_found() {
        let mut stream = MockTcpStream {
            read_data: Arc::new(Mutex::new(create_valid_get_request())),
            write_data: Arc::new(Mutex::new(Vec::new())),
        };
        let handle = spawn(handle_connection(
            stream.clone(),
            Arc::new(DashMap::new()),
            None,
        ));
        task::sleep(Duration::from_secs(1)).await;
        drop(handle);
        let res = stream.write_data.clone();
        {
            let respond_buffer = res.lock().unwrap();
            assert_eq!(
                respond_buffer.clone(),
                [ClientRequestResult::NoEntryFound as u8]
            );
        }
    }

    #[async_std::test]
    async fn test_response_valid_push_and_get_request() {
        let mut stream = MockTcpStream {
            read_data: Arc::new(Mutex::new(create_valid_push_request())),
            write_data: Arc::new(Mutex::new(Vec::new())),
        };
        let mut handle = spawn(handle_connection(
            stream.clone(),
            Arc::new(DashMap::new()),
            None,
        ));
        task::sleep(Duration::from_secs(1)).await;
        let res = stream.write_data.clone();
        {
            let respond_buffer = res.lock().unwrap();
            assert_eq!(
                respond_buffer.clone(),
                [ClientRequestResult::SuccesfulEntry as u8]
            );
        }
        let read_handle = stream.read_data.clone();
        {
            let mut handle = read_handle.lock().unwrap();
            *handle = create_valid_get_request();
        }
        task::sleep(Duration::from_secs(1)).await;
        drop(handle);
        let res = stream.write_data.clone();
        {
            const EXPECTED_VALUE: &[u8] = b"cachedvalue";
            let mut expected_buffer = [0u8; 1 + EXPECTED_VALUE.len()];
            let expected_buffer_length = expected_buffer.len();
            expected_buffer[0] = ClientRequestResult::SuccesfulRead as u8;
            expected_buffer[1..expected_buffer_length].copy_from_slice(EXPECTED_VALUE);
            let respond_buffer = res.lock().unwrap();
            assert_eq!(respond_buffer.clone(), expected_buffer);
        }
    }
}
