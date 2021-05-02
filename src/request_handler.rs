use std::{net::Ipv4Addr, sync::Arc};

use async_std::net::{TcpListener, TcpStream};
use async_std::task::spawn;

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
            println!("Got tcp stream connection");
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
            let mut buffer = Vec::with_capacity(cached_byte_stream.len() + 1);
            buffer.push(ClientRequestResult::SuccesfulRead as u8);
            buffer.extend_from_slice(&cached_byte_stream);
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
    use byteorder::{ByteOrder, LittleEndian, ReadBytesExt};
    use futures::io::Error;
    use futures::task::{Context, Poll};
    use std::cmp::min;
    use std::pin::Pin;

    struct MockTcpStream {
        read_data: Vec<u8>,
        write_data: Vec<u8>,
    }

    impl Read for MockTcpStream {
        fn poll_read(
            self: Pin<&mut Self>,
            _: &mut Context,
            buf: &mut [u8],
        ) -> Poll<Result<usize, Error>> {
            let size: usize = min(self.read_data.len(), buf.len());
            buf[..size].copy_from_slice(&self.read_data[..size]);
            Poll::Ready(Ok(size))
        }
    }

    impl Write for MockTcpStream {
        fn poll_write(
            mut self: Pin<&mut Self>,
            _: &mut Context,
            buf: &[u8],
        ) -> Poll<Result<usize, Error>> {
            self.write_data = Vec::from(buf);
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
        const REQUESTTYPE: &[u8] = b"PUSH";
        const KEY: &[u8] = b"testkey";
        let mut buffer = [0; 4 + REQUESTTYPE.len() + 1 + KEY.len()];
        let buffer_len = buffer.len();
        LittleEndian::write_u32_into(&[15], &mut buffer[0..4]);
        buffer[4..7].copy_from_slice(REQUESTTYPE);
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
    async fn test_response_valid_get_request() {
        let mut stream = MockTcpStream {
            read_data: create_valid_get_request(),
            write_data: Vec::new(),
        };
        let handle = spawn(handle_connection(stream, Arc::new(DashMap::new()), None));
    }

    #[async_std::test]
    async fn test_parse_invalid_oversized_request() {
        let mut stream = MockTcpStream {
            read_data: create_invalid_get_request_oversized_buffer(),
            write_data: Vec::new(),
        };

        let reg = Request::from_stream(&mut stream).await;

        assert_eq!(reg, Request::Error)
    }

    #[async_std::test]
    async fn test_parse_invalid_request_expected_size_smaller() {
        let mut stream = MockTcpStream {
            read_data: create_invalid_get_request_expected_size_smaller_than_buffer(),
            write_data: Vec::new(),
        };

        let reg = Request::from_stream(&mut stream).await;

        assert_eq!(reg, Request::Error)
    }
}
