use async_std::io::{Read, Write};
use async_std::net::TcpStream;
use byteorder::{ByteOrder, LittleEndian, ReadBytesExt};
use futures::AsyncReadExt;
use std::{io::Cursor, usize};

impl Request {
    pub const MAX_STACK_BUFFER_SIZE: usize = 256;

    pub async fn from_stream(mut stream: impl Read + Write + Unpin) -> Request {
        let mut current_buffer_size = 0;
        let mut expected_buffer_size = 0;
        let mut is_new_req = true;
        let mut complete_buffer = Vec::new();
        let mut stack_buffer = [0; Request::MAX_STACK_BUFFER_SIZE];
        println!("Check for request");
        loop {
            match stream.read(&mut stack_buffer).await {
                Ok(current_size) => {
                    println!("current size = {}", current_size);
                    //If the size of the current bytes read = 0 then we error out.
                    //We cant read a request containing 0 bytes.
                    if current_size == 0 {
                        println!("Error request");
                        return Request::Error;
                    } else {
                        //If this is the first buffer of the request then we:
                        //Take the first 4 bytes and set the expected buffer size using little endian (x86)
                        //We flip the is_new_req
                        //We create a vector filled with the current stack buffer.
                        if is_new_req {
                            let mut cursor = Cursor::new(&stack_buffer[0..4]);
                            expected_buffer_size =
                                cursor.read_u32::<LittleEndian>().unwrap() as usize;
                            println!("Expected size = {}", expected_buffer_size);
                            complete_buffer = stack_buffer[4..stack_buffer.len()].to_vec();
                            is_new_req = false;
                        }
                        //Extend the the complete buffer with the current stack buffer read from the tcp stream.
                        else {
                            complete_buffer.extend_from_slice(&stack_buffer);
                        }
                        current_buffer_size += current_size;
                        //If the buffer is the size of the expected buffer size that's decided in the first stack buffer of the request
                        if current_buffer_size == expected_buffer_size {
                            println!("valid request");
                            let req =
                                Request::from_buf(&complete_buffer[..expected_buffer_size - 4]);
                            return req;
                        }
                        //If the size of the current buffer exceeds the buffer that was expected in the first stack buffer of the request, then we error out.
                        else if current_buffer_size > expected_buffer_size {
                            println!(
                                "Error request too large size; current size {} expected size {}",
                                current_buffer_size, expected_buffer_size
                            );
                            return Request::Error;
                        }
                        //Stack buffer wasn't completely filled. Since the size isn't equal to the expected buffer
                        //it means we've gotten an incorrect buffer and we error out the request.
                        if stack_buffer.len() < Request::MAX_STACK_BUFFER_SIZE {
                            println!("Error request to small size");
                            return Request::Error;
                        }
                    }
                }
                Err(_) => {
                    return Request::Error;
                }
            }
        }
    }

    pub fn from_buf(request_buffer: &[u8]) -> Request {
        if request_buffer.starts_with(b"GET") {
            let key_length = request_buffer[3] as usize;
            let key = &request_buffer[4..key_length + 4];
            let key_string = String::from_utf8_lossy(key).to_string();
            Request::Get(GetRequest { key: key_string })
        } else if request_buffer.starts_with(b"PUSH") {
            let key_length = request_buffer[4] as usize;
            let key = &request_buffer[5..key_length + 5];
            let key_string = String::from_utf8_lossy(key).to_string();
            let value_bufer = request_buffer[key_length + 5..request_buffer.len()].to_vec();
            println!("{:#?}", value_bufer);
            println!("length x {}", value_bufer.len());
            Request::Push(PushRequest {
                key: key_string,
                value: value_bufer,
            })
        } else {
            Request::UnknownRequest
        }
    }
}

#[derive(Debug, PartialEq)]
pub struct GetRequest {
    pub key: String,
}

#[derive(Debug, PartialEq)]
pub struct PushRequest {
    pub key: String,
    pub value: Vec<u8>,
}

#[derive(Debug, PartialEq)]
pub enum Request {
    Get(GetRequest),
    Push(PushRequest),
    UnknownRequest,
    Error,
}

#[cfg(test)]
mod tests {

    use super::*;
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

    fn create_invalid_get_request_oversized_buffer() -> Vec<u8> {
        let mut buffer = [0; 16];
        let buffer_len = buffer.len();
        let request_type = b"GET";
        LittleEndian::write_u32_into(&[15], &mut buffer[0..4]);
        buffer[4..7].copy_from_slice(request_type);
        buffer[7] = 7 as u8;
        buffer[8..buffer_len - 1].copy_from_slice(b"testkey");
        buffer.to_vec()
    }

    fn create_invalid_get_request_expected_size_smaller_than_buffer() -> Vec<u8> {
        let mut buffer = [0; 16];
        let buffer_len = buffer.len();
        let request_type = b"GET";
        LittleEndian::write_u32_into(&[17], &mut buffer[0..4]);
        buffer[4..7].copy_from_slice(request_type);
        buffer[7] = 7 as u8;
        buffer[8..buffer_len - 1].copy_from_slice(b"testkey");
        buffer.to_vec()
    }

    fn create_valid_get_request() -> Vec<u8> {
        let mut buffer = [0; 15];
        let buffer_len = buffer.len();
        let request_type = b"GET";
        LittleEndian::write_u32_into(&[15], &mut buffer[0..4]);
        buffer[4..7].copy_from_slice(request_type);
        buffer[7] = 7 as u8;
        buffer[8..buffer_len].copy_from_slice(b"testkey");
        buffer.to_vec()
    }

    #[async_std::test]
    async fn test_parse_valid_request() {
        let mut stream = MockTcpStream {
            read_data: create_valid_get_request(),
            write_data: Vec::new(),
        };

        let reg = Request::from_stream(&mut stream).await;

        assert_eq!(
            reg,
            Request::Get(GetRequest {
                key: String::from("testkey")
            })
        )
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
