use anyhow::{format_err, Error};
use futures::{AsyncBufRead, AsyncBufReadExt, AsyncReadExt};
use std::{
    borrow::{Borrow, BorrowMut},
    fmt::Debug,
    future::{Future, Ready},
    io::{self, BufRead, BufReader, Bytes, Write},
    os::fd::{AsFd, BorrowedFd},
    pin::{self, Pin},
    ptr,
    rc::Rc,
    sync::Arc,
    task::{Context, Poll, RawWaker, RawWakerVTable, Waker},
};
use tracing::info;

use crate::reactor::reactor;

// TODO: I will probably have to end up giving lifetime params here
#[derive(Debug, PartialEq)]
pub enum DataTypes {
    // For now .. bulk string and string
    // have the same representation
    //
    // TODO: if the string doesn't require mutation .. use a different type instead..?
    String(Arc<str>),
    ErrorMessage(Arc<str>),
    Int(i64),
    Array(Vec<DataTypes>),
    NullArray,
    NullBulkString,
    Boolean(bool),
    Double(f64),
    // TODO: Support more complex data tyeps in Redis
}

#[derive(Debug, PartialEq)]
pub enum Command {
    Ping,
    Echo(Arc<str>), // TODO: prob want to use a better type here..
}

// NOOP waker from the stdlib unstable feature
const NOOP: RawWaker = {
    const VTABLE: RawWakerVTable = RawWakerVTable::new(
        // Cloning just returns a new no-op raw waker
        |_| NOOP,
        // `wake` does nothing
        |_| {},
        // `wake_by_ref` does nothing
        |_| {},
        // Dropping does nothing as we don't allocate anything
        |_| {},
    );
    RawWaker::new(ptr::null(), &VTABLE)
};
// create a readbufline from a bufread

pub struct RedisParser<T: AsyncBufRead + Unpin> {
    buf: Vec<u8>, // A buffer allocated once to reuse through out the entire parse
    bufread: T,   // the buffer to read from
}

impl<T: AsyncBufRead + Unpin> RedisParser<T> {
    pub fn new(bufread: T) -> Self {
        Self {
            buf: Vec::new(),
            bufread,
        }
    }
    // we can have async methods in traits!
    pub async fn parse(&mut self) -> Result<DataTypes, Error> {
        // clear the buffer before each parse
        self.buf.clear();

        let _s = self.bufread.read_until(b'\r', &mut self.buf).await?;
        self.buf.pop(); // pop the b'\r' b/c we want inclusive

        let utf8 = std::str::from_utf8(self.buf.as_slice())?;
        let mut bytes_iter = self.buf.iter();
        if let Some(b'\n') = bytes_iter.clone().peekable().peek() {
            bytes_iter.next();
        }

        // TODO: if I just have a special error type here..
        let first = bytes_iter.next().ok_or(format_err!("No first byte"))?;

        match first {
            b'+' => {
                let s: &[u8] = bytes_iter.as_slice();
                let utf8 = std::str::from_utf8(s)?;
                Ok(DataTypes::String(utf8.into()))
            }
            b':' => {
                let x = bytes_iter.as_slice();
                let utf8 = std::str::from_utf8(x)?;
                println!("Utf8 is: {}.", utf8);
                let num = str::parse(utf8).map_err(|_e| format_err!("invalid digit: {}", utf8))?;
                Ok(DataTypes::Int(num))
            }
            b'$' => {
                let len_str = std::str::from_utf8(bytes_iter.as_slice())?;
                let len = str::parse(len_str)?;
                // read the extra newline byte
                let _s = self.bufread.read_until(b'\n', &mut self.buf).await?;

                // clear the vector
                self.buf.clear();
                self.buf.resize(len, 0);

                // read into the buffer the length of the buffer
                self.bufread.read_exact(&mut self.buf).await?;
                let utf8 = std::str::from_utf8(&self.buf)?.into();
                let mut buf = [0; 2];
                let e = self.bufread.read_exact(&mut buf).await;
                if let Err(er) = e {
                    // try to read the \r\n
                    // but we aren't concernted
                    if er.kind() != io::ErrorKind::UnexpectedEof {
                        return Err(er.into());
                    }
                }

                Ok(DataTypes::String(utf8))
            }
            b'*' => {
                println!("Array");
                let len_str = std::str::from_utf8(&bytes_iter.as_slice())?;
                println!("utf8: {len_str}");
                let len = u64::from_str_radix(&len_str, 10)
                    .map_err(|_e| format_err!("Invalid digit: {len_str}."))?;
                // Allocate a new datatype vector for the array
                let mut v = Vec::with_capacity(len as usize);

                for _ in 0..len {
                    let d = Box::pin(self.parse()).await?;
                    println!("parsed: {d:?}");
                    v.push(d);
                }
                Ok(DataTypes::Array(v))
            }
            b => Err(format_err!("Invalid datatype specifier: {}", *b as char)),
        }
    }
}

/// Handles a redis request by iterating over the bytes, consuming the iterator
pub fn into_command(d: &DataTypes) -> Result<Command, Error> {
    match d {
        DataTypes::Array(v) => {
            let command = v.first().ok_or(format_err!("Command array is empty"))?;
            match command {
                DataTypes::String(s) => match s.to_uppercase().as_str() {
                    "ECHO" => {
                        let arg = v.get(1).ok_or(format_err!("Expected ECHO argument"))?;
                        if let DataTypes::String(s) = arg {
                            // TODO: this clones the string, which is not great
                            Ok(Command::Echo(s.clone()))
                        } else {
                            Err(format_err!("Echo argument not a string"))
                        }
                    }
                    "PING" => Ok(Command::Ping),
                    _ => Err(format_err!("Unknown command")),
                },
                _ => Err(format_err!("Command is not a string")),
            }
        }
        _ => Err(format_err!(
            "Could not convert into command because datatype is not an array"
        )),
    }
}

// turn a datatype into bytes
pub fn into_bytes<W>(d: DataTypes, writer: &mut W) -> Result<(), Error>
where
    W: Write,
{
    // Ok(())
    todo!()
}

#[cfg(test)]
mod test {
    use futures::{future::Ready, io::Cursor, FutureExt};
    use std::{
        pin, ptr,
        task::{Context, Poll, RawWaker, RawWakerVTable, Waker},
    };

    use super::*;

    fn expect_parse_result<T: AsRef<[u8]> + Unpin + Debug>(bytes: T, expect: DataTypes) {
        let s = Cursor::new(bytes);
        let bytes = futures::io::BufReader::new(s);
        let mut r = RedisParser::new(bytes);
        let waker = unsafe { Waker::from_raw(NOOP) };
        let mut context = Context::from_waker(&waker);
        let mut future = Box::pin(r.parse());

        if let Poll::Ready(res) = future.poll_unpin(&mut context) {
            assert!(res.is_ok(), "r wasn't true, error is: {}", res.unwrap_err());
            assert_eq!(res.unwrap(), expect);
        } else {
            assert!(false, "future was not ready right away");
        }
    }

    // // TODO: this was really easy to parse .. but probably want more thorough tests.
    #[test]
    fn test_parse_redis_request_string() {
        let s = Cursor::new(b"+hello world\r\n");
        let bytes = futures::io::BufReader::new(s);
        let mut r = RedisParser::new(bytes);
        // use the noop waker
        let waker = unsafe { Waker::from_raw(NOOP) };
        let mut context = Context::from_waker(&waker);
        let mut future = Box::pin(r.parse());
        if let Poll::Ready(res) = future.poll_unpin(&mut context) {
            assert!(res.is_ok(), "r wasn't true, error is: {}", res.unwrap_err());
            assert_eq!(
                res.unwrap(),
                DataTypes::String(String::from("hello world").into())
            );
        } else {
            assert!(false, "future was not ready right away");
        }
    }

    #[test]
    fn test_parse_redis_array() {
        expect_parse_result(
            b"*2\r\n:12345\r\n+hello world this is foo bar\r\n" as &[u8],
            DataTypes::Array(vec![
                DataTypes::Int(12345),
                DataTypes::String(String::from("hello world this is foo bar").into()),
            ]),
        );
    }
    // //
    #[test]
    fn test_parse_redis_request_big_string() {
        let mut bytes = b"$5\r\nhello\r\n";
        let expected = DataTypes::String(String::from("hello").into());
        expect_parse_result(bytes, expected);
    }

    #[test]
    fn test_parse_request() {
        let bytes = "*2\r\n$4\r\nECHO\r\n$3\r\nhey\r\n".as_bytes();
        let s = Cursor::new(bytes);
        let bytes = futures::io::BufReader::new(s);
        let mut r = RedisParser::new(bytes);
        let waker = unsafe { Waker::from_raw(NOOP) };
        let mut context = Context::from_waker(&waker);
        let mut future = Box::pin(r.parse());

        if let Poll::Ready(res) = future.poll_unpin(&mut context) {
            assert!(res.is_ok(), "r wasn't true, error is: {}", res.unwrap_err());
            if let Ok(DataTypes::Array(a)) = res {
                assert_eq!(a.len(), 2);
                assert_eq!(a[0], DataTypes::String(String::from("ECHO").into()));
                assert_eq!(a[1], DataTypes::String(String::from("hey").into()));
            } else {
                assert!(false, "r wasn't an array");
            }
        } else {
            assert!(false, "poll wasn't ready");
        }
    }
}
