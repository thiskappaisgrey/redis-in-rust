use anyhow::{format_err, Error};
use std::{
    borrow::{Borrow, BorrowMut},
    fmt::Debug,
    future::{Future, Ready},
    io::{self, BufRead, BufReader, Bytes, Write},
    pin::{self, Pin},
    ptr,
    rc::Rc,
    sync::Arc,
    task::{Context, Poll, RawWaker, RawWakerVTable, Waker},
};
use tracing::info;

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

pub struct ReadBufLine<T: BufRead> {
    bufread: T,
    buf: Vec<u8>, // a buffer for partially read data
}

// this is not thread safe though..
pub struct IsEmpty<'a> {
    bufread: &'a mut dyn BufRead,
}

unsafe impl<'a> Send for IsEmpty<'a> {}

impl<'a> Future for IsEmpty<'a> {
    type Output = Result<bool, Error>;
    fn poll(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Self::Output> {
        let r = Pin::into_inner(self).bufread.fill_buf();
        match r {
            Ok(v) => {
                info!("is empty returned buffer");
                return Poll::Ready(Ok(v.len() == 0));
            }
            Err(k) => {
                if k.kind() == io::ErrorKind::WouldBlock {
                    info!("is empty returned would block");
                    return Poll::Pending;
                } else {
                    return Poll::Ready(Err(k.into()));
                }
            }
        }
    }
}

impl<T: BufRead> ReadBufLine<T> {
    pub fn new(b: T) -> ReadBufLine<T>
    where
        T: BufRead,
    {
        return ReadBufLine {
            bufread: b,
            buf: Vec::new(),
        };
    }

    // is empty should really be a new future..
    pub fn is_empty(&mut self) -> IsEmpty {
        IsEmpty {
            bufread: &mut self.bufread,
        }
    }
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

impl<T: BufRead> Unpin for ReadBufLine<T> {}

impl<T: BufRead> Future for ReadBufLine<T> {
    type Output = Result<Vec<u8>, Error>;
    fn poll(
        self: std::pin::Pin<&mut Self>,
        _cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        // poll should
        let this = Pin::into_inner(self);
        let bufread = this.bufread.read_until(b'\r', &mut this.buf);
        match bufread {
            Ok(_n) => {
                // if this char is a newline .. ignore it
                // FIXME: this is a terrible implemetation b/c of allocation
                // but it works ;)
                let mut buf: Vec<u8> = this.buf.clone();
                if buf.first() == Some(&b'\n') {
                    info!("Removing newline");
                    buf.remove(0);
                }
                buf.pop();
                this.buf = Vec::new();
                return std::task::Poll::Ready(Ok(buf));
            }
            Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                info!("Linebuf returned wouldblock");
                return std::task::Poll::Pending;
            }
            Err(e) => {
                return std::task::Poll::Ready(Err(e.into()));
            }
        }
    }
}

/// Handles a redis request by iterating over the bytes, consuming the iterator
// TODO: when parsing a redis_datatype ..
// we need to make this "resumable" in case somethng blocks
// So .. maybe we can make an iterator over this data..?
// I guess .. the input would be better served as "lines of bytes"
// instead of just plain bytes

// line_getter is a "future" that I explicitly await on
// .. basically this is kinda like an iterator
//
// This is hard .. b/c I'm not sure what the right abstraction is..
pub async fn parse_redis_datatype<T>(line_getter: &mut ReadBufLine<T>) -> Result<DataTypes, Error>
where
    T: BufRead,
    T: Debug,
{
    // TODO: we should attach context to these errors and make them
    // more meaningful
    //
    // read the first byte to see what type it is
    info!("Awaiting");
    let mut bytes = line_getter.borrow_mut().await?;
    info!("Got line: {}.", String::from_utf8(bytes.clone()).unwrap());
    if bytes.first().is_none() {
        return Err(format_err!("Missing specifier"));
    }
    // this is so inefficient lol
    let first = bytes.remove(0);

    let r = match first {
        b'+' => {
            let utf8 = String::from_utf8(bytes)?;
            Ok(DataTypes::String(utf8.into()))
        }
        b':' => {
            let x = bytes;
            let utf8 = std::str::from_utf8(&x)?;
            let num = i64::from_str_radix(&utf8, 10)
                .map_err(|_e| format_err!("invalid digit: {}", utf8))?;
            Ok(DataTypes::Int(num))
        }
        b'$' => {
            let len_str = std::str::from_utf8(&bytes)?;
            let len = usize::from_str_radix(len_str, 10)?;
            let mut str = line_getter.borrow_mut().await?;
            while str.len() < len {
                str.append(&mut line_getter.borrow_mut().await?);
            }
            let utf8 = String::from_utf8(str)?.into();
            Ok(DataTypes::String(utf8))
        }
        b'*' => {
            let len_bytes = bytes;
            let len_str = String::from_utf8(len_bytes)?;
            let len = u64::from_str_radix(&len_str, 10)
                .map_err(|_e| format_err!("Invalid digit: {len_str}"))?;
            let mut v = Vec::with_capacity(len as usize);

            for _ in 0..len {
                let d = Box::pin(parse_redis_datatype(line_getter)).await?;
                v.push(d);
            }
            Ok(DataTypes::Array(v))
        }
        b => Err(format_err!("Invalid datatype specifier: {}", b as char)),
    };
    r
}
pub fn parse_redis_datatype_sync<T>(bytes: &mut ReadBufLine<T>) -> Result<DataTypes, Error>
where
    T: io::BufRead,
    T: Debug,
{
    let mut r = pin::pin!(parse_redis_datatype(bytes));
    let waker = unsafe { Waker::from_raw(NOOP) };
    let mut cx = Context::from_waker(&waker);
    // poll until future's ready
    loop {
        match r.as_mut().poll(&mut cx) {
            Poll::Ready(r) => {
                return r;
            }
            Poll::Pending => {
                info!("pending");
            }
        }
    }
}

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
    use std::{
        pin, ptr,
        task::{Context, Poll, RawWaker, RawWakerVTable, Waker},
    };

    use io::Read;

    use super::*;
    #[test]
    fn test_future() {
        // TODO: I wonder if I can mock the read trait here.
        let b: &[u8] = b"12345\r\n6789\r\n";
        let mut line_buf_read: Pin<&mut ReadBufLine<&[u8]>> = pin::pin!(ReadBufLine::new(b));

        unsafe fn clone(_null: *const ()) -> RawWaker {
            unimplemented!()
        }

        unsafe fn wake(_null: *const ()) {
            unimplemented!()
        }

        unsafe fn wake_by_ref(_null: *const ()) {
            unimplemented!()
        }

        unsafe fn drop(_null: *const ()) {}

        let data = ptr::null();
        let vtable = &RawWakerVTable::new(clone, wake, wake_by_ref, drop);
        let raw_waker = RawWaker::new(data, vtable);
        let waker = unsafe { Waker::from_raw(raw_waker) };
        let mut cx = Context::from_waker(&waker);

        let r = line_buf_read.as_mut().poll(&mut cx);
        if let Poll::Ready(r) = r {
            assert_eq!(String::from_utf8(r.unwrap()).unwrap(), "12345");
        }

        let r = line_buf_read.as_mut().poll(&mut cx);
        if let Poll::Ready(r) = r {
            assert_eq!(String::from_utf8(r.unwrap()).unwrap(), "6789");
        }
    }

    // // TODO: this was really easy to parse .. but probably want more thorough tests.
    #[test]
    fn test_parse_redis_request_string() {
        let mut bytes = ReadBufLine::new(b"+hello world\r\n" as &[u8]);
        let r = parse_redis_datatype_sync(&mut bytes);
        assert!(r.is_ok(), "r wasn't true, error is: {}", r.unwrap_err());
        assert_eq!(
            r.unwrap(),
            DataTypes::String(String::from("hello world").into())
        );
    }
    #[test]
    fn test_parse_redis_array() {
        let mut bytes =
            ReadBufLine::new(b"*2\r\n:12345\r\n+hello world this is foo bar\r\n" as &[u8]);

        let r = pin::pin!(parse_redis_datatype(&mut bytes));
        let waker = unsafe { Waker::from_raw(NOOP) };
        let mut cx = Context::from_waker(&waker);
        let res = r.poll(&mut cx);

        if let Poll::Ready(r) = res {
            if let Ok(DataTypes::Array(a)) = r {
                assert_eq!(a.len(), 2);
                assert_eq!(a[0], DataTypes::Int(12345));
                assert_eq!(
                    a[1],
                    DataTypes::String(String::from("hello world this is foo bar").into())
                );
            } else {
                assert!(false, "r wasn't an array");
            }
        } else {
            assert!(false, "poll should be immediately ready")
        }
    }
    //
    #[test]
    fn test_parse_redis_request_big_string() {
        let mut bytes = ReadBufLine::new(b"$5\r\nhello\r\n" as &[u8]);
        let r = pin::pin!(parse_redis_datatype(&mut bytes));
        let waker = unsafe { Waker::from_raw(NOOP) };
        let mut cx = Context::from_waker(&waker);
        let res = r.poll(&mut cx);

        if let Poll::Ready(r) = res {
            assert!(r.is_ok(), "r wasn't true, error is: {}", r.unwrap_err());
            assert_eq!(r.unwrap(), DataTypes::String(String::from("hello").into()));
        } else {
            assert!(false, "poll should be immediately ready")
        }
    }
    //
    // #[test]
    // fn test_parse_request() {
    //     let bytes = "*2\r\n$4\r\nECHO\r\n$3\r\nhey\r".as_bytes();
    //     let mut bytes_iter = bytes.bytes();
    //     let r = parse_redis_datatype(&mut bytes_iter);
    //     if r.is_err() {
    //         let e = r.as_ref().unwrap_err();
    //         let cause = e.as_fail().cause();
    //         // let bt = e.backtrace();
    //         println!("cause: {cause:?}");
    //     }
    //     assert!(r.is_ok(), "r wasn't true, error is: {}", r.unwrap_err());
    //     if let Ok(DataTypes::Array(a)) = r {
    //         assert_eq!(a.len(), 2);
    //         assert_eq!(a[0], DataTypes::String(String::from("ECHO").into()));
    //         assert_eq!(a[1], DataTypes::String(String::from("hey").into()));
    //     } else {
    //         assert!(false, "r wasn't an array");
    //     }
    // }
}
