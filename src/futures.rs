use std::{
    io::{self, ErrorKind, Read, Result, Write},
    net::TcpStream,
    pin::Pin,
    task::Poll,
};

use futures::{AsyncBufRead, AsyncBufReadExt, AsyncRead, AsyncWrite};

use crate::reactor::reactor;
// futures and other utilities

// Top level implementation of futures
// .. will manually implement individual TCP stream async stuff
// so I can learn how it works

// A simple concrete implementation of an async tcp stream
pub struct AsyncTCPStream {
    // the inner stream
    inner_stream: TcpStream,
    // an optional id to track
    // this future with the reactor
    id: Option<u64>,
}

impl AsyncTCPStream {
    pub fn new(stream: TcpStream) -> std::io::Result<Self> {
        // set the stream to non-blocking
        stream.set_nonblocking(true)?;
        Ok(Self {
            inner_stream: stream,
            id: None,
        })
    }
}

impl AsyncRead for AsyncTCPStream {
    fn poll_read(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut [u8],
    ) -> std::task::Poll<std::io::Result<usize>> {
        let this = Pin::into_inner(self);
        match this.inner_stream.read(buf) {
            Ok(s) => Poll::Ready(Ok(s)),
            Err(e) if e.kind() == ErrorKind::WouldBlock => {
                let id = reactor().register(&this.inner_stream, cx.waker().clone(), this.id);
                this.id = Some(id);
                Poll::Pending
            }
            Err(e) => Poll::Ready(Err(e)),
        }
    }
}

impl AsyncWrite for AsyncTCPStream {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        let this = Pin::into_inner(self);
        match this.inner_stream.write(buf) {
            Ok(s) => Poll::Ready(Ok(s)),
            Err(e) if e.kind() == ErrorKind::WouldBlock => {
                let id = reactor().register(&this.inner_stream, cx.waker().clone(), this.id);
                this.id = Some(id);
                Poll::Pending
            }
            Err(e) => Poll::Ready(Err(e)),
        }
    }

    fn poll_flush(
        self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<std::io::Result<()>> {
        let this = Pin::into_inner(self);
        match this.inner_stream.flush() {
            Ok(_) => Poll::Ready(Ok(())),
            Err(e) if e.kind() == ErrorKind::WouldBlock => {
                let id = reactor().register(&this.inner_stream, cx.waker().clone(), this.id);
                this.id = Some(id);
                Poll::Pending
            }
            Err(e) => Poll::Ready(Err(e)),
        }
    }

    fn poll_close(
        self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<std::io::Result<()>> {
        self.poll_flush(cx)
    }
}
