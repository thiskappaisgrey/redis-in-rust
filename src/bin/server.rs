use anyhow::Error;
use redis_rust::redis::ReadBufLine;
use std::borrow::BorrowMut;
use std::collections::VecDeque;
use std::io::{prelude::*, BufReader, BufWriter};
use std::net::{Shutdown, TcpListener, TcpStream};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tracing::{info, span, Level};
use tracing_subscriber::fmt;

// type Queue<Work> = Arc<Mutex<VecDeque<Work>>>;

// A simple server .. is a function that takes
// a known message type .. and sends a response
// to the client
//
// TODO: how do I handle concurrent clients..? just have a custom executor!
fn handle_client(stream: &mut TcpStream) -> Result<(), Error> {
    let span = span!(Level::INFO, "handle_client");
    let _enter = span.enter();
    // create a buffered reader / writer
    // for the tcp stream
    let mut write_stream = BufWriter::new(stream.try_clone()?);
    // set a read timeout so we don't wait forever
    stream.set_read_timeout(Some(Duration::from_millis(100)))?;
    let bufreader = BufReader::new(stream);
    let mut readbufline = ReadBufLine::new(bufreader);
    // while there's data in the buffer
    loop {
        info!("Checking has data left");
        let has_data_left = readbufline.has_data()?;
        if has_data_left {
            break;
        }

        // parse more commands
        info!("Calling parse");
        // TODO: we have to poll this until completion
        // I wonder what completion means ..
        let d = redis_rust::redis::parse_redis_datatype_sync(&mut readbufline)?;
        let command = redis_rust::redis::into_command(&d)?;
        match command {
            redis_rust::redis::Command::Ping => {
                write_stream.write("$4\r\nPONG\r\n".as_bytes())?;
                write_stream.flush()?;
            }
            redis_rust::redis::Command::Echo(e) => {}
        }
    }

    // TODO:

    Ok(())
}

pub fn main() -> std::io::Result<()> {
    fmt::init();
    info!("Starting server");
    let listener = TcpListener::bind("127.0.0.1:6379")?;
    for stream in listener.incoming() {
        // TODO: for concurrent
        let err = handle_client(&mut stream?);
        if err.is_err() {
            let e = err.err().unwrap();
            eprintln!("Handle client encountered error: {}", e);
        }
    }

    Ok(())
}
