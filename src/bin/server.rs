use failure::Error;
use std::collections::VecDeque;
use std::io::prelude::*;
use std::net::{Shutdown, TcpListener, TcpStream};
use std::sync::{Arc, Mutex};

// type Queue<Work> = Arc<Mutex<VecDeque<Work>>>;

// A simple server .. is a function that takes
// a known message type .. and sends a response
// to the client

fn handle_client(stream: &mut TcpStream) -> Result<(), Error> {
    let mut bytes = stream.bytes();
    let d = redis_rust::redis::parse_redis_request(&mut bytes)?;
    println!("Finished request");
    let command = redis_rust::redis::into_command(d)?;
    match command {
        redis_rust::redis::Command::Ping => {
            println!("Returning PONG");
            stream.write("PONG\r\n".as_bytes())?;
        }
        redis_rust::redis::Command::Echo(e) => {
            // TODO:
        }
    }

    Ok(())
}

pub fn main() -> std::io::Result<()> {
    println!("starting server");
    let listener = TcpListener::bind("127.0.0.1:3456")?;
    for stream in listener.incoming() {
        let err = handle_client(&mut stream?);
        if err.is_err() {
            let e = err.err().unwrap();
            eprintln!("Handle client encountered error: {}", e);
        }
    }

    Ok(())
}
