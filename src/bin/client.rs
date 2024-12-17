use std::io::prelude::*;
use std::net::{Shutdown, TcpListener, TcpStream};
use std::thread::sleep;
use std::time::Duration;

// TODO: the client needs to test the redis server..
pub fn main() -> std::io::Result<()> {
    // connnect to the server addr
    let mut stream = TcpStream::connect("127.0.0.1:6379")?;
    stream.write("*1\r\n$4\r\nPING\r\n\n".as_bytes())?;
    stream.flush()?;

    let mut buf = [0; 4096];
    // read into the buffer of the response .. 64 bytes..
    // for now

    loop {
        match stream.read(&mut buf) {
            Ok(0) => {
                break;
            }
            Ok(_) => {
                println!("buffer: {}", std::str::from_utf8(&buf).unwrap())
            }
            Err(e) => match e.kind() {
                std::io::ErrorKind::WouldBlock => {
                    continue;
                }
                _ => return Err(e),
            },
        }
    }

    Ok(())
}
