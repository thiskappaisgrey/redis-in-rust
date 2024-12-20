use anyhow::Error;
use futures::executor;
use redis_rust::executor::new_executor_and_spawner;
use redis_rust::redis::ReadBufLine;
use std::io::{prelude::*, BufReader, BufWriter};
use std::net::{Shutdown, TcpListener, TcpStream};
use std::thread;
use std::time::Duration;
use tracing::{error, info, span, Instrument, Level};
use tracing_subscriber::fmt;

// type Queue<Work> = Arc<Mutex<VecDeque<Work>>>;

// A simple server .. is a function that takes
// a known message type .. and sends a response
// to the client
//
// TODO: how do I handle concurrent clients..? just have a custom executor!
async fn handle_client(stream: &mut TcpStream) -> Result<(), Error> {
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
        if readbufline.is_empty().await? {
            info!("Task ended");
            break;
        }
        // parse more commands
        info!("Calling parse");
        // TODO: we have to poll this until completion
        // I wonder what completion means ..
        let d = redis_rust::redis::parse_redis_datatype(&mut readbufline).await?;
        let command = redis_rust::redis::into_command(&d)?;
        match command {
            redis_rust::redis::Command::Ping => {
                write_stream.write("$4\r\nPONG\r\n".as_bytes())?;
                write_stream.flush()?;
            }
            redis_rust::redis::Command::Echo(e) => {}
        }
    }

    info!("Task ended");
    Ok(())
}

pub fn main() -> std::io::Result<()> {
    fmt::init();
    info!("Starting server");
    let listener = TcpListener::bind("127.0.0.1:6379")?;
    let (executor, spawner) = new_executor_and_spawner();
    // spapwn the executor in another thread
    thread::spawn(move || {
        executor.run();
    });
    let mut counter = 0;
    for stream in listener.incoming() {
        let task_id = counter;
        counter += 1;
        // TODO: for concurrent
        spawner.spawn(async move {
            let span = span!(Level::INFO, "task", value = task_id);
            async move {
                info!("Spawning tasks_{task_id}");
                let mut s = stream.unwrap();
                let err = handle_client(&mut s).await;
                if err.is_err() {
                    let e = err.err().unwrap();
                    error!("Handle client encountered error: {}", e);
                }
            }
            .instrument(span)
            .await;

            ()
        });
    }

    Ok(())
}
