use anyhow::Error;
use futures::io::{BufReader, BufWriter};
use futures::{executor, AsyncWriteExt};
use redis_rust::executor::new_executor_and_spawner;
use redis_rust::reactor;
use redis_rust::redis::RedisParser;
use std::net::{Shutdown, TcpListener, TcpStream};
use std::os::fd::AsFd;
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
async fn handle_client(stream: TcpStream) -> Result<(), Error> {
    let read_stream = redis_rust::futures::AsyncTCPStream::new(stream.try_clone().unwrap())?;
    let write_stream = redis_rust::futures::AsyncTCPStream::new(stream)?;
    let span = span!(Level::INFO, "handle_client");
    let _enter = span.enter();
    let mut write_stream = BufWriter::new(write_stream);
    let bufreader = BufReader::new(read_stream);
    let mut parser = RedisParser::new(bufreader);
    // while there's data in the buffer
    loop {
        let r = parser.parse().await?;
        let command = redis_rust::redis::into_command(&r)?;
        //     // parse more commands
        //     info!("Calling parse");
        //     // TODO: we have to poll this until completion
        //     // I wonder what completion means ..
        //     // let d = redis_rust::redis::parse_redis_datatype(&mut readbufline).await?;
        //     //
        match command {
            redis_rust::redis::Command::Ping => {
                write_stream.write_all("$4\r\nPONG\r\n".as_bytes()).await?;
                write_stream.flush().await?;
            }
            redis_rust::redis::Command::Echo(e) => {}
        }
    }

    info!("Task ended");
    Ok(())
}

pub fn main() -> std::io::Result<()> {
    fmt::init();
    // setup and run the reactor
    reactor::Reactor::setup().unwrap();
    reactor::Reactor::set_up_event_loop();

    info!("Starting server");
    let listener = TcpListener::bind("127.0.0.1:6379")?;
    let (executor, spawner) = new_executor_and_spawner();
    // spapwn the executor in another thread
    thread::spawn(move || {
        executor.run();
    });
    for (counter, stream) in listener.incoming().enumerate() {
        let task_id = counter;
        let span = span!(Level::INFO, "task", value = task_id);
        // TODO: for concurrent
        spawner.spawn(async move {
            async move {
                info!("Spawning tasks_{task_id}");
                let s = stream.unwrap();
                let err = handle_client(s).await;
                if err.is_err() {
                    let e = err.err().unwrap();
                    error!("Handle client encountered error: {}", e);
                }
            }
            .instrument(span)
            .await;
        });
    }

    Ok(())
}
