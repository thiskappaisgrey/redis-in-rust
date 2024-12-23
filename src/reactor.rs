use anyhow::{anyhow, Error};
use std::{
    collections::HashMap,
    io::ErrorKind,
    net::TcpStream,
    os::fd::{AsFd, AsRawFd, BorrowedFd, OwnedFd},
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, Mutex, OnceLock,
    },
    task::Waker,
    thread,
    time::Duration,
};
use tracing::{error, info};

use rustix::event::epoll::{self, EventVec};

// There will only be one global reactor
pub struct Reactor {
    // map from id of the file descriptor we are interested in to waker
    wakers: Arc<Mutex<HashMap<u64, Waker>>>,
    next_id: AtomicU64,
    epoll: OwnedFd, // a file descriptor to the epoll instance
}

// reactor is caleld only once
static REACTOR: OnceLock<Reactor> = OnceLock::new();

pub fn reactor() -> &'static Reactor {
    REACTOR.get().expect("Called outside an runtime context")
}

impl Reactor {
    // can only be called once
    pub fn setup() -> Result<(), Error> {
        // create the epoll instance
        let epoll = epoll::create(epoll::CreateFlags::CLOEXEC)?;
        let r = Reactor {
            wakers: Arc::new(Mutex::new(HashMap::new())),
            next_id: 1.into(),
            epoll,
        };
        REACTOR
            .set(r)
            .map_err(|_| anyhow!("Reactor setup called more than once"))?;
        // run the reactor in a thread .. and poll for data

        Ok(())
    }
    // only call this once
    pub fn set_up_event_loop() {
        let wakers = REACTOR.get().unwrap().wakers.clone();
        let fd = REACTOR.get().unwrap().epoll.as_fd();
        std::thread::spawn(move || loop {
            let mut events = EventVec::with_capacity(100);
            // this should be safe
            epoll::wait(fd, &mut events, 10).unwrap();
            for event in &events {
                info!("Processing event");
                let data = event.data.u64();
                let wakers = wakers.lock().unwrap();
                if let Some(waker) = wakers.get(&data) {
                    info!("Waking up waker");
                    waker.wake_by_ref();
                }
            }
            thread::sleep(Duration::from_millis(100));
        });
    }

    // register a tcp stream + set the waker .. returning an id
    // that tracks the stream registered
    pub fn register(&self, stream: impl AsFd, waker: Waker, id: Option<u64>) -> u64 {
        let nid = id.unwrap_or(self.next_id.fetch_add(1, Ordering::Relaxed));
        if id.is_none() {
            let r = epoll::add(
                &self.epoll,
                stream,
                epoll::EventData::new_u64(nid),
                epoll::EventFlags::IN,
            );
            if let Err(e) = r {
                // we don't care about registering twice
                if e.kind() != ErrorKind::AlreadyExists {
                    error!("register: Error in registering stream: {}", r.unwrap_err());
                }
            }
        }

        self.wakers
            .lock()
            .map(|mut w| w.insert(nid, waker))
            .unwrap();
        return nid;
    }

    // deregisters tcp stream
    pub fn deregister(&self, stream: &TcpStream, id: u64) {
        // TODO: we never deregister..
        epoll::delete(&self.epoll, stream).unwrap();
        self.wakers.lock().map(|mut w| w.remove(&id)).unwrap();
    }
}

#[cfg(test)]
mod test {
    use std::net::TcpListener;

    use anyhow::Error;
    use rustix::event::epoll;
    fn test() -> Result<(), Error> {
        let listener = TcpListener::bind("127.0.0.1:6379")?;
        let (stream, _) = listener.accept()?;

        let epoll = epoll::create(epoll::CreateFlags::CLOEXEC).unwrap();
        // tracking when the events
        epoll::add(
            &epoll,
            stream,
            epoll::EventData::new_u64(1),
            epoll::EventFlags::IN,
        )?;

        Ok(())
    }
}
