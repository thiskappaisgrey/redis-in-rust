use std::{
    future::Future,
    sync::{
        mpsc::{sync_channel, Receiver, SyncSender},
        Arc, Mutex,
    },
    task::Context,
};

// Executor from the rust async book

use futures::{
    future::{BoxFuture, FutureExt},
    task::{waker_ref, ArcWake},
};

// A simple executor in Rust
// recieves work from another thread .. and executes them
pub struct Executor {
    ready_queue: Receiver<Arc<Task>>,
}

impl Executor {
    pub fn run(&self) {
        while let Ok(task) = self.ready_queue.recv() {
            let mut future_slot = task.future.lock().unwrap();
            // a task can be Some if it needs to be polled again
            // or None if it's done.
            //
            // We only poll the task when it's pending
            if let Some(mut future) = future_slot.take() {
                let waker = waker_ref(&task);
                let context = &mut Context::from_waker(&waker);

                // From a Pin<Box<dyn Future<Output>> ...
                // We want a Pin<&mut dyn Future> by calling as_mut
                // then call poll on it
                if future.as_mut().poll(context).is_pending() {
                    // put it back in its task to be run again in the future
                    *future_slot = Some(future);
                }
            };
        }
    }
}

#[derive(Clone)]
pub struct Spawner {
    task_sender: SyncSender<Arc<Task>>,
}

impl Spawner {
    pub fn spawn(&self, future: impl Future<Output = ()> + 'static + Send) {
        // box the future
        let future = future.boxed();
        let task = Arc::new(Task {
            future: Mutex::new(Some(future)),
            task_sender: self.task_sender.clone(),
        });

        // here .. we send the task to the executor thread
        // to be polled
        self.task_sender
            .try_send(task)
            .expect("Too many tasks queued");
    }
}

struct Task {
    /// In-progress future that should be pushed to completion
    ///
    /// the future is polled / mutated from one thread, so we could use unsafe-cell
    /// instead of mutex here..?
    future: Mutex<Option<BoxFuture<'static, ()>>>,

    /// Handle to place the task itself back onto the task queue.
    task_sender: SyncSender<Arc<Task>>,
}

// create a waker for task
impl ArcWake for Task {
    fn wake_by_ref(arc_self: &Arc<Self>) {
        let cloned = arc_self.clone();
        arc_self
            .task_sender
            .try_send(cloned)
            .expect("Too many tasks queued");
    }
}

pub fn new_executor_and_spawner() -> (Executor, Spawner) {
    const MAX_QUEUED_TASKS: usize = 10_000;
    // if I want to have a threadpool ..
    // this impl would have to use a MPMC
    let (task_sender, ready_queue) = sync_channel(MAX_QUEUED_TASKS);
    (Executor { ready_queue }, Spawner { task_sender })
}
