use futures::Future;
use serde::Serialize;

use crate::job::Job;
use crate::shared_state::SharedState;
use crate::{Error, Queue, Result};

impl Queue {
    pub async fn add_worker<T: Into<String>>(job_types: &[T]) -> Result<Worker> {
        todo!()
    }
}

type WorkerId = u64;

pub struct Worker {
    id: WorkerId,
    autoheartbeat: bool,
    queue: SharedState,
    closed: bool,
}

impl Worker {
    pub async fn run<F, Fut, T, E>(&self, f: F) -> Result<()>
    where
        F: FnMut(Job) -> Fut,
        Fut: Future<Output = Result<T, E>>,
        T: Serialize,
        E: Serialize,
    {
        todo!()
    }

    pub async fn unregister(mut self) -> Result<()> {
        self.closed = true;
        Self::close_internal(&self.queue, self.id).await
    }

    async fn close_internal(state: &SharedState, id: WorkerId) -> Result<()> {
        todo!();
    }
}

impl Drop for Worker {
    fn drop(&mut self) {
        if !self.closed {
            let state = self.queue.clone();
            let id = self.id;
            tokio::task::spawn(async move { Self::close_internal(&state, id).await });
        }
    }
}
