use futures::Future;
use serde::Serialize;
use std::fmt::Debug;

use crate::job::Job;
use crate::job_registry::JobRegistry;
use crate::shared_state::SharedState;
use crate::{Error, Queue, Result, SmartString};

type WorkerId = u64;

pub struct Worker<CONTEXT>
where
    CONTEXT: Send + Sync + Debug + Clone + 'static,
{
    id: WorkerId,
    queue: SharedState,
    closed: bool,
    context: CONTEXT,
    min_concurrency: usize,
    max_concurrency: usize,
}

impl<CONTEXT> Worker<CONTEXT>
where
    CONTEXT: Send + Sync + Debug + Clone + 'static,
{
    pub async fn new<F, Fut, T, E>(
        &self,
        queue: &Queue,
        registry: &JobRegistry<CONTEXT>,
    ) -> Result<()>
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

impl<CONTEXT> Drop for Worker<CONTEXT>
where
    CONTEXT: Send + Sync + Debug + Clone + 'static,
{
    fn drop(&mut self) {
        if !self.closed {
            let state = self.queue.clone();
            let id = self.id;
            tokio::task::spawn(async move { Self::close_internal(&state, id).await });
        }
    }
}

pub struct WorkerBuilder<'a, CONTEXT>
where
    CONTEXT: Send + Sync + Debug + Clone + 'static,
{
    registry: &'a JobRegistry<CONTEXT>,
    context: CONTEXT,
    jobs: Vec<SmartString>,
    min_concurrency: Option<usize>,
    max_concurrency: Option<usize>,
}

impl<'a, CONTEXT> WorkerBuilder<'a, CONTEXT>
where
    CONTEXT: Send + Sync + Debug + Clone + 'static,
{
    pub fn new(registry: &JobRegistry<CONTEXT>, context: CONTEXT) -> Self {
        Self {
            registry,
            context,
            jobs: Vec::new(),
            min_concurrency: None,
            max_concurrency: None,
        }
    }

    pub fn allow_jobs(mut self, jobs: &[impl AsRef<str>]) -> Self {
        self.jobs = jobs
            .into_iter()
            .map(|s| SmartString::from(s.as_ref()))
            .collect();
        self
    }

    pub fn min_concurrency(mut self, min_concurrency: usize) -> Self {
        self.min_concurrency = Some(min_concurrency);
        self
    }

    pub fn max_concurrency(mut self, max_concurrency: usize) -> Self {
        self.max_concurrency = Some(max_concurrency);
        self
    }

    pub async fn build(self, queue: &Queue) -> Result<Worker<CONTEXT>> {
        let job_list = if self.jobs.is_empty() {
            self.registry.jobs.keys().cloned().collect()
        } else {
            self.jobs
        };

        let jobs_max_concurrency = job_list
            .iter()
            .map(|job| self.registry.jobs.get(job).map(|d| d.weight).unwrap_or(1))
            .max()
            .unwrap_or(1);
        let max_concurrency = self.max_concurrency.unwrap_or(jobs_max_concurrency);

        let mut workers = queue.state.workers.lock().await;
        let worker_id = workers.add_worker(job_list);
        drop(workers);
        queue.state.notify_updated.notify_one();

        Ok(Worker {
            id: worker_id,
            queue: queue.state.clone(),
            closed: false,
            context: self.context,
            min_concurrency: self.min_concurrency.unwrap_or(1),
            max_concurrency,
        })
    }
}
