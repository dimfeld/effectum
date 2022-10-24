use futures::future::join_all;
use futures::Future;
use serde::Serialize;
use std::fmt::Debug;
use std::sync::atomic::{AtomicU16, Ordering};
use std::sync::Arc;
use time::Duration;
use tokio::sync::{broadcast, oneshot, Notify};
use tokio::task::JoinHandle;

use crate::job::Job;
use crate::job_loop::ListeningWorker;
use crate::job_registry::JobRegistry;
use crate::shared_state::SharedState;
use crate::{Error, Queue, Result, SmartString};

type WorkerId = u64;

struct CancellableTask {
    close_tx: oneshot::Sender<()>,
    join_handle: JoinHandle<()>,
}

pub struct Worker {
    pub id: WorkerId,
    job_loop_task: Option<CancellableTask>,
}

impl Worker {
    pub async fn unregister(mut self, timeout: Option<Duration>) -> Result<()> {
        if let Some(task) = self.job_loop_task.take() {
            task.close_tx.send(()).ok();
            task.join_handle.await?;
        }
        Ok(())
    }

    fn run_ready_jobs(&mut self) {}
}

impl Drop for Worker {
    fn drop(&mut self) {
        if let Some(task) = self.job_loop_task.take() {
            task.close_tx.send(()).ok();
            tokio::spawn(task.join_handle);
        }
    }
}

impl<CONTEXT> WorkerInternal<CONTEXT>
where
    CONTEXT: Send + Sync + Debug + Clone + 'static,
{
    async fn close(state: &SharedState, id: WorkerId, timeout: Option<Duration>) -> Result<()> {
        let mut workers = state.workers.lock().await;
        workers.remove_worker(id).ok();
        drop(workers);

        Ok(())
    }
}

pub struct WorkerBuilder<'a, CONTEXT>
where
    CONTEXT: Send + Sync + Debug + Clone + 'static,
{
    /// The job registry from which this worker should take its job functions.
    registry: &'a JobRegistry<CONTEXT>,
    /// The context value to send to the worker's jobs.
    context: CONTEXT,
    /// Limit the job types this worker will run. Defaults to all job types in the registry.
    jobs: Vec<SmartString>,
    /// Fetch new jobs when the number of running jobs drops to this number. Defaults to
    /// max_concurrency / 2.
    min_concurrency: Option<u16>,
    /// The maximum number of jobs that can be run concurrently.
    /// Defaults to the highest weight of any job in the registry, accounting for `jobs` if set.
    max_concurrency: Option<u16>,
}

impl<'a, CONTEXT> WorkerBuilder<'a, CONTEXT>
where
    CONTEXT: Send + Sync + Debug + Clone + 'static,
{
    pub fn new(registry: &'a JobRegistry<CONTEXT>, context: CONTEXT) -> Self {
        Self {
            registry,
            context,
            jobs: Vec::new(),
            min_concurrency: None,
            max_concurrency: None,
        }
    }

    pub fn job_types(mut self, job_types: &[impl AsRef<str>]) -> Self {
        self.jobs = job_types
            .into_iter()
            .map(|s| {
                assert!(
                    self.registry.jobs.contains_key(s.as_ref()),
                    "Job type {} not found in registry",
                    s.as_ref()
                );

                SmartString::from(s.as_ref())
            })
            .collect();
        self
    }

    pub fn min_concurrency(mut self, min_concurrency: u16) -> Self {
        assert!(min_concurrency > 0);
        self.min_concurrency = Some(min_concurrency);
        self
    }

    pub fn max_concurrency(mut self, max_concurrency: u16) -> Self {
        assert!(max_concurrency > 0);
        self.max_concurrency = Some(max_concurrency);
        self
    }

    pub async fn build(self, queue: &Queue) -> Result<Worker> {
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

        let max_concurrency = self
            .max_concurrency
            .unwrap_or(jobs_max_concurrency)
            .max(jobs_max_concurrency);

        let min_concurrency = self.min_concurrency.unwrap_or(max_concurrency / 2).max(1);

        let (close_tx, close_rx) = oneshot::channel();

        let mut workers = queue.state.workers.lock().await;
        let listener = workers.add_worker(&job_list);
        drop(workers);

        let worker_id = listener.id;
        let worker_internal = WorkerInternal {
            listener,
            job_list,
            job_finished: Notify::new(),
            queue: queue.state.clone(),
            context: self.context,
            min_concurrency,
            max_concurrency,
            current_jobs: AtomicU16::new(0),
        };

        let join_handle = tokio::spawn(worker_internal.run(close_rx));

        Ok(Worker {
            id: worker_id,
            job_loop_task: Some(CancellableTask {
                close_tx,
                join_handle,
            }),
        })
    }
}

struct WorkerInternal<CONTEXT>
where
    CONTEXT: Send + Sync + Debug + Clone + 'static,
{
    listener: Arc<ListeningWorker>,
    queue: SharedState,
    job_list: Vec<SmartString>,
    job_finished: Notify,
    context: CONTEXT,
    current_jobs: AtomicU16,
    min_concurrency: u16,
    max_concurrency: u16,
}

impl<CONTEXT> WorkerInternal<CONTEXT>
where
    CONTEXT: Send + Sync + Debug + Clone + 'static,
{
    async fn run(self, mut close_rx: oneshot::Receiver<()>) {
        loop {
            let running_jobs = self.current_jobs.load(Ordering::Relaxed);
            let grab_new_jobs = running_jobs < self.min_concurrency;

            tokio::select! {
                biased;
                _ = &mut close_rx => {
                    let mut workers = self.queue.workers.lock().await;
                    workers.remove_worker(self.listener.id);
                    return;
                }
                _ = self.listener.notify_task_ready.notified(), if grab_new_jobs  => {
                    self.run_ready_jobs().await;
                }
                _ = self.job_finished.notified(), if !grab_new_jobs => {
                    continue;
                }
            }
        }
    }

    async fn run_ready_jobs(&self) {
        todo!()
    }
}
