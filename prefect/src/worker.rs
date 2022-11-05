use ahash::HashMap;
use std::fmt::Debug;
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::sync::Arc;
use tokio::sync::{oneshot, Notify};
use tokio::task::JoinHandle;
use tokio::time::Instant;
use tracing::{event, instrument, Level, Span};

use crate::db_writer::ready_jobs::{GetReadyJobsArgs, ReadyJob};
use crate::db_writer::{DbOperation, DbOperationType};
use crate::job_registry::{JobDef, JobRegistry};
use crate::shared_state::{SharedState, Time};
use crate::worker_list::ListeningWorker;
use crate::{Error, Queue, Result, SmartString};

type WorkerId = u64;

struct CancellableTask {
    close_tx: oneshot::Sender<()>,
    join_handle: JoinHandle<()>,
}

pub struct Worker {
    pub id: WorkerId,
    counts: Arc<RunningJobs>,
    worker_list_task: Option<CancellableTask>,
}

pub struct WorkerCounts {
    pub started: u64,
    pub finished: u64,
}

impl Worker {
    pub async fn unregister(mut self, timeout: Option<std::time::Duration>) -> Result<()> {
        if let Some(task) = self.worker_list_task.take() {
            task.close_tx.send(()).ok();
            if let Some(timeout) = timeout {
                tokio::time::timeout(timeout, task.join_handle)
                    .await
                    .map_err(|_| Error::Timeout)??;
            } else {
                task.join_handle.await?;
            }
        }
        Ok(())
    }

    pub fn builder<'a, CONTEXT>(queue: &'a Queue, context: CONTEXT) -> WorkerBuilder<'a, CONTEXT>
    where
        CONTEXT: Send + Sync + Debug + Clone + 'static,
    {
        WorkerBuilder::new(queue, context)
    }

    pub fn counts(&self) -> WorkerCounts {
        WorkerCounts {
            started: self.counts.started.load(Ordering::Relaxed),
            finished: self.counts.finished.load(Ordering::Relaxed),
        }
    }
}

impl Drop for Worker {
    fn drop(&mut self) {
        if let Some(task) = self.worker_list_task.take() {
            task.close_tx.send(()).ok();
            tokio::spawn(task.join_handle);
        }
    }
}

pub struct WorkerBuilder<'a, CONTEXT>
where
    CONTEXT: Send + Sync + Debug + Clone + 'static,
{
    /// The job registry from which this worker should take its job functions.
    registry: Option<&'a JobRegistry<CONTEXT>>,
    job_defs: Option<Vec<JobDef<CONTEXT>>>,
    queue: &'a Queue,
    /// The context value to send to the worker's jobs.
    context: CONTEXT,
    /// Limit the job types this worker will run. Defaults to all job types in the registry.
    jobs: Vec<SmartString>,
    /// Fetch new jobs when the number of running jobs drops to this number. Defaults to
    /// the same as max_concurrency.
    min_concurrency: Option<u16>,
    /// The maximum number of jobs that can be run concurrently. Defaults to 1, but you will
    /// usually want to set this to a higher number.
    max_concurrency: Option<u16>,
}

impl<'a, CONTEXT> WorkerBuilder<'a, CONTEXT>
where
    CONTEXT: Send + Sync + Debug + Clone + 'static,
{
    pub fn new(queue: &'a Queue, context: CONTEXT) -> Self {
        Self {
            registry: None,
            job_defs: None,
            queue,
            context,
            jobs: Vec::new(),
            min_concurrency: None,
            max_concurrency: None,
        }
    }

    /// Get the job definitions from this JobRegistry object.
    pub fn registry(mut self, registry: &'a JobRegistry<CONTEXT>) -> Self {
        if self.job_defs.is_some() {
            panic!("Cannot set both registry and job_defs");
        }

        self.registry = Some(registry);
        self
    }

    /// Get the job definitions from this list of jobs.
    pub fn jobs(mut self, jobs: impl Into<Vec<JobDef<CONTEXT>>>) -> Self {
        if self.job_defs.is_some() {
            panic!("Cannot set both registry and job_defs");
        }

        self.job_defs = Some(jobs.into());
        self
    }

    fn has_job_type(&self, job_type: &str) -> bool {
        if let Some(job_defs) = self.job_defs.as_ref() {
            job_defs.iter().any(|job_def| job_def.name == job_type)
        } else if let Some(registry) = self.registry.as_ref() {
            registry.jobs.contains_key(job_type)
        } else {
            panic!("Must set either registry or job_defs");
        }
    }

    /// Limit this worker to only running these job types, even if the registry contains more
    /// types.
    pub fn limit_job_types(mut self, job_types: &[impl AsRef<str>]) -> Self {
        self.jobs = job_types
            .iter()
            .map(|s| {
                assert!(
                    self.has_job_type(s.as_ref()),
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

    pub async fn build(self) -> Result<Worker> {
        let job_defs: HashMap<SmartString, JobDef<CONTEXT>> = if let Some(job_defs) = self.job_defs
        {
            job_defs
                .into_iter()
                .filter(|job| self.jobs.is_empty() || self.jobs.contains(&job.name))
                .map(|job| (job.name.clone(), job))
                .collect()
        } else if let Some(registry) = self.registry {
            let job_list = if self.jobs.is_empty() {
                registry.jobs.keys().cloned().collect()
            } else {
                self.jobs
            };

            job_list
                .iter()
                .filter_map(|job| {
                    registry
                        .jobs
                        .get(job)
                        .map(|job_def| (job.clone(), job_def.clone()))
                })
                .collect()
        } else {
            panic!("Must set either registry or jobs");
        };

        let max_concurrency = self.max_concurrency.unwrap_or(1).max(1);
        let min_concurrency = self.min_concurrency.unwrap_or(max_concurrency).max(1);

        let job_list = job_defs.keys().cloned().collect::<Vec<_>>();

        event!(
            Level::INFO,
            ?job_list,
            min_concurrency,
            max_concurrency,
            "Starting worker",
        );

        let (close_tx, close_rx) = oneshot::channel();

        let mut workers = self.queue.state.workers.write().await;
        let listener = workers.add_worker(&job_list);
        drop(workers);

        let counts = Arc::new(RunningJobs {
            started: AtomicU64::new(0),
            finished: AtomicU64::new(0),
            current_weighted: AtomicU32::new(0),
            job_finished: Notify::new(),
        });

        let worker_id = listener.id;
        let worker_internal = WorkerInternal {
            listener,
            running_jobs: counts.clone(),
            job_list: job_list.into_iter().map(String::from).collect(),
            job_defs: Arc::new(job_defs),
            queue: self.queue.state.clone(),
            context: self.context,
            min_concurrency,
            max_concurrency,
        };

        let join_handle = tokio::spawn(worker_internal.run(close_rx));

        Ok(Worker {
            id: worker_id,
            counts,
            worker_list_task: Some(CancellableTask {
                close_tx,
                join_handle,
            }),
        })
    }
}

pub(crate) struct RunningJobs {
    pub started: AtomicU64,
    pub finished: AtomicU64,
    pub current_weighted: AtomicU32,
    pub job_finished: Notify,
}

struct WorkerInternal<CONTEXT>
where
    CONTEXT: Send + Sync + Debug + Clone + 'static,
{
    listener: Arc<ListeningWorker>,
    queue: SharedState,
    job_list: Vec<String>,
    job_defs: Arc<HashMap<SmartString, JobDef<CONTEXT>>>,
    running_jobs: Arc<RunningJobs>,
    context: CONTEXT,
    min_concurrency: u16,
    max_concurrency: u16,
}

pub(crate) fn log_error<T, E>(result: Result<T, E>)
where
    E: std::error::Error,
{
    if let Err(e) = result {
        event!(Level::ERROR, ?e);
    }
}

impl<CONTEXT> WorkerInternal<CONTEXT>
where
    CONTEXT: Send + Sync + Debug + Clone + 'static,
{
    #[instrument(parent = None, name="worker_loop", skip_all, fields(worker_id = %self.listener.id))]
    async fn run(self, mut close_rx: oneshot::Receiver<()>) {
        let mut global_close_rx = self.queue.close.clone();
        loop {
            let mut running_jobs = self.running_jobs.current_weighted.load(Ordering::Relaxed);
            let min_concurrency = self.min_concurrency as u32;
            if running_jobs < min_concurrency {
                log_error(self.run_ready_jobs().await);
                running_jobs = self.running_jobs.current_weighted.load(Ordering::Relaxed);
            }

            let grab_new_jobs = running_jobs < min_concurrency;

            tokio::select! {
                biased;
                _ = &mut close_rx => {
                    log_error(self.shutdown().await);
                    break;
                }
                _ = global_close_rx.changed() => {
                    log_error(self.shutdown().await);
                    break;
                }
                _ = self.listener.notify_task_ready.notified(), if grab_new_jobs  => {
                    event!(Level::TRACE, "New task ready");
                }
                _ = self.running_jobs.job_finished.notified() => {
                    event!(Level::TRACE, "Job finished");
                }
            }
        }
    }

    async fn shutdown(&self) -> Result<()> {
        let mut running_jobs = self.running_jobs.current_weighted.load(Ordering::Relaxed);
        while running_jobs > 0 {
            self.running_jobs.job_finished.notified().await;
            running_jobs = self.running_jobs.current_weighted.load(Ordering::Relaxed);
        }

        let mut workers = self.queue.workers.write().await;
        workers.remove_worker(self.listener.id)
    }

    async fn run_ready_jobs(&self) -> Result<()> {
        let running_count = self.running_jobs.current_weighted.load(Ordering::Relaxed);
        let max_concurrency = self.max_concurrency as u32;
        let max_jobs = max_concurrency - running_count;
        let job_types = self
            .job_list
            .iter()
            .map(|s| rusqlite::types::Value::from(s.clone()))
            .collect::<Vec<_>>();

        let running_jobs = self.running_jobs.clone();
        let worker_id = self.listener.id;
        let now = self.queue.time.now();
        event!(Level::TRACE, %now, current_running = %running_count, %max_concurrency, "Checking ready jobs");

        let (result_tx, result_rx) = oneshot::channel();
        self.queue
            .db_write_tx
            .send(DbOperation {
                worker_id,
                span: Span::current(),
                operation: DbOperationType::GetReadyJobs(GetReadyJobsArgs {
                    job_types,
                    max_jobs,
                    max_concurrency,
                    running_jobs,
                    now,
                    result_tx,
                }),
            })
            .await
            .map_err(|_| Error::QueueClosed)?;

        let ready_jobs = result_rx.await.map_err(|_| Error::QueueClosed)??;

        for job in ready_jobs {
            self.run_job(job).await?;
        }

        Ok(())
    }

    #[instrument(level="debug", skip(self, done), fields(worker_id = %self.listener.id))]
    async fn run_job(
        &self,
        ReadyJob {
            job,
            done_rx: mut done,
        }: ReadyJob,
    ) -> Result<()> {
        let job_def = self
            .job_defs
            .get(job.job_type.as_str())
            .expect("Got job for unsupported type");

        let worker_id = self.listener.id;
        let running = self.running_jobs.clone();
        let autoheartbeat = job_def.autoheartbeat;
        let time = job.queue.time.clone();

        (job_def.runner)(job.clone(), self.context.clone());

        tokio::spawn(async move {
            let use_autohearbeat = autoheartbeat && job.heartbeat_increment > 0;
            event!(Level::DEBUG, ?job, "Starting job monitor task");
            loop {
                let expires = job.expires.load(Ordering::Relaxed);
                let expires_instant = time.instant_for_timestamp(expires);

                tokio::select! {
                    _ = wait_for_next_autoheartbeat(&time, expires, job.heartbeat_increment), if use_autohearbeat => {
                        event!(Level::DEBUG, %job, "Sending autoheartbeat");
                        let new_time =
                            crate::job::send_heartbeat(job.job_id, worker_id, job.heartbeat_increment, &job.queue).await;

                        match new_time {
                            Ok(new_time) => job.expires.store(new_time.unix_timestamp(), Ordering::Relaxed),
                            Err(e) => event!(Level::ERROR, ?e),
                        }
                    }
                    _ = tokio::time::sleep_until(expires_instant) => {
                        event!(Level::DEBUG, %job, "Job expired");
                        let now_expires = job.expires.load(Ordering::Relaxed);
                        if now_expires == expires {
                            if !job.is_done().await {
                                log_error(job.fail("Job expired").await);
                            }
                            break;
                        }
                    }
                    _ = done.changed() => {
                        break;
                    }
                }
            }

            // Do this in a separate task from the job runner so that even if something goes horribly wrong
            // we'll still be able to update the internal counts.
            running
                .current_weighted
                .fetch_sub(job.weight as u32, Ordering::Relaxed);
            running.finished.fetch_add(1, Ordering::Relaxed);
            running.job_finished.notify_one();
        });

        Ok(())
    }
}

async fn wait_for_next_autoheartbeat(time: &Time, expires: i64, heartbeat_increment: i32) {
    let now = time.now();
    let before = (heartbeat_increment.min(30) / 2) as i64;
    let next_heartbeat_time = expires - before;

    let time_from_now = next_heartbeat_time - now.unix_timestamp();
    let instant = Instant::now() + std::time::Duration::from_secs(time_from_now.max(0) as u64);

    tokio::time::sleep_until(instant).await
}
