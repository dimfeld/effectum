pub mod add_job;
mod error;
pub mod job_status;
mod migrations;
mod shared_state;
mod worker_list;

pub mod job;
pub mod job_registry;
mod sqlite_functions;
#[cfg(test)]
mod test_util;
pub mod worker;

use std::{path::Path, sync::Arc};

pub use error::{Error, Result};
use rusqlite::Connection;
use shared_state::{SharedState, SharedStateData};
use sqlite_functions::register_functions;
use time::Duration;
use worker_list::Workers;

pub(crate) type SmartString = smartstring::SmartString<smartstring::LazyCompact>;

#[derive(Debug, Clone)]
pub struct Retries {
    pub max_retries: u32,
    pub backoff_multiplier: f32,
    pub backoff_randomization: f32,
    pub backoff_initial_interval: Duration,
}

impl Default for Retries {
    fn default() -> Self {
        Self {
            max_retries: 3,
            backoff_multiplier: 2f32,
            backoff_randomization: 0.2,
            backoff_initial_interval: Duration::seconds(20),
        }
    }
}

#[derive(Debug, Clone)]
pub struct NewJob {
    job_type: String,
    priority: Option<i64>,
    /// When to run the job. `None` means to run it right away.
    run_at: Option<time::OffsetDateTime>,
    payload: Vec<u8>,
    retries: Retries,
    timeout: time::Duration,
    heartbeat_increment: time::Duration,
    recurring_job_id: Option<i64>,
}

impl Default for NewJob {
    fn default() -> Self {
        Self {
            job_type: Default::default(),
            priority: Default::default(),
            run_at: Default::default(),
            payload: Default::default(),
            retries: Default::default(),
            timeout: Duration::minutes(5),
            heartbeat_increment: Duration::seconds(120),
            recurring_job_id: Default::default(),
        }
    }
}

struct Tasks {
    close: tokio::sync::watch::Sender<()>,
    worker_count_rx: tokio::sync::watch::Receiver<usize>,
}

pub struct Queue {
    state: SharedState,
    tasks: Option<Tasks>,
}

impl Queue {
    /// Open or create a new Queue database at the given path.
    ///
    /// Note that if you use an existing database file, this queue will set the journal style to
    /// WAL mode.
    pub fn new(file: &Path) -> Result<Queue> {
        let mut conn = Connection::open(file).map_err(Error::OpenDatabase)?;
        conn.pragma_update(None, "journal", "wal")
            .map_err(Error::OpenDatabase)?;

        register_functions(&mut conn)?;
        crate::migrations::migrate(&mut conn)?;

        let (close_tx, close_rx) = tokio::sync::watch::channel(());

        let pool_cfg = deadpool_sqlite::Config::new(file);
        let read_conn_pool = pool_cfg.create_pool(deadpool_sqlite::Runtime::Tokio1)?;

        let (worker_count_tx, worker_count_rx) = tokio::sync::watch::channel(0);
        let shared_state = SharedState(Arc::new(SharedStateData {
            db: std::sync::Mutex::new(conn),
            read_conn_pool,
            workers: tokio::sync::RwLock::new(Workers::new(worker_count_tx)),
            close: close_rx,
        }));

        // TODO Optionally clean up running jobs here, treating them all as failures and scheduling
        // for retry. For later server mode, we probably want to do something more intelligent so
        // that we can continue to receive "job finished" notifications. This will probably involve
        // persisting the worker information to the database so we can properly recover it.

        // TODO task to monitor expired jobs
        // TODO task to schedule recurring jobs
        // TODO Optional task to delete old jobs from `done_jobs`

        let q = Queue {
            state: shared_state,
            tasks: Some(Tasks {
                close: close_tx,
                worker_count_rx,
            }),
        };

        Ok(q)
    }

    async fn wait_for_workers_to_stop(
        tasks: &mut Tasks,
        timeout: std::time::Duration,
    ) -> Result<()> {
        if *tasks.worker_count_rx.borrow_and_update() == 0 {
            return Ok(());
        }

        let timeout = tokio::time::sleep(timeout);
        tokio::pin!(timeout);

        loop {
            tokio::select! {
                _ = &mut timeout => return Err(Error::Timeout),
                res = tasks.worker_count_rx.changed() => {
                    if res.is_err() || *tasks.worker_count_rx.borrow() == 0 {
                        return Ok(());
                    }
                }
            }
        }
    }

    /// Stop the queue, and wait for existing workers to finish.
    pub async fn close(&mut self, timeout: std::time::Duration) -> Result<()> {
        if let Some(mut tasks) = self.tasks.take() {
            tasks.close.send(()).ok();
            Self::wait_for_workers_to_stop(&mut tasks, timeout).await?;
        }
        Ok(())
    }
}

impl Drop for Queue {
    fn drop(&mut self) {
        if let Some(tasks) = self.tasks.take() {
            tasks.close.send(()).ok();
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use crate::{
        job_registry::{JobDef, JobRegistry},
        test_util::{create_test_queue, wait_for, wait_for_job, TestContext, TestEnvironment},
        worker::Worker,
        NewJob,
    };

    #[tokio::test]
    async fn create_queue() {
        create_test_queue();
    }

    #[tokio::test]
    async fn run_job() {
        let test = TestEnvironment::default();

        let _worker = test.worker().build().await.expect("failed to build worker");

        let (_, job_id) = test
            .queue
            .add_job(NewJob {
                job_type: "counter".to_string(),
                ..Default::default()
            })
            .await
            .expect("failed to add job");

        wait_for_job("job to run", &test.queue, job_id).await;
    }

    #[tokio::test]
    async fn worker_gets_pending_jobs_when_starting() {
        let test = TestEnvironment::default();

        let (_, job_id) = test
            .queue
            .add_job(NewJob {
                job_type: "counter".to_string(),
                ..Default::default()
            })
            .await
            .expect("failed to add job");

        let _worker = test.worker().build().await.expect("failed to build worker");

        wait_for_job("job to run", &test.queue, job_id).await;
    }

    #[tokio::test]
    async fn run_future_job() {
        todo!();
    }

    mod retry {
        #[tokio::test]
        async fn success_after_retry() {
            todo!();
        }

        #[tokio::test]
        async fn exceed_max_retries() {
            todo!();
        }

        #[tokio::test]
        async fn retry_max_backoff() {
            todo!();
        }
    }

    #[tokio::test]
    async fn explicit_complete() {
        todo!();
    }

    #[tokio::test]
    async fn explicit_fail() {
        todo!();
    }

    #[tokio::test]
    #[ignore]
    async fn remove_jobs() {
        unimplemented!();
    }

    #[tokio::test]
    #[ignore]
    async fn clear_jobs() {
        unimplemented!();
    }

    #[tokio::test]
    async fn weighted_jobs() {
        todo!();
    }

    #[tokio::test]
    #[ignore]
    async fn recurring_jobs() {
        unimplemented!();
    }

    #[tokio::test]
    async fn job_type_subset() {
        todo!();
    }

    #[tokio::test]
    async fn job_timeout() {
        // TODO Need to track by a specific job_run_id, not just the worker id, since
        // the next run of the job could assign it to the same worker again.
        todo!();
    }

    #[tokio::test]
    async fn manual_heartbeat() {
        todo!();
    }

    #[tokio::test]
    async fn auto_heartbeat() {
        todo!();
    }

    #[tokio::test]
    async fn job_priority() {
        todo!();
    }

    #[tokio::test]
    async fn checkpoint() {
        // Set a checkpoint
        // Fail the first run
        // Ensure that the second run starts from the checkpoint.
        todo!();
    }

    mod concurrency {
        #[tokio::test]
        async fn limit_to_max_concurrency() {
            todo!();
        }

        #[tokio::test]
        async fn fetches_batch() {
            todo!();
        }

        #[tokio::test]
        async fn fetches_again_at_min_concurrency() {
            todo!();
        }
    }

    #[tokio::test]
    async fn shutdown() {
        todo!();
    }
}
