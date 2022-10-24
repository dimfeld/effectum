pub mod add_job;
mod db;
mod error;
mod job_loop;
pub mod job_status;
mod migrations;
mod shared_state;
mod update_job;

pub mod job;
pub mod job_registry;
mod sqlite_functions;
#[cfg(test)]
mod test_util;
pub mod worker;

use std::{path::Path, sync::Arc};

pub use error::{Error, Result};
use job_loop::{run_jobs_task, Workers};
use rusqlite::Connection;
use shared_state::{SharedState, SharedStateData};
use sqlite_functions::register_functions;
use time::Duration;
use tokio::{sync::Mutex, task::JoinHandle, time::error::Elapsed};

pub(crate) type SmartString = smartstring::SmartString<smartstring::LazyCompact>;

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

pub struct NewJob {
    job_type: String,
    priority: Option<i64>,
    /// When to run the job. `None` means to run it right away.
    run_at: Option<time::OffsetDateTime>,
    payload: Vec<u8>,
    retries: Retries,
    timeout: time::Duration,
    heartbeat_increment: time::Duration,
}

struct Tasks {
    close: tokio::sync::watch::Sender<()>,
    run_jobs_task: JoinHandle<Result<()>>,
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

        crate::migrations::migrate(&mut conn)?;

        register_functions(&mut conn)?;

        let (close_tx, close_rx) = tokio::sync::watch::channel(());

        let pool_cfg = deadpool_sqlite::Config::new(file);
        let read_conn_pool = pool_cfg.create_pool(deadpool_sqlite::Runtime::Tokio1)?;

        let notify_updated = Arc::new(tokio::sync::Notify::new());
        let shared_state = SharedState(Arc::new(SharedStateData {
            db: std::sync::Mutex::new(conn),
            read_conn_pool,
            workers: Mutex::new(Workers::new(notify_updated.clone())),
            notify_updated,
            notify_workers_done: tokio::sync::Notify::new(),
            close: close_rx,
        }));

        // TODO Optionally clean up running jobs here, treating them all as failures and scheduling
        // for retry. For later server mode, we probably want to do something more intelligent so
        // that we can continue to receive "job finished" notifications.

        let run_jobs_task = tokio::task::spawn(run_jobs_task(shared_state.clone()));

        let q = Queue {
            state: shared_state,
            tasks: Some(Tasks {
                close: close_tx,
                run_jobs_task,
            }),
        };

        Ok(q)
    }

    /// Stop the queue, and wait for existing workers to finish.
    pub async fn close(&mut self, timeout: time::Duration) -> Result<()> {
        if let Some(tasks) = self.tasks.take() {
            tasks.close.send(()).ok();

            let done_notify = self.state.notify_workers_done.notified();
            tokio::pin!(done_notify);
            done_notify.as_mut().enable();

            tasks.run_jobs_task.await??;
            tokio::time::timeout(timeout.unsigned_abs(), done_notify)
                .await
                .map_err(|_| Error::Timeout)?;
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
    use crate::test_util::create_test_queue;

    #[tokio::test]
    async fn create_queue() {
        create_test_queue();
    }
}
