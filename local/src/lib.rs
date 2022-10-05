mod add_job;
mod db;
mod error;
mod job_loop;
mod migrations;
mod shared_state;
mod update_job;

use std::sync::Arc;

pub use error::{Error, Result};
use job_loop::{run_jobs_task, WaitingWorkers};
use rusqlite::Connection;
use shared_state::{SharedState, SharedStateData};
use time::Duration;
use tokio::sync::Mutex;

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
    run_at: Option<time::OffsetDateTime>,
    payload: Vec<u8>,
    retries: Retries,
    timeout: time::Duration,
    heartbeat_increment: time::Duration,
}

struct Queue {
    close: tokio::sync::watch::Sender<()>,
    state: SharedState,
}

impl Queue {
    /// Create a new Queue at the given file path. This is a normal SQLite database connection
    /// string, so you can use `:memory:` to create an in-memory queue, with the normal caveats
    /// about lack of persistence.
    ///
    /// Note that if you use an existing database file, this queue will set the journal style to
    /// WAL mode.
    pub fn new(file: &str) -> Result<Queue> {
        let mut conn = Connection::open(file).map_err(Error::OpenDatabase)?;
        conn.pragma_update(None, "journal", "wal")
            .map_err(Error::OpenDatabase)?;

        crate::migrations::migrate(&mut conn)?;

        let (close_tx, close_rx) = tokio::sync::watch::channel(());

        let shared_state = Arc::new(SharedStateData {
            db: std::sync::Mutex::new(conn),
            waiting_workers: Mutex::new(WaitingWorkers {}),
            notify_updated: tokio::sync::Notify::new(),
            close: close_rx,
        });

        let q = Queue {
            close: close_tx,
            state: shared_state.clone(),
        };

        tokio::task::spawn(run_jobs_task(shared_state));
        Ok(q)
    }
}

#[cfg(test)]
mod tests {
    #[tokio::test]
    async fn create_queue() {
        super::Queue::new(":memory:").unwrap();
    }
}
