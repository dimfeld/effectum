mod add_job;
mod db;
mod error;
mod job_loop;
mod migrations;
mod shared_state;
mod update_job;

use std::sync::Arc;

pub use error::{Error, Result};
use job_loop::{wait_for_pending_jobs, WaitingWorkers};
use rusqlite::Connection;
use shared_state::{SharedState, SharedStateData};
use tokio::sync::Mutex;

pub struct NewJob {
    job_type: String,
    run_at: time::OffsetDateTime,
    payload: Vec<u8>,
    num_retries_allowed: u32,
    backoff: backoff::ExponentialBackoff,
    timeout: time::Duration,
    heartbeat_expiration_increment: time::Duration,
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
        let conn = Connection::open(file).map_err(Error::OpenDatabase)?;
        conn.pragma_update(None, "journal", "wal")
            .map_err(Error::OpenDatabase)?;

        let (close_tx, close_rx) = tokio::sync::watch::channel(());

        let shared_state = Arc::new(SharedStateData {
            conn: Mutex::new(conn),
            waiting_workers: Mutex::new(WaitingWorkers {}),
            notify_updated: tokio::sync::Notify::new(),
            close: close_rx,
        });

        let q = Queue {
            close: close_tx,
            state: shared_state.clone(),
        };

        tokio::task::spawn(wait_for_pending_jobs(shared_state));
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
