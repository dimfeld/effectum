use std::sync::Arc;

use rusqlite::Connection;

use crate::job_loop::WaitingWorkers;

pub(crate) struct SharedStateData {
    pub db: std::sync::Mutex<Connection>,
    /// Separate pool for miscellaneous read-only calls so they won't block the writes.
    pub read_conn_pool: deadpool_sqlite::Pool,
    pub waiting_workers: tokio::sync::Mutex<WaitingWorkers>,
    pub notify_updated: tokio::sync::Notify,
    pub notify_workers_done: tokio::sync::Notify,
    pub close: tokio::sync::watch::Receiver<()>,
}

pub(crate) type SharedState = Arc<SharedStateData>;
