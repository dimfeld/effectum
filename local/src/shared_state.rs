use std::sync::Arc;

use rusqlite::Connection;
use tokio::sync::Mutex;

use crate::job_loop::WaitingWorkers;

pub(crate) struct SharedStateData {
    pub conn: Mutex<Connection>,
    pub waiting_workers: Mutex<WaitingWorkers>,
    pub notify_updated: tokio::sync::Notify,
    pub close: tokio::sync::watch::Receiver<()>,
}

pub(crate) type SharedState = Arc<SharedStateData>;
