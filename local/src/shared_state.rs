use std::sync::Arc;

use rusqlite::Connection;

use crate::job_loop::WaitingWorkers;

pub(crate) struct SharedStateData {
    pub db: std::sync::Mutex<Connection>,
    pub waiting_workers: tokio::sync::Mutex<WaitingWorkers>,
    pub notify_updated: tokio::sync::Notify,
    pub close: tokio::sync::watch::Receiver<()>,
}

pub(crate) type SharedState = Arc<SharedStateData>;
