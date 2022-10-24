use std::ops::Deref;
use std::sync::Arc;

use rusqlite::Connection;

use crate::job_loop::Workers;
use crate::Result;

pub(crate) struct SharedStateData {
    pub db: std::sync::Mutex<Connection>,
    /// Separate pool for miscellaneous read-only calls so they won't block the writes.
    pub read_conn_pool: deadpool_sqlite::Pool,
    pub workers: tokio::sync::Mutex<Workers>,
    pub notify_updated: tokio::sync::Notify,
    pub notify_workers_done: tokio::sync::Notify,
    pub close: tokio::sync::watch::Receiver<()>,
}

#[derive(Clone)]
pub(crate) struct SharedState(pub Arc<SharedStateData>);

impl Deref for SharedState {
    type Target = Arc<SharedStateData>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl SharedState {
    pub(crate) async fn write_db<F, T>(&self, f: F) -> Result<T>
    where
        F: (FnOnce(&mut Connection) -> Result<T>) + Send + 'static,
        T: Send + 'static,
    {
        let state = self.0.clone();
        let result = tokio::task::spawn_blocking(move || {
            let mut db = state.db.lock().unwrap();
            f(&mut db)
        })
        .await??;

        Ok(result)
    }
}
