use std::ops::Deref;
use std::sync::Arc;

use rusqlite::Connection;
use time::OffsetDateTime;

use crate::worker_list::Workers;
use crate::Result;

pub(crate) struct SharedStateData {
    pub db: std::sync::Mutex<Connection>,
    /// Separate pool for miscellaneous read-only calls so they won't block the writes.
    pub read_conn_pool: deadpool_sqlite::Pool,
    pub workers: tokio::sync::RwLock<Workers>,
    pub close: tokio::sync::watch::Receiver<()>,
    pub time: Time,
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

#[derive(Clone)]
pub(crate) struct Time {
    start_instant: tokio::time::Instant,
    start_time: time::OffsetDateTime,
}

impl Time {
    pub fn new() -> Self {
        let start_instant = tokio::time::Instant::now();
        let start_time = time::OffsetDateTime::now_utc();

        Time {
            start_instant,
            start_time,
        }
    }

    pub fn now(&self) -> OffsetDateTime {
        let now = tokio::time::Instant::now() - self.start_instant;
        self.start_time + now
    }
}
