use std::ops::Deref;
use std::sync::Arc;
use std::time::Duration;

use time::OffsetDateTime;
use tokio::sync::mpsc;
use tokio::time::Instant;

use crate::db_writer::DbOperation;
use crate::pending_jobs::ScheduledJobType;
use crate::worker_list::Workers;

pub(crate) struct SharedStateData {
    pub db_write_tx: mpsc::Sender<DbOperation>,
    /// Separate pool for miscellaneous read-only calls so they won't block the writes.
    pub read_conn_pool: deadpool_sqlite::Pool,
    pub workers: tokio::sync::RwLock<Workers>,
    pub close: tokio::sync::watch::Receiver<()>,
    pub time: Time,
    pub pending_jobs_tx: tokio::sync::mpsc::Sender<ScheduledJobType>,
}

#[derive(Clone)]
pub(crate) struct SharedState(pub Arc<SharedStateData>);

impl Deref for SharedState {
    type Target = Arc<SharedStateData>;

    fn deref(&self) -> &Self::Target {
        &self.0
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
        let now = self.start_instant.elapsed();
        self.start_time + now
    }

    pub fn instant_for_timestamp(&self, timestamp: i64) -> Instant {
        let ts = std::cmp::max(timestamp - self.start_time.unix_timestamp(), 0) as u64;
        let duration = Duration::from_secs(ts);
        self.start_instant + duration
    }
}
