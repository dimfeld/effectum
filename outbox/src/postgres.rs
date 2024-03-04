//! Outbox pattern listener implementation for PostgreSQL

use std::{sync::Arc, time::Duration};

use futures::future::TryFutureExt;
use sqlx::{postgres::PgListener, Acquire, Row};
use thiserror::Error;
#[cfg(feature = "tracing")]
use tracing::{event, instrument, Level};
use uuid::Uuid;

use crate::{OutboxRow, QueueOperation};

#[derive(Debug, Error)]
pub enum OutboxError {
    #[error("Database error {0}")]
    Db(#[from] sqlx::Error),
    #[error("Error communicating with queue {0}")]
    Queue(#[from] effectum::Error),
}

const DEFAULT_NOTIFY_CHANNEL: &str = "effectum-task-outbox";
const DEFAULT_OUTBOX_TABLE: &str = "effectum_outbox";
const DEFAULT_LOCK_KEY: u64 = 0x9c766a023f590ad;

pub struct PgOutbox {
    /// The name of the outbox table.
    pub table: Option<String>,
    /// The postgres LISTEN channel to listen on. Use [DEFAULT_NOTIFY_CHANNEL] if you don't have
    /// another preference
    pub channel: Option<String>,
    /// If the job should only run tasks from a particular version of the code, then set this.
    pub code_version: Option<String>,
    /// The database pool
    pub db: sqlx::PgPool,
    /// A unique key used to reduce contention on the database from multiple drainers.
    /// If `code_version` is in use, each particular code version should have a different `lock_key`.
    pub lock_key: Option<u64>,
    pub queue: Arc<effectum::Queue>,
}

impl PgOutbox {
    pub fn start(self) -> (tokio::sync::watch::Sender<()>, tokio::task::JoinHandle<()>) {
        let (close_tx, close_rx) = tokio::sync::watch::channel(());
        let listener = self.start_with_shutdown(close_rx);
        (close_tx, listener)
    }

    pub fn start_with_shutdown(
        self,
        shutdown: tokio::sync::watch::Receiver<()>,
    ) -> tokio::task::JoinHandle<()> {
        tokio::task::spawn(self.listener_task(shutdown))
    }

    async fn listener_task(mut self, mut shutdown: tokio::sync::watch::Receiver<()>) {
        let mut listener = None;

        loop {
            if listener.is_none() {
                let l = PgListener::connect_with(&self.db)
                    .and_then(|mut l| {
                        let channel = self.channel.as_deref().unwrap_or(DEFAULT_NOTIFY_CHANNEL);
                        async move {
                            l.listen(channel).await?;
                            Ok(l)
                        }
                    })
                    .await;

                match l {
                    Ok(l) => {
                        listener = Some(l);
                    }
                    Err(e) => {
                        #[cfg(feature = "tracing")]
                        event!(Level::ERROR, error=?e, "Error creating Postgres queue listener");
                    }
                };
            }

            match self.try_drain().await {
                Ok(true) => {
                    // We got some rows, so check again in case there are more.
                    continue;
                }
                // No rows, so pass through back to listening again.
                Ok(false) => {}
                Err(e) => {
                    #[cfg(feature = "tracing")]
                    event!(Level::ERROR, error=?e, "Error draining job queue");
                }
            };

            tokio::select! {
                // If we failed to create the listener, then try again in 5 seconds.
                _ = tokio::time::sleep(Duration::from_secs(5)), if listener.is_none() => continue,
                notify = listener.as_mut().unwrap().try_recv(), if listener.is_some() => {
                    match notify {
                        Ok(Some(_)) => {
                            // Got a notification so loop around. We also destroy the listener here
                            // so that the notification queue is emptied to avoid spamming the
                            // database with queries.
                            listener = None;
                        },
                        Ok(None) => {
                            // Connection died. Normally the listener would restart itself, but
                            // instead we kill it here so that we can manually restart the
                            // connection before running `try_drain`.
                            listener = None;
                        }
                        Err(e) => {
                            event!(Level::ERROR, error=?e, "Error listening for queue notify");
                            listener = None;
                        }
                    };
                }
                _ = shutdown.changed() => break,
            }
        }
    }

    #[cfg_attr(feature = "tracing", instrument(level = "DEBUG", skip(self)))]
    async fn try_drain(&mut self) -> Result<bool, OutboxError> {
        let mut conn = self.db.acquire().await?;
        let mut tx = conn.begin().await?;
        let lock_result = sqlx::query(&format!(
            "SELECT pg_try_advisory_xact_lock({})",
            self.lock_key.unwrap_or(DEFAULT_LOCK_KEY)
        ))
        .fetch_one(&mut *tx)
        .await?;

        let acquired_lock: bool = lock_result.get(0);
        if !acquired_lock {
            // Something else has the lock, so just exit and try again after a sleep.
            return Ok(false);
        }

        let table = self.table.as_deref().unwrap_or(DEFAULT_OUTBOX_TABLE);
        let jobs: Vec<OutboxRow> = todo!("get jobs from queue");

        if jobs.is_empty() {
            return Ok(false);
        }

        for OutboxRow { payload, .. } in &jobs {
            match payload.0 {
                QueueOperation::Add { job } => {
                    #[cfg(feature = "tracing")]
                    event!(Level::INFO, ?job, "Enqueueing job");
                    self.queue.add_job(job).await?;
                }
                QueueOperation::Remove { job_id } => {
                    #[cfg(feature = "tracing")]
                    event!(Level::INFO, job=%job_id, "Removing pending job");
                    self.queue.cancel_job(job_id).await?;
                }
                QueueOperation::Update { job } => {
                    #[cfg(feature = "tracing")]
                    event!(Level::INFO, ?job, "Updating pending job");
                    self.queue.update_job(job).await?;
                }
            }
        }
        tx.commit().await?;

        Ok(true)
    }
}
