//! Outbox pattern listener implementation for PostgreSQL

use std::{sync::Arc, time::Duration};

use futures::future::TryFutureExt;
use sqlx::{postgres::PgListener, Acquire, PgConnection, Row};
use thiserror::Error;
#[cfg(feature = "tracing")]
use tracing::{event, instrument, Level};

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

pub struct PgOutboxOptions {
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
    /// The queue to submit jobs into
    pub queue: Arc<effectum::Queue>,
}

pub struct PgOutbox {
    shutdown: tokio::sync::watch::Sender<bool>,
    listen_task: Option<tokio::task::JoinHandle<()>>,
}

struct PgOutboxListener {
    /// The postgres LISTEN channel to listen on. Use [DEFAULT_NOTIFY_CHANNEL] if you don't have
    /// another preference
    channel: Option<String>,
    /// If the job should only run tasks from a particular version of the code, then set this.
    code_version: Option<String>,
    /// The database pool
    db: sqlx::PgPool,
    /// The queue to submit jobs into
    queue: Arc<effectum::Queue>,
    get_rows_query: String,
    delete_query: String,
    lock_query: String,
    shutdown: tokio::sync::watch::Receiver<bool>,
}

impl PgOutbox {
    pub fn new(options: PgOutboxOptions) -> Self {
        let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
        let (get_rows_where, delete_where) = if options.code_version.is_some() {
            (
                format!("WHERE code_version = $1",),
                format!(" AND code_version = $2"),
            )
        } else {
            (String::new(), String::new())
        };

        let table = options.table.as_deref().unwrap_or(DEFAULT_OUTBOX_TABLE);

        let get_rows_query = format!(
            "SELECT id, payload
            timeout, max_retries, run_at, retry_backoff, operation
            FROM {table}
            {get_rows_where}
            ORDER BY id LIMIT 50",
        );

        let delete_query = format!("DELETE FROM {table} WHERE id <= $1{delete_where}",);

        let lock_query = format!(
            "SELECT pg_try_advisory_xact_lock({})",
            options.lock_key.unwrap_or(DEFAULT_LOCK_KEY)
        );

        let listener = PgOutboxListener {
            code_version: options.code_version,
            queue: options.queue,
            lock_query,
            get_rows_query,
            delete_query,
            channel: options.channel,
            db: options.db,
            shutdown: shutdown_rx,
        };
        let listen_task = tokio::task::spawn(listener.start());

        Self {
            shutdown: shutdown_tx,
            listen_task: Some(listen_task),
        }
    }

    pub async fn close(&mut self) {
        self.shutdown.send(true).ok();
        if let Some(task) = self.listen_task.take() {
            task.await.ok();
        }
    }
}

impl PgOutboxListener {
    async fn start(mut self) {
        let mut listener = None;

        loop {
            if *self.shutdown.borrow() {
                break;
            }

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
                            #[cfg(feature = "tracing")]
                            event!(Level::ERROR, error=?e, "Error listening for queue notify");
                            listener = None;
                        }
                    };
                }
                _ = self.shutdown.changed() => break,
            }
        }
    }

    #[cfg_attr(feature = "tracing", instrument(level = "DEBUG", skip(self)))]
    async fn try_drain(&self) -> Result<bool, OutboxError> {
        let mut conn = self.db.acquire().await?;
        let mut tx = conn.begin().await?;
        let lock_result = sqlx::query(&self.lock_query).fetch_one(&mut *tx).await?;

        let acquired_lock: bool = lock_result.get(0);
        if !acquired_lock {
            // Something else has the lock, so just exit and try again after a sleep.
            return Ok(false);
        }

        let jobs = self.get_jobs(&mut *tx).await?;
        if jobs.is_empty() {
            return Ok(false);
        }

        for OutboxRow { payload, .. } in jobs {
            let result = match payload.0 {
                QueueOperation::Add { job } => {
                    #[cfg(feature = "tracing")]
                    event!(Level::INFO, ?job, "Enqueueing job");
                    self.queue.add_job(job).await.map(|_| ())
                }
                QueueOperation::Remove { job_id } => {
                    #[cfg(feature = "tracing")]
                    event!(Level::INFO, job=%job_id, "Removing pending job");
                    self.queue.cancel_job(job_id).await
                }
                QueueOperation::Update { job } => {
                    #[cfg(feature = "tracing")]
                    event!(Level::INFO, ?job, "Updating pending job");
                    self.queue.update_job(job).await
                }
            };

            if let Err(e) = result {
                #[cfg(feature = "tracing")]
                event!(Level::ERROR, error=?e, "Error submitting job");

                // Don't bail if it failed just because an update or cancel happened too late.
                if !e.is_update_too_late() {
                    return Err(e.into());
                }
            }
        }
        tx.commit().await?;

        Ok(true)
    }

    async fn get_jobs(&self, tx: &mut PgConnection) -> Result<Vec<OutboxRow>, OutboxError> {
        let q = sqlx::query_as::<_, OutboxRow>(&self.get_rows_query);
        let q = if let Some(version) = &self.code_version {
            q.bind(version)
        } else {
            q
        };

        let results = q.fetch_all(&mut *tx).await?;

        if let Some(max_id) = results.last().map(|r| r.id) {
            let q = sqlx::query(&self.delete_query).bind(max_id);
            let q = if let Some(version) = &self.code_version {
                q.bind(version)
            } else {
                q
            };

            q.execute(&mut *tx).await?;
        }

        Ok(results)
    }
}
