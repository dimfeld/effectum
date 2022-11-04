use std::fmt::{Debug, Display};
use std::ops::Deref;
use std::sync::atomic::AtomicI64;
use std::sync::Arc;

use serde::{Deserialize, Serialize};
use time::OffsetDateTime;
use tokio::sync::Mutex;
use tracing::{event, instrument, Level, Span};
use uuid::Uuid;

use crate::db_writer::complete::CompleteJobArgs;
use crate::db_writer::heartbeat::{WriteCheckpointArgs, WriteHeartbeatArgs};
use crate::db_writer::retry::RetryJobArgs;
use crate::db_writer::{DbOperation, DbOperationType};
use crate::job_status::RunInfo;
use crate::shared_state::SharedState;
use crate::worker::log_error;
use crate::{Error, Result, SmartString};

#[derive(Debug, Clone)]
pub struct Job(pub Arc<JobData>);

impl Deref for Job {
    type Target = JobData;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl Display for Job {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

pub struct JobData {
    pub id: Uuid,
    pub(crate) job_id: i64,
    pub worker_id: u64,
    pub heartbeat_increment: i32,
    pub job_type: String,
    pub priority: i32,
    pub weight: u16,
    pub payload: Vec<u8>,
    pub expires: AtomicI64,

    pub start_time: OffsetDateTime,

    pub backoff_multiplier: f64,
    pub backoff_randomization: f64,
    pub backoff_initial_interval: i32,
    pub current_try: i32,
    pub max_retries: i32,

    pub(crate) done: Mutex<Option<tokio::sync::watch::Sender<bool>>>,
    pub(crate) queue: SharedState,
}

impl Debug for JobData {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Job")
            .field("id", &self.id)
            .field("job_id", &self.job_id)
            .field("worker_id", &self.worker_id)
            .field("heartbeat_increment", &self.heartbeat_increment)
            .field("job_type", &self.job_type)
            .field("priority", &self.priority)
            .field("weight", &self.weight)
            .field("payload", &self.payload)
            .field("expires", &self.expires)
            .field("start_time", &self.start_time)
            .field("backoff_multiplier", &self.backoff_multiplier)
            .field("backoff_randomization", &self.backoff_randomization)
            .field("backoff_initial_interval", &self.backoff_initial_interval)
            .field("current_try", &self.current_try)
            .field("max_retries", &self.max_retries)
            .finish_non_exhaustive()
    }
}

impl Display for JobData {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let expires = OffsetDateTime::from_unix_timestamp(
            self.expires.load(std::sync::atomic::Ordering::Relaxed),
        )
        .map(|t| t.to_string())
        .unwrap_or_default();

        write!(
            f,
            "Job {{ id: {}, job_type: {}, priority: {}, start_time: {}, expires: {}, try: {} }}",
            self.id, self.job_type, self.priority, self.start_time, expires, self.current_try
        )
    }
}

impl JobData {
    /// Checkpoint the task, replacing the payload with the passed in value.
    #[instrument(level = "debug")]
    pub async fn checkpoint_blob(&self, new_payload: Vec<u8>) -> Result<OffsetDateTime> {
        // This counts as a heartbeat, so update the expiration.
        // Update the checkpoint_payload.
        let job_id = self.job_id;
        let worker_id = self.worker_id;
        let now = self.queue.time.now().unix_timestamp();
        let new_expiration = now + (self.heartbeat_increment as i64);

        let (result_tx, result_rx) = tokio::sync::oneshot::channel();
        self.queue
            .db_write_tx
            .send(DbOperation {
                worker_id,
                span: Span::current(),
                operation: DbOperationType::WriteCheckpoint(WriteCheckpointArgs {
                    job_id,
                    new_expiration,
                    payload: new_payload,
                    result_tx,
                }),
            })
            .await
            .map_err(|_| Error::QueueClosed)?;
        let actual_new_expire_time = result_rx.await.map_err(|_| Error::QueueClosed)??;

        let new_time = actual_new_expire_time.ok_or(Error::Expired).and_then(|t| {
            OffsetDateTime::from_unix_timestamp(t)
                .map_err(|_| Error::TimestampOutOfRange("new expiration time"))
        })?;

        self.update_expiration(new_time);

        Ok(new_time)
    }

    /// Checkpoint the task, replacing the payload with the passed in value.
    pub async fn checkpoint_json<T: Serialize>(&self, new_payload: T) -> Result<OffsetDateTime> {
        let blob = serde_json::to_vec(&new_payload).map_err(Error::PayloadError)?;
        self.checkpoint_blob(blob).await
    }

    /// Tell the queue that the task is still running.
    #[instrument(level = "debug")]
    pub async fn heartbeat(&self) -> Result<OffsetDateTime> {
        let new_time = send_heartbeat(
            self.job_id,
            self.worker_id,
            self.heartbeat_increment,
            &self.queue,
        )
        .await?;

        self.update_expiration(new_time);

        Ok(new_time)
    }

    #[instrument(level = "trace")]
    fn update_expiration(&self, new_expiration: OffsetDateTime) {
        self.expires.store(
            new_expiration.unix_timestamp(),
            std::sync::atomic::Ordering::Relaxed,
        );
    }

    pub(crate) async fn is_done(&self) -> bool {
        let done = self.done.lock().await;
        done.is_none()
    }

    /// Return if the task is past the expiration time or not.
    pub fn is_expired(&self) -> bool {
        let now = self.queue.time.now().unix_timestamp();
        let expired = self.expires.load(std::sync::atomic::Ordering::Relaxed);
        now >= expired
    }

    pub fn json_payload<'a, T: Deserialize<'a>>(&'a self) -> Result<T, serde_json::Error> {
        serde_json::from_slice(self.payload.as_slice())
    }

    #[instrument(level = "debug")]
    async fn mark_job_permanently_done<T: Serialize + Send + Debug>(
        &self,
        info: T,
        success: bool,
    ) -> Result<(), Error> {
        let mut done = self.done.lock().await;
        let _chan = done.take().expect("Called complete after job finished");
        drop(done);

        let info = RunInfo {
            success,
            start: self.start_time,
            end: self.queue.time.now(),
            info,
        };

        let this_run_info = serde_json::to_string(&info).map_err(Error::InvalidJobRunInfo)?;

        let job_id = self.job_id;
        let worker_id = self.worker_id;
        let now = self.queue.time.now().unix_timestamp();
        let started_at = self.start_time.unix_timestamp();
        let (result_tx, result_rx) = tokio::sync::oneshot::channel();
        self.queue
            .db_write_tx
            .send(DbOperation {
                worker_id,
                span: Span::current(),
                operation: DbOperationType::CompleteJob(CompleteJobArgs {
                    job_id,
                    run_info: this_run_info,
                    now,
                    started_at,
                    success,
                    result_tx,
                }),
            })
            .await
            .map_err(|_| Error::QueueClosed)?;
        result_rx.await.map_err(|_| Error::QueueClosed)??;

        Ok(())
    }

    /// Mark the job as successful.
    #[instrument]
    pub async fn complete<T: Serialize + Send + Debug>(&self, info: T) -> Result<(), Error> {
        self.mark_job_permanently_done(info, true).await
    }

    /// Mark the job as failed.
    #[instrument]
    pub async fn fail<T: Serialize + Send + Debug>(&self, info: T) -> Result<(), Error> {
        // Remove task from running jobs, update job info, calculate new retry time, and stick the
        // job back into pending.
        // If there is a checkpointed payload, use that. Otherwise use the original payload from the
        // job.

        if self.current_try + 1 > self.max_retries {
            return self.mark_job_permanently_done(info, false).await;
        }

        let mut done = self.done.lock().await;
        let _chan = done.take().expect("Called fail after job finished");
        drop(done);

        // Calculate the next run time, given the backoff.
        let now = self.queue.time.now();
        let run_delta = (self.backoff_initial_interval as f64)
            * (self.backoff_multiplier).powi(self.current_try)
            * (1.0 + rand::random::<f64>() * self.backoff_randomization);
        event!(Level::DEBUG, %run_delta, current_try=%self.current_try);
        let next_time = now.unix_timestamp() + (run_delta as i64);
        let job_id = self.job_id;
        let worker_id = self.worker_id;

        let info = RunInfo {
            success: false,
            start: self.start_time,
            end: now,
            info,
        };

        let this_run_info = serde_json::to_string(&info).map_err(Error::InvalidJobRunInfo)?;

        let (result_tx, result_rx) = tokio::sync::oneshot::channel();
        self.queue
            .db_write_tx
            .send(DbOperation {
                worker_id,
                span: Span::current(),
                operation: DbOperationType::RetryJob(RetryJobArgs {
                    job_id,
                    run_info: this_run_info,
                    next_time,
                    result_tx,
                }),
            })
            .await
            .map_err(|_| Error::QueueClosed)?;
        result_rx.await.map_err(|_| Error::QueueClosed)??;

        // Make sure that the pending job watcher knows about the rescheduled job.
        log_error(
            self.queue
                .pending_jobs_tx
                .send((SmartString::from(&self.job_type), next_time))
                .await,
        );

        Ok(())
    }
}

pub(crate) async fn send_heartbeat(
    job_id: i64,
    worker_id: u64,
    heartbeat_increment: i32,
    queue: &SharedState,
) -> Result<OffsetDateTime> {
    let now = queue.time.now().unix_timestamp();
    let new_expiration = now + heartbeat_increment as i64;

    let (result_tx, result_rx) = tokio::sync::oneshot::channel();
    queue
        .db_write_tx
        .send(DbOperation {
            worker_id,
            span: Span::current(),
            operation: DbOperationType::WriteHeartbeat(WriteHeartbeatArgs {
                job_id,
                new_expiration,
                result_tx,
            }),
        })
        .await
        .map_err(|_| Error::QueueClosed)?;
    let actual_new_expire_time = result_rx.await.map_err(|_| Error::QueueClosed)??;

    let new_time = actual_new_expire_time.ok_or(Error::Expired).and_then(|t| {
        OffsetDateTime::from_unix_timestamp(t)
            .map_err(|_| Error::TimestampOutOfRange("new expiration time"))
    })?;

    Ok(new_time)
}
