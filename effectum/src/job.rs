use std::{
    fmt::{Debug, Display},
    ops::Deref,
    sync::{atomic::AtomicI64, Arc},
};

use serde::{Deserialize, Serialize};
use time::OffsetDateTime;
use tokio::sync::Mutex;
use tracing::{event, instrument, Level, Span};
use uuid::Uuid;

use crate::{
    db_writer::{
        complete::CompleteJobArgs,
        heartbeat::{WriteCheckpointArgs, WriteHeartbeatArgs},
        retry::RetryJobArgs,
        DbOperation, DbOperationType,
    },
    job_status::RunInfo,
    shared_state::SharedState,
    worker::{log_error, WorkerId},
    Error, Result, SmartString,
};

/// Information about a running job.
#[derive(Debug, Clone)]
pub struct RunningJob(pub Arc<RunningJobData>);

impl Deref for RunningJob {
    type Target = RunningJobData;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl Display for RunningJob {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// Information about a running job. This is usually accessed through the [RunningJob] type,
/// which wraps this in an [Arc].
pub struct RunningJobData {
    /// The id of this job.
    pub id: Uuid,
    pub(crate) job_id: i64,
    /// The name given to this job
    pub name: Option<String>,
    /// The ID of the [Worker](crate::worker::Worker) that is running this job.
    pub worker_id: WorkerId,
    /// How many seconds a heartbeat can extend the expiration time.
    pub heartbeat_increment: i32,
    /// The type of the job.
    pub job_type: String,
    /// The job's priority.
    pub priority: i32,
    /// How much this job counts against the worker's concurrency limit.
    pub weight: u16,
    /// The payload of the job. JSON payloads can be parsed using the [RunningJobData::json_payload] function.
    pub payload: Vec<u8>,
    /// The timestamp, in seconds, when this job expires.
    pub expires: AtomicI64,

    /// When the job was started.
    pub start_time: OffsetDateTime,

    pub(crate) backoff_multiplier: f64,
    pub(crate) backoff_randomization: f64,
    pub(crate) backoff_initial_interval: i32,
    /// How many times this job has been tried already. On the first run, this will be 0.
    pub current_try: i32,
    /// The number of times this job can be retried before giving up permanently.
    pub max_retries: i32,

    pub(crate) done: Mutex<Option<tokio::sync::watch::Sender<bool>>>,
    pub(crate) queue: SharedState,
    pub(crate) orig_run_at: OffsetDateTime,
}

impl Debug for RunningJobData {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Job")
            .field("id", &self.id)
            .field("job_id", &self.job_id)
            .field("name", &self.name)
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
            .field("orig_run_at", &self.orig_run_at)
            .finish_non_exhaustive()
    }
}

impl Display for RunningJobData {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let expires = OffsetDateTime::from_unix_timestamp(
            self.expires.load(std::sync::atomic::Ordering::Relaxed),
        )
        .map(|t| t.to_string())
        .unwrap_or_default();

        write!(
            f,
            "Job {{ id: {}, name: {}, job_type: {}, priority: {}, start_time: {}, expires: {}, try: {} }}",
            self.id,
            self.name.as_deref().unwrap_or("_"),
            self.job_type,
            self.priority,
            self.start_time,
            expires,
            self.current_try
        )
    }
}

impl RunningJobData {
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

    /// Deserialize a JSON payload into the requested type.
    pub fn json_payload<'a, T: Deserialize<'a>>(&'a self) -> Result<T> {
        serde_json::from_slice(self.payload.as_slice()).map_err(Error::PayloadError)
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
        let next_time = result_rx.await.map_err(|_| Error::QueueClosed)??;
        if let Some(next_time) = next_time {
            log_error(
                self.queue
                    .pending_jobs_tx
                    .send((
                        SmartString::from(self.job_type.clone()),
                        next_time.unix_timestamp(),
                    ))
                    .await,
            );
        }

        Ok(())
    }

    /// Mark the job as successful.
    #[instrument(skip(self), fields(self = %self))]
    pub async fn complete<T: Serialize + Send + Debug>(&self, info: T) -> Result<(), Error> {
        self.mark_job_permanently_done(info, true).await
    }

    /// Calculate the next run time, given the backoff.
    pub(crate) fn calculate_next_run_time(
        now: &OffsetDateTime,
        current_try: i32,
        backoff_initial_interval: i32,
        backoff_multiplier: f64,
        backoff_randomization: f64,
    ) -> i64 {
        let run_delta = (backoff_initial_interval as f64)
            * (backoff_multiplier).powi(current_try)
            * (1.0 + rand::random::<f64>() * backoff_randomization);
        event!(Level::DEBUG, %run_delta, %current_try);
        now.unix_timestamp() + (run_delta as i64)
    }

    /// Mark the job as failed.
    #[instrument(skip(self), fields(self = %self))]
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

        let now = self.queue.time.now();
        let next_time = Self::calculate_next_run_time(
            &now,
            self.current_try,
            self.backoff_initial_interval,
            self.backoff_multiplier,
            self.backoff_randomization,
        );
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
