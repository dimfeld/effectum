use std::{borrow::Cow, time::Duration};

use ahash::{HashMap, HashSet};
use serde::{Deserialize, Serialize};
use time::OffsetDateTime;
use tracing::{instrument, Span};
use uuid::Uuid;

use crate::{
    db_writer::{
        add_job::{AddJobArgs, AddMultipleJobsArgs, AddMultipleJobsResult},
        cancel_job::CancelJobArgs,
        update_job::UpdateJobArgs,
        DbOperation, DbOperationType,
    },
    shared_state::SharedState,
    worker::log_error,
    Error, Queue, Result, SmartString,
};

/// A job to be submitted to the queue.
/// Jobs are uniquely identified by their `id`, so adding a job with the same ID twice will fail.
/// If you want to clone the same Job object and submit it multiple times, use [JobBuilder::clone_as_new]
/// to generate a new ID with each clone.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Job {
    /// A unique identifier for the job.
    pub id: Uuid,
    /// The name of the job, which matches the name used in the [JobRunner](crate::JobRunner) for the job.
    pub job_type: Cow<'static, str>,
    /// A description for this job which can be passed to [Queue::get_jobs_by_name]. This value does not
    /// have to be unique among all jobs.
    pub name: Option<String>,
    /// Jobs with higher `priority` will be executed first.
    pub priority: i32,
    /// Jobs that are expected to take more processing resources can be given a higher weight
    /// to account for this. A worker counts the job's weight (1, by default) against its
    /// maximum concurrency when deciding how many jobs it can execute. For example,
    /// a worker with a `max_concurrency` of 10 would run three jobs at a time if each
    /// had a weight of three.
    ///
    /// For example, a video transcoding task might alter the weight depending on the resolution of
    /// the video or the processing requirements of the codec for each run.
    pub weight: u32,
    /// When to run the job. `None` means to run it right away.
    pub run_at: Option<time::OffsetDateTime>,
    /// The payload to pass to the job when it runs.
    pub payload: Vec<u8>,
    /// Retry behavior when the job fails.
    pub retries: Retries,
    /// How long to allow the job to run before it is considered failed.
    pub timeout: Duration,
    /// How much extra time a heartbeat will add to the expiration time.
    pub heartbeat_increment: Duration,
    pub(crate) from_recurring: Option<i64>,
}

impl Job {
    /// Create a [JobBuilder] for the given `job_type`.
    pub fn builder(job_type: impl Into<Cow<'static, str>>) -> JobBuilder {
        JobBuilder::new(job_type)
    }

    /// Clone the [Job] with a new `id`.
    pub fn clone_as_new(&self) -> Self {
        let mut job = self.clone();
        job.id = Uuid::now_v7();
        job
    }
}

/// `Retries` controls the exponential backoff behavior when retrying failed jobs.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Retries {
    /// How many times to retry a job before it is considered to have failed permanently.
    pub max_retries: u32,
    /// How long to wait before retrying the first time. Defaults to 20 seconds.
    pub backoff_initial_interval: Duration,
    /// For each retry after the first, the backoff time will be multiplied by `backoff_multiplier ^ current_retry`.
    /// Defaults to `2`, which will double the backoff time for each retry.
    pub backoff_multiplier: f32,
    /// To avoid pathological cases where multiple jobs are retrying simultaneously, a
    /// random percentage will be added to the backoff time when a job is rescheduled.
    /// `backoff_randomization` is the maximum percentage to add.
    pub backoff_randomization: f32,
}

impl Default for Retries {
    fn default() -> Self {
        Self {
            max_retries: 3,
            backoff_multiplier: 2f32,
            backoff_randomization: 0.2,
            backoff_initial_interval: Duration::from_secs(20),
        }
    }
}

impl Default for Job {
    fn default() -> Self {
        Self {
            id: Uuid::now_v7(),
            job_type: Default::default(),
            name: None,
            priority: 0,
            weight: 1,
            run_at: Default::default(),
            payload: Default::default(),
            retries: Default::default(),
            timeout: Duration::from_secs(300),
            heartbeat_increment: Duration::from_secs(120),
            from_recurring: Default::default(),
        }
    }
}

/// A builder for a job to submit to the queue.
pub struct JobBuilder {
    job: Job,
}

impl JobBuilder {
    /// Create a new job builder.
    pub fn new(job_type: impl Into<Cow<'static, str>>) -> Self {
        Self {
            job: Job {
                job_type: job_type.into(),
                ..Default::default()
            },
        }
    }

    /// Set the name of this job. This name is purely informational, and does not have to be unique. Jobs can be fetched by
    /// their name using [Queue::get_jobs_by_name].
    pub fn name(mut self, name: impl ToString) -> Self {
        self.job.name = Some(name.to_string());
        self
    }

    /// Set the name of this job. This name is purely informational, and does not have to be unique. Jobs can be fetched by
    /// their name using [Queue::get_jobs_by_name].
    pub fn name_opt(mut self, name: Option<String>) -> Self {
        self.job.name = name.map(|n| n.to_string());
        self
    }

    /// Set the priority of the job.
    pub fn priority(mut self, priority: i32) -> Self {
        self.job.priority = priority;
        self
    }

    /// Set the weight of the job.
    pub fn weight(mut self, weight: u32) -> Self {
        assert!(weight >= 1, "weight must be at least 1");
        self.job.weight = weight;
        self
    }

    /// Set the time at which the job should run.
    pub fn run_at(mut self, run_at: time::OffsetDateTime) -> Self {
        self.job.run_at = Some(run_at);
        self
    }

    /// Set the payload of the job.
    pub fn payload(mut self, payload: Vec<u8>) -> Self {
        self.job.payload = payload;
        self
    }

    /// Serialize the payload of the job using `serde_json`.
    pub fn json_payload<T: ?Sized + serde::Serialize>(mut self, payload: &T) -> Result<Self> {
        self.job.payload = serde_json::to_vec(payload).map_err(Error::PayloadError)?;
        Ok(self)
    }

    /// Configure all of the retry behavior of the job.
    pub fn retries(mut self, retries: Retries) -> Self {
        self.job.retries = retries;
        self
    }

    /// Set the maximum number of retries for the job.
    pub fn max_retries(mut self, max_retries: u32) -> Self {
        self.job.retries.max_retries = max_retries;
        self
    }

    /// Set the initial backoff interval for the job. See [Retries::backoff_initial_interval] for more details
    pub fn backoff_initial_interval(mut self, backoff_initial_interval: Duration) -> Self {
        self.job.retries.backoff_initial_interval = backoff_initial_interval;
        self
    }

    /// Set the backoff multiplier for the job. See [Retries::backoff_multiplier] for more details
    pub fn backoff_multiplier(mut self, backoff_multiplier: f32) -> Self {
        self.job.retries.backoff_multiplier = backoff_multiplier;
        self
    }

    /// Set the backoff randomization factor for the job. See [Retries::backoff_randomization] for more details
    pub fn backoff_randomization(mut self, backoff_randomization: f32) -> Self {
        self.job.retries.backoff_randomization = backoff_randomization;
        self
    }

    /// Set the timeout of the job.
    pub fn timeout(mut self, timeout: Duration) -> Self {
        self.job.timeout = timeout;
        self
    }

    /// Set the heartbeat increment of the job.
    pub fn heartbeat_increment(mut self, heartbeat_increment: Duration) -> Self {
        self.job.heartbeat_increment = heartbeat_increment;
        self
    }

    pub(crate) fn from_recurring(mut self, recurring_id: i64) -> Self {
        self.job.from_recurring = Some(recurring_id);
        self
    }

    /// Build the job.
    pub fn build(self) -> Job {
        self.job
    }

    /// Build the job and add it to a [Queue].
    pub async fn add_to(self, queue: &Queue) -> Result<Uuid> {
        queue.add_job(self.job).await
    }
}

/// Specified fields of a job to be updated, using the [Queue::update_job] method.
/// All of these fields except the job ID are optional, so the update can set
/// only the desired fields.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JobUpdate {
    /// The ID of the job to update
    pub id: Uuid,
    /// A new time for the job to run
    pub run_at: Option<time::OffsetDateTime>,
    /// A new payload for the job
    pub payload: Option<Vec<u8>>,
    /// When changing the payload on a job that has failed and has a checkpointed payload,
    /// set this to `true` to also update the checkpointed payload with the new one.
    /// Otherwise the original checkpointed payload remains in place and the new payload
    /// will not apply.
    pub update_checkpointed_payload: bool,
    /// A new weight for the job
    pub weight: Option<u32>,
    /// A new priority for the job
    pub priority: Option<i32>,
}

impl JobUpdate {
    /// Create a [JobUpdateBuilder]
    pub fn builder(job_id: Uuid) -> JobUpdateBuilder {
        JobUpdateBuilder::new(job_id)
    }
}

/// A builder for a [JobUpdate]
pub struct JobUpdateBuilder {
    update: JobUpdate,
}

impl JobUpdateBuilder {
    /// Create a new builder to update the given job
    pub fn new(id: Uuid) -> Self {
        Self {
            update: JobUpdate {
                id,
                run_at: None,
                payload: None,
                update_checkpointed_payload: false,
                weight: None,
                priority: None,
            },
        }
    }

    /// Alter the job's run_at time
    pub fn run_at(mut self, run_at: time::OffsetDateTime) -> Self {
        self.update.run_at = Some(run_at);
        self
    }

    /// Alter the job's payload
    pub fn payload(mut self, payload: Vec<u8>) -> Self {
        self.update.payload = Some(payload);
        self
    }

    /// Alter the job's payload, encoding the argument as JSON.
    pub fn json_payload<T: ?Sized + serde::Serialize>(mut self, payload: &T) -> Result<Self> {
        self.update.payload = Some(serde_json::to_vec(&payload).map_err(Error::PayloadError)?);
        Ok(self)
    }

    /// Configure whether or not the updated payload should also update the checkpointed payload, if one exists.
    pub fn update_checkpointed_payload(mut self, update_checkpointed_payload: bool) -> Self {
        self.update.update_checkpointed_payload = update_checkpointed_payload;
        self
    }

    /// Alter the job's weight
    pub fn weight(mut self, weight: u32) -> Self {
        self.update.weight = Some(weight);
        self
    }

    /// Alter the job's priority
    pub fn priority(mut self, priority: i32) -> Self {
        self.update.priority = Some(priority);
        self
    }

    /// Create the [JobUpdate]
    pub fn build(self) -> JobUpdate {
        self.update
    }
}

impl SharedState {
    pub(crate) async fn notify_for_job_type(
        &self,
        now: OffsetDateTime,
        run_time: OffsetDateTime,
        job_type: &str,
    ) {
        if run_time <= now {
            let workers = self.workers.read().await;
            workers.new_job_available(job_type);
        } else {
            let mut job_type = SmartString::from(job_type);
            job_type.shrink_to_fit();
            log_error(
                self.pending_jobs_tx
                    .send((job_type, run_time.unix_timestamp()))
                    .await,
            );
        }
    }

    /// Submit a job to the queue
    pub(crate) async fn add_job(&self, job_config: Job) -> Result<Uuid> {
        let job_type = job_config.job_type.clone();
        let now = self.time.now();
        let run_time = job_config.run_at.unwrap_or(now);

        let (result_tx, result_rx) = tokio::sync::oneshot::channel();

        // Ensure that non-awaited promises don't do anything. Without this,
        // we send into the channel to create the job, but don't do the rest,
        // which causes the job to be added but does not cause the notification.
        std::future::ready(()).await;

        self.db_write_tx
            .send(DbOperation {
                worker_id: 0,
                span: Span::current(),
                operation: DbOperationType::AddJob(AddJobArgs {
                    job: job_config,
                    now,
                    result_tx,
                }),
            })
            .await
            .map_err(|_| Error::QueueClosed)?;
        let ids = result_rx.await.map_err(|_| Error::QueueClosed)??;

        self.notify_for_job_type(now, run_time, &job_type).await;

        Ok(ids)
    }

    /// Submit multiple jobs to the queue
    #[instrument(skip(self))]
    pub async fn add_jobs(&self, jobs: Vec<Job>) -> Result<Vec<Uuid>> {
        let mut ready_job_types: HashSet<String> = HashSet::default();
        let mut pending_job_types: HashMap<String, i64> = HashMap::default();

        let now = self.time.now();
        let now_ts = now.unix_timestamp();
        for job_config in &jobs {
            let run_time = job_config
                .run_at
                .map(|t| t.unix_timestamp())
                .unwrap_or(now_ts);
            if run_time <= now_ts {
                ready_job_types.insert(job_config.job_type.to_string());
            } else {
                pending_job_types
                    .entry(job_config.job_type.to_string())
                    .and_modify(|e| *e = std::cmp::min(*e, run_time))
                    .or_insert(run_time);
            }
        }

        // Ensure that non-awaited promises don't do anything. Without this,
        // we send into the channel to create the job, but don't do the rest,
        // which causes the job to be added but does not cause the notification.
        std::future::ready(()).await;

        let (result_tx, result_rx) = tokio::sync::oneshot::channel();
        self.db_write_tx
            .send(DbOperation {
                worker_id: 0,
                span: Span::current(),
                operation: DbOperationType::AddMultipleJobs(AddMultipleJobsArgs {
                    jobs,
                    now,
                    result_tx,
                }),
            })
            .await
            .map_err(|_| Error::QueueClosed)?;
        let AddMultipleJobsResult { ids } = result_rx.await.map_err(|_| Error::QueueClosed)??;

        for (job_type, job_time) in pending_job_types {
            let mut job_type = SmartString::from(job_type);
            job_type.shrink_to_fit();
            log_error(self.pending_jobs_tx.send((job_type, job_time)).await);
        }

        if !ready_job_types.is_empty() {
            let workers = self.workers.read().await;
            for job_type in ready_job_types {
                workers.new_job_available(&job_type);
            }
        }

        Ok(ids)
    }
}

impl Queue {
    /// Submit a job to the queue
    pub async fn add_job(&self, job: Job) -> Result<Uuid> {
        self.state.add_job(job).await
    }

    /// Submit multiple jobs to the queue
    pub async fn add_jobs(&self, jobs: Vec<Job>) -> Result<Vec<Uuid>> {
        self.state.add_jobs(jobs).await
    }

    /// Update some aspects of a job. Jobs can not be updated while running or after they have
    /// finished.
    #[instrument(skip(self))]
    pub async fn update_job(&self, job: JobUpdate) -> Result<()> {
        let (result_tx, result_rx) = tokio::sync::oneshot::channel();

        let new_run_at = job.run_at;

        self.state
            .db_write_tx
            .send(DbOperation {
                worker_id: 0,
                span: Span::current(),
                operation: DbOperationType::UpdateJob(UpdateJobArgs { job, result_tx }),
            })
            .await
            .map_err(|_| Error::QueueClosed)?;
        let job_type = result_rx.await.map_err(|_| Error::QueueClosed)??;

        if let Some(new_run_at) = new_run_at {
            let now = self.state.time.now();
            self.state
                .notify_for_job_type(now, new_run_at, &job_type)
                .await;
        }

        Ok(())
    }

    /// Cancel a job. Jobs can not be cancelled while are running or after they have
    /// finished.
    #[instrument(skip(self))]
    pub async fn cancel_job(&self, job_id: Uuid) -> Result<()> {
        let (result_tx, result_rx) = tokio::sync::oneshot::channel();
        let now = self.state.time.now();
        self.state
            .db_write_tx
            .send(DbOperation {
                worker_id: 0,
                span: Span::current(),
                operation: DbOperationType::CancelJob(CancelJobArgs {
                    id: job_id,
                    now,
                    result_tx,
                }),
            })
            .await
            .map_err(|_| Error::QueueClosed)?;
        result_rx.await.map_err(|_| Error::QueueClosed)??;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::{sync::Arc, time::Duration};

    use temp_dir::TempDir;
    use uuid::Uuid;

    use crate::{
        test_util::{
            create_test_queue, wait_for_job, wait_for_job_fn, wait_for_job_status, TestContext,
            TestEnvironment,
        },
        Error, Job, JobRunner, JobState, JobUpdate, RunningJob,
    };

    #[tokio::test]
    async fn add_job() {
        let dir = TempDir::new().unwrap();
        let queue = create_test_queue(dir).await;

        let job = Job::builder("a_job").priority(1).build();

        let external_id = queue.add_job(job).await.unwrap();
        let after_start_time = queue.state.time.now();
        let status = queue.get_job_status(external_id).await.unwrap();

        assert_eq!(status.state, JobState::Pending);
        assert_eq!(status.id, external_id);
        assert_eq!(status.priority, 1);
        assert!(status.orig_run_at < after_start_time);
    }

    #[tokio::test]
    async fn add_job_at_time() {
        let dir = TempDir::new().unwrap();
        let queue = create_test_queue(dir).await;

        let job_time = (queue.state.time.now() + time::Duration::minutes(10))
            .replace_nanosecond(0)
            .unwrap();

        let job = Job::builder("a_job").run_at(job_time).build();

        let external_id = queue.add_job(job).await.unwrap();
        let status = queue.get_job_status(external_id).await.unwrap();

        assert_eq!(status.orig_run_at, job_time);
        assert_eq!(status.state, JobState::Pending);
        assert_eq!(status.id, external_id);
        assert_eq!(status.priority, 0);
    }

    #[tokio::test]
    async fn full_update_job() {
        let test = TestEnvironment::new().await;
        let _worker = test
            .worker()
            .max_concurrency(10)
            .build()
            .await
            .expect("Failed to build worker");
        let now = test.time.now();

        let job = Job::builder("counter")
            .run_at(now + Duration::from_secs(600))
            .json_payload(&2)
            .expect("payload")
            .priority(2)
            .weight(2)
            .add_to(&test.queue)
            .await
            .expect("Adding job to queue");

        test.queue
            .update_job(
                JobUpdate::builder(job)
                    .priority(10)
                    .weight(3)
                    .run_at(now)
                    .json_payload(&3)
                    .expect("update payload")
                    .build(),
            )
            .await
            .expect("Updating job");

        let status = wait_for_job("job to succeed", &test.queue, job).await;

        assert_eq!(status.priority, 10);
        assert_eq!(status.weight, 3);
        assert_eq!(
            test.context
                .counter
                .load(std::sync::atomic::Ordering::Relaxed),
            3
        );
    }

    #[tokio::test]
    async fn simple_update_job() {
        let test = TestEnvironment::new().await;
        let _worker = test
            .worker()
            .max_concurrency(10)
            .build()
            .await
            .expect("Failed to build worker");
        let now = test.time.now();

        let job = Job::builder("counter")
            .run_at(now + Duration::from_secs(600))
            .json_payload(&2)
            .expect("payload")
            .priority(2)
            .weight(2)
            .add_to(&test.queue)
            .await
            .expect("Adding job to queue");

        test.queue
            .update_job(JobUpdate::builder(job).run_at(now).build())
            .await
            .expect("Updating job");

        let status = wait_for_job("job to succeed", &test.queue, job).await;

        assert_eq!(status.priority, 2);
        assert_eq!(status.weight, 2);
        assert_eq!(
            test.context
                .counter
                .load(std::sync::atomic::Ordering::Relaxed),
            2
        );
    }

    async fn test_update_payload_on_checkpoint(should_alter: bool) {
        let mut test = TestEnvironment::new().await;

        let job_def = JobRunner::builder(
            "test_job",
            move |job: RunningJob, _context: Arc<TestContext>| async move {
                if job.current_try == 0 {
                    job.checkpoint_json("checkpointing").await.unwrap();
                    Err("fail first")
                } else {
                    let payload: String = job.json_payload().unwrap();
                    if should_alter {
                        assert_eq!(payload, "altered", "payload should be altered");
                    } else {
                        assert_eq!(payload, "checkpointing", "payload should not be altered");
                    }
                    Ok(())
                }
            },
        )
        .build();
        test.registry.add(&job_def);
        let _worker = test.worker().build().await.expect("Failed to build worker");
        let now = test.time.now();

        let job = Job::builder("test_job")
            .retries(crate::Retries {
                backoff_initial_interval: Duration::from_secs(60),
                ..Default::default()
            })
            .add_to(&test.queue)
            .await
            .expect("Adding job to queue");

        wait_for_job_fn("job to fail once", &test.queue, job, |status| {
            status.current_try.unwrap_or(0) == 1
        })
        .await;

        test.queue
            .update_job(
                JobUpdate::builder(job)
                    .run_at(now)
                    .json_payload("altered")
                    .expect("setting payload")
                    .update_checkpointed_payload(should_alter)
                    .build(),
            )
            .await
            .unwrap();

        wait_for_job("job to succeed", &test.queue, job).await;
    }

    #[tokio::test]
    async fn update_checkpointed_job_payload() {
        test_update_payload_on_checkpoint(true).await;
    }

    #[tokio::test]
    async fn do_not_update_checkpointed_job_payload() {
        test_update_payload_on_checkpoint(false).await;
    }

    #[tokio::test]
    async fn update_nonexistent_job() {
        let test = TestEnvironment::new().await;
        let result = test
            .queue
            .update_job(
                JobUpdate::builder(Uuid::now_v7().into())
                    .run_at(test.time.now())
                    .build(),
            )
            .await;

        assert!(matches!(result, Err(Error::NotFound)));
    }

    #[tokio::test]
    async fn update_running_job() {
        let test = TestEnvironment::new().await;
        let _worker = test.worker().build().await.expect("Failed to build worker");

        let job = Job::builder("sleep")
            .json_payload(&600000)
            .expect("payload")
            .add_to(&test.queue)
            .await
            .expect("adding job");

        wait_for_job_status("job to start", &test.queue, job, JobState::Running).await;

        let result = test
            .queue
            .update_job(
                JobUpdate::builder(job)
                    .json_payload(&1)
                    .expect("payload")
                    .build(),
            )
            .await;

        assert!(matches!(result, Err(Error::JobRunning)));
    }

    #[tokio::test]
    async fn update_finished_job() {
        let test = TestEnvironment::new().await;
        let _worker = test.worker().build().await.expect("Failed to build worker");

        let job = Job::builder("counter")
            .add_to(&test.queue)
            .await
            .expect("adding job");

        wait_for_job("job to run", &test.queue, job).await;

        let result = test
            .queue
            .update_job(
                JobUpdate::builder(job)
                    .json_payload(&1)
                    .expect("payload")
                    .build(),
            )
            .await;

        assert!(matches!(result, Err(Error::JobFinished)));
    }

    #[tokio::test]
    async fn cancel_job() {
        let test = TestEnvironment::new().await;
        let now = test.time.now();
        let _worker = test.worker().build().await.expect("Failed to build worker");
        let job = Job::builder("counter")
            .run_at(now + Duration::from_secs(600))
            .add_to(&test.queue)
            .await
            .expect("adding job");

        wait_for_job_status("job is pending", &test.queue, job, JobState::Pending).await;

        test.queue.cancel_job(job).await.expect("cancelling job");

        wait_for_job_status("job to be cancelled", &test.queue, job, JobState::Cancelled).await;
        tokio::time::pause();
        tokio::time::advance(Duration::from_secs(1000)).await;

        wait_for_job_status(
            "job is still cancelled",
            &test.queue,
            job,
            JobState::Cancelled,
        )
        .await;
        assert_eq!(
            test.context
                .counter
                .load(std::sync::atomic::Ordering::Relaxed),
            0
        );

        // Shouldn't be able to cancel a job that's already cancelled
        let double_cancel_result = test.queue.cancel_job(job).await;
        assert!(matches!(double_cancel_result, Err(Error::JobFinished)));
    }

    #[tokio::test]
    async fn cancel_nonexistent_job() {
        let test = TestEnvironment::new().await;
        let result = test.queue.cancel_job(Uuid::now_v7().into()).await;

        assert!(matches!(result, Err(Error::NotFound)));
    }

    #[tokio::test]
    async fn cancel_running_job() {
        let test = TestEnvironment::new().await;
        let _worker = test.worker().build().await.expect("Failed to build worker");

        let job = Job::builder("sleep")
            .json_payload(&600000)
            .expect("payload")
            .add_to(&test.queue)
            .await
            .expect("adding job");

        wait_for_job_status("job to start", &test.queue, job, JobState::Running).await;

        let result = test.queue.cancel_job(job).await;

        assert!(matches!(result, Err(Error::JobRunning)));
    }

    #[tokio::test]
    async fn cancel_finished_job() {
        let test = TestEnvironment::new().await;
        let _worker = test.worker().build().await.expect("Failed to build worker");

        let job = Job::builder("counter")
            .add_to(&test.queue)
            .await
            .expect("adding job");

        wait_for_job("job to run", &test.queue, job).await;

        let result = test.queue.cancel_job(job).await;

        assert!(matches!(result, Err(Error::JobFinished)));
    }
}
