use std::{borrow::Cow, time::Duration};

use ahash::{HashMap, HashSet};
use tracing::Span;
use uuid::Uuid;

use crate::{
    db_writer::{
        add_job::{AddJobArgs, AddMultipleJobsArgs, AddMultipleJobsResult},
        DbOperation, DbOperationType,
    },
    worker::log_error,
    Error, Queue, Result, SmartString,
};

/// A job to be submitted to the queue.
#[derive(Debug, Clone)]
pub struct Job {
    /// The name of the job, which matches the name used in the [JobRunner](crate::JobRunner) for the job.
    pub job_type: Cow<'static, str>,
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
}

impl Job {
    /// Create a [JobBuilder] for the given `job_type`.
    pub fn builder(job_type: impl Into<Cow<'static, str>>) -> JobBuilder {
        JobBuilder::new(job_type)
    }
}

/// `Retries` controls the exponential backoff behavior when retrying failed jobs.
#[derive(Debug, Clone)]
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
            job_type: Default::default(),
            priority: 0,
            weight: 1,
            run_at: Default::default(),
            payload: Default::default(),
            retries: Default::default(),
            timeout: Duration::from_secs(300),
            heartbeat_increment: Duration::from_secs(120),
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
    pub fn json_payload<T: ?Sized + serde::Serialize>(
        mut self,
        payload: &T,
    ) -> Result<Self, serde_json::Error> {
        self.job.payload = serde_json::to_vec(payload)?;
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

    /// Build the job.
    pub fn build(self) -> Job {
        self.job
    }

    /// Build the job and add it to a [Queue].
    pub async fn add_to(self, queue: &Queue) -> Result<Uuid> {
        queue.add_job(self.job).await
    }
}

impl Queue {
    /// Submit a job to the queue
    pub async fn add_job(&self, job_config: Job) -> Result<Uuid> {
        let job_type = job_config.job_type.clone();
        let now = self.state.time.now();
        let run_time = job_config.run_at.unwrap_or(now);

        let (result_tx, result_rx) = tokio::sync::oneshot::channel();
        self.state
            .db_write_tx
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

        if run_time <= now {
            let workers = self.state.workers.read().await;
            workers.new_job_available(&job_type);
        } else {
            let mut job_type = SmartString::from(job_type);
            job_type.shrink_to_fit();
            log_error(
                self.state
                    .pending_jobs_tx
                    .send((job_type, run_time.unix_timestamp()))
                    .await,
            );
        }

        Ok(ids)
    }

    /// Submit multiple jobs to the queue
    pub async fn add_jobs(&self, jobs: Vec<Job>) -> Result<Vec<Uuid>> {
        let mut ready_job_types: HashSet<String> = HashSet::default();
        let mut pending_job_types: HashMap<String, i64> = HashMap::default();

        let now = self.state.time.now();
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

        let (result_tx, result_rx) = tokio::sync::oneshot::channel();
        self.state
            .db_write_tx
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
            log_error(self.state.pending_jobs_tx.send((job_type, job_time)).await);
        }

        if !ready_job_types.is_empty() {
            let workers = self.state.workers.read().await;
            for job_type in ready_job_types {
                workers.new_job_available(&job_type);
            }
        }

        Ok(ids)
    }
}

#[cfg(test)]
mod tests {
    use crate::{test_util::create_test_queue, Job, JobState};

    #[tokio::test]
    async fn add_job() {
        let queue = create_test_queue().await;

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
        let queue = create_test_queue().await;

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
}
