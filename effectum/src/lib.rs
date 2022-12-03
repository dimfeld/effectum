#![warn(missing_docs)]
//! A SQLite-based task queue library that allows running background jobs without requiring
//! external dependencies.
//!
//! ```no_run
//! # use std::sync::Arc;
//! # use std::path::Path;
//! # use serde::{Deserialize, Serialize};
//! # use serde_json::json;
//! use effectum::{Error, Job, JobState, JobRunner, RunningJob, Queue, Worker};
//!
//! #[derive(Debug)]
//! pub struct JobContext {
//!    // database pool or other things here
//! }
//!
//! #[derive(Serialize, Deserialize)]
//! struct RemindMePayload {
//!   email: String,
//!   message: String,
//! }
//!
//! async fn remind_me_job(job: RunningJob, context: Arc<JobContext>) -> Result<(), Error> {
//!     let payload: RemindMePayload = job.json_payload()?;
//!     // do something with the job
//!     Ok(())
//! }
//!
//! #[tokio::main(flavor = "current_thread")]
//! async fn main() -> Result<(), Error> {
//!   // Create a queue
//!   let queue = Queue::new(Path::new("effectum.db")).await?;
//!
//!   // Define a job type for the queue.
//!   let a_job = JobRunner::builder("remind_me", remind_me_job).build();
//!
//!   let context = Arc::new(JobContext{
//!     // database pool or other things here
//!   });
//!
//!   // Create a worker to run jobs.
//!   let worker = Worker::builder(&queue, context)
//!     .max_concurrency(10)
//!     .jobs([a_job])
//!     .build();
//!
//!   // Submit a job to the queue.
//!   let job_id = Job::builder("remind_me")
//!     .run_at(time::OffsetDateTime::now_utc() + std::time::Duration::from_secs(3600))
//!     .json_payload(&RemindMePayload {
//!         email: "me@example.com".to_string(),
//!         message: "Time to go!".to_string()
//!     })?
//!     .add_to(&queue)
//!     .await?;
//!
//!   // See what's happening with the job.
//!   let status = queue.get_job_status(job_id).await?;
//!   assert_eq!(status.state, JobState::Pending);
//!
//!   // Do other stuff...
//!
//!   Ok(())
//! }
//! ```

mod add_job;
mod error;
mod job_status;
mod migrations;
mod shared_state;
mod worker_list;

mod db_writer;
mod job;
mod job_registry;
mod local_queue;
mod pending_jobs;
mod sqlite_functions;
#[cfg(test)]
mod test_util;
mod worker;

pub use add_job::{Job, JobBuilder, Retries};
pub use error::{Error, Result};
pub use job::{RunningJob, RunningJobData};
pub use job_registry::{JobRegistry, JobRunner, JobRunnerBuilder};
pub use job_status::{JobState, JobStatus, RunInfo};
pub use local_queue::*;
pub use worker::{Worker, WorkerBuilder};

pub(crate) type SmartString = smartstring::SmartString<smartstring::LazyCompact>;

/// How to treat jobs which are already marked as running when the queue starts.
/// This accounts for cases where the process is restarted unexpectedly.
#[non_exhaustive]
pub enum JobRecoveryBehavior {
    /// Mark the job as failed, and schedule the next try to run immediately. (Normal worker
    /// concurrency limits will still apply.)
    FailAndRetryImmediately,
    /// Mark the job as failed, and schedule the next try to run with the normal retry backoff
    /// timing.
    FailAndRetryWithBackoff,
    // /// Don't touch the job. This should only be used if the queue is running in a separate process
    // /// from the workers, which will be possible in a future version.
    // DoNothing,
}
