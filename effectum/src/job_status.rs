use std::{
    fmt::{Debug, Display},
    str::FromStr,
};

use serde::{Deserialize, Serialize};
use serde_json::value::RawValue;
use smallvec::SmallVec;
use time::{Duration, OffsetDateTime};
use uuid::Uuid;

use crate::{Error, Queue, Result};

/// Information about the results of a job run.
#[derive(Debug, Serialize, Deserialize)]
pub struct RunInfo<T: Send + Debug> {
    /// If this run succeeded or not.
    pub success: bool,
    /// When this run started
    #[serde(with = "time::serde::timestamp")]
    pub start: OffsetDateTime,
    /// When this run ended
    #[serde(with = "time::serde::timestamp")]
    pub end: OffsetDateTime,
    /// Information about the run returned from the task runner function.
    pub info: T,
}

/// The current state of a job.
#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub enum JobState {
    /// The job is waiting to run.
    Pending,
    /// The job is currently running.
    Running,
    /// The job finished successfully.
    Succeeded,
    /// The job failed and exceeded its retry limit. It will not be retried.
    Failed,
    /// The job was cancelled by the user.
    Cancelled,
}

impl JobState {
    /// Return a string representation of the state.
    pub fn as_str(&self) -> &'static str {
        match self {
            JobState::Pending => "pending",
            JobState::Running => "running",
            JobState::Succeeded => "succeeded",
            JobState::Failed => "failed",
            JobState::Cancelled => "cancelled",
        }
    }
}

impl Display for JobState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

impl FromStr for JobState {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self> {
        match s {
            "pending" => Ok(JobState::Pending),
            "running" => Ok(JobState::Running),
            "succeeded" => Ok(JobState::Succeeded),
            "failed" => Ok(JobState::Failed),
            "cancelled" => Ok(JobState::Cancelled),
            _ => Err(Error::InvalidJobState(s.to_string())),
        }
    }
}

/// Status information about a job.
#[derive(Debug)]
pub struct JobStatus {
    /// The job's ID.
    pub id: Uuid,
    /// The type of a job
    pub job_type: String,
    /// If the job is waiting, running, or finished
    pub state: JobState,
    /// Higher priority jobs will be run first.
    pub priority: i32,
    /// Higher weight indicates a job counts more against a worker's concurrency.
    pub weight: u16,
    /// The original run_at time, before any retries.
    pub orig_run_at: OffsetDateTime,
    /// The current run_at time, if the job is pending.
    pub run_at: Option<OffsetDateTime>,
    /// The job's payload
    pub payload: Vec<u8>,
    /// The current try count, if the job is running or pending.
    pub current_try: Option<i32>,
    /// The limit on the number of retries.
    pub max_retries: i32,
    /// The multiplier used when calculating the next retry time. See [Retries](crate::Retries).
    pub backoff_multiplier: f64,
    /// The random factor used when calculating the next retry time. See [Retries](crate::Retries).
    pub backoff_randomization: f64,
    /// The initial delay used when calculating the next retry time. See [Retries](crate::Retries).
    pub backoff_initial_interval: Duration,
    /// When the job was added to the queue.
    pub added_at: OffsetDateTime,
    /// When the job's last run started.
    pub started_at: Option<OffsetDateTime>,
    /// When the job finished.
    pub finished_at: Option<OffsetDateTime>,
    /// If currently running, when the job will time out.
    pub expires_at: Option<OffsetDateTime>,
    /// Information about each run of the job.
    pub run_info: SmallVec<[RunInfo<Box<RawValue>>; 4]>,
}

#[derive(Serialize)]
pub struct NumActiveJobs {
    pub pending: u64,
    pub running: u64,
}

impl Queue {
    /// Return information about a job
    pub async fn get_job_status(&self, external_id: Uuid) -> Result<JobStatus> {
        let conn = self.state.read_conn_pool.get().await?;

        let status = conn
            .interact(move |conn| {
                let mut stmt = conn.prepare_cached(
                    r##"
    SELECT jobs.job_type,
        CASE
            WHEN active_worker_id IS NOT NULL THEN 'running'
            WHEN active_jobs.priority IS NOT NULL THEN 'pending'
            ELSE jobs.status
        END AS status,
        jobs.priority, weight, orig_run_at, run_at, payload, current_try,
        max_retries, backoff_multiplier, backoff_randomization, backoff_initial_interval,
        added_at,
        COALESCE(active_jobs.started_at, jobs.started_at) AS started_at,
        finished_at, expires_at, run_info
    FROM jobs
    LEFT JOIN active_jobs USING(job_id)
    WHERE external_id=?1
    "##,
                )?;

                let mut rows = stmt.query_and_then([external_id], |row| {
                    let started_at = row
                        .get_ref(13)?
                        .as_i64_or_null()
                        .map_err(|e| Error::ColumnType(e.into(), "started_at"))?
                        .map(|i| {
                            OffsetDateTime::from_unix_timestamp(i)
                                .map_err(|_| Error::TimestampOutOfRange("started_at"))
                        })
                        .transpose()?;

                    let finished_at = row
                        .get_ref(14)?
                        .as_i64_or_null()
                        .map_err(|e| Error::ColumnType(e.into(), "finished_at"))?
                        .map(|i| {
                            OffsetDateTime::from_unix_timestamp(i)
                                .map_err(|_| Error::TimestampOutOfRange("finished_at"))
                        })
                        .transpose()?;

                    let expires_at = row
                        .get_ref(15)?
                        .as_i64_or_null()
                        .map_err(|e| Error::ColumnType(e.into(), "expires_at"))?
                        .map(|i| {
                            OffsetDateTime::from_unix_timestamp(i)
                                .map_err(|_| Error::TimestampOutOfRange("expires_at"))
                        })
                        .transpose()?;

                    let run_info_str = row
                        .get_ref(16)?
                        .as_str_or_null()
                        .map_err(|e| Error::ColumnType(e.into(), "run_info"))?;
                    let run_info: SmallVec<[RunInfo<Box<RawValue>>; 4]> = match run_info_str {
                        Some(run_info_str) => {
                            serde_json::from_str(run_info_str).map_err(Error::InvalidJobRunInfo)?
                        }
                        None => SmallVec::new(),
                    };

                    let status = JobStatus {
                        id: external_id,
                        job_type: row.get(0).map_err(|e| Error::ColumnType(e, "job_type"))?,
                        state: row
                            .get_ref(1)?
                            .as_str()
                            .map_err(|e| Error::ColumnType(e.into(), "state"))?
                            .parse()?,
                        priority: row.get(2)?,
                        weight: row.get(3)?,
                        orig_run_at: OffsetDateTime::from_unix_timestamp(row.get(4)?)
                            .map_err(|_| Error::TimestampOutOfRange("orig_run_at"))?,
                        run_at: row
                            .get_ref(5)?
                            .as_i64_or_null()
                            .map_err(|e| Error::ColumnType(e.into(), "run_at"))?
                            .map(OffsetDateTime::from_unix_timestamp)
                            .transpose()
                            .map_err(|_| Error::TimestampOutOfRange("run_at"))?,
                        payload: row.get(6)?,
                        current_try: row.get(7)?,
                        max_retries: row.get(8)?,
                        backoff_multiplier: row.get(9)?,
                        backoff_randomization: row.get(10)?,
                        backoff_initial_interval: Duration::seconds(row.get(11)?),
                        added_at: OffsetDateTime::from_unix_timestamp(row.get(12)?)
                            .map_err(|_| Error::TimestampOutOfRange("added_at"))?,
                        started_at,
                        finished_at,
                        expires_at,
                        run_info,
                    };

                    Ok::<_, Error>(status)
                })?;

                let status = rows.next().ok_or(Error::NotFound)??;

                Ok::<_, Error>(status)
            })
            .await??;

        Ok(status)
    }

    /// Return counts about the number of jobs running and waiting to run.
    pub async fn num_active_jobs(&self) -> Result<NumActiveJobs> {
        let conn = self.state.read_conn_pool.get().await?;
        let (total, running): (i64, i64) = conn
            .interact(move |conn| {
                let mut stmt = conn.prepare_cached(
                    r##"SELECT COUNT(*) as total, COUNT(active_worker_id) AS running
                    FROM active_jobs"##,
                )?;
                stmt.query_row([], |row| Ok((row.get(0)?, row.get(1)?)))
            })
            .await??;

        Ok(NumActiveJobs {
            pending: (total - running) as u64,
            running: running as u64,
        })
    }
}
