use serde::{de::DeserializeOwned, Deserialize, Serialize};
use serde_json::value::RawValue;
use smallvec::SmallVec;
use std::fmt::Debug;
use time::{Duration, OffsetDateTime};
use tracing::{event, Level};
use uuid::Uuid;

use crate::{Error, Queue, Result};

#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "status")]
pub struct RunInfo<T: Send + Debug> {
    pub success: bool,
    #[serde(with = "time::serde::timestamp")]
    pub start: OffsetDateTime,
    #[serde(with = "time::serde::timestamp")]
    pub end: OffsetDateTime,
    pub info: T,
}

#[derive(Debug)]
pub struct JobStatus {
    pub id: Uuid,
    pub job_type: String,
    pub status: String,
    pub priority: i32,
    pub weight: u16,
    pub orig_run_at: OffsetDateTime,
    pub run_at: Option<OffsetDateTime>,
    pub payload: Vec<u8>,
    pub current_try: Option<i32>,
    pub max_retries: i32,
    pub backoff_multiplier: f64,
    pub backoff_randomization: f64,
    pub backoff_initial_interval: Duration,
    pub added_at: OffsetDateTime,
    pub started_at: Option<OffsetDateTime>,
    pub finished_at: Option<OffsetDateTime>,
    pub expires_at: Option<OffsetDateTime>,
    pub run_info: SmallVec<[RunInfo<Box<RawValue>>; 4]>,
}

#[derive(Serialize)]
pub struct NumActiveJobs {
    pub pending: u64,
    pub running: u64,
}

impl Queue {
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
                        .map_err(|e| Error::FromSql(e, "started_at"))?
                        .map(|i| {
                            OffsetDateTime::from_unix_timestamp(i)
                                .map_err(|_| Error::TimestampOutOfRange("started_at"))
                        })
                        .transpose()?;

                    let finished_at = row
                        .get_ref(14)?
                        .as_i64_or_null()
                        .map_err(|e| Error::FromSql(e, "finished_at"))?
                        .map(|i| {
                            OffsetDateTime::from_unix_timestamp(i)
                                .map_err(|_| Error::TimestampOutOfRange("finished_at"))
                        })
                        .transpose()?;

                    let expires_at = row
                        .get_ref(15)?
                        .as_i64_or_null()
                        .map_err(|e| Error::FromSql(e, "expires_at"))?
                        .map(|i| {
                            OffsetDateTime::from_unix_timestamp(i)
                                .map_err(|_| Error::TimestampOutOfRange("expires_at"))
                        })
                        .transpose()?;

                    let run_info_str = row
                        .get_ref(16)?
                        .as_str_or_null()
                        .map_err(|e| Error::FromSql(e, "run_info"))?;
                    let run_info: SmallVec<[RunInfo<Box<RawValue>>; 4]> = match run_info_str {
                        Some(run_info_str) => {
                            serde_json::from_str(run_info_str).map_err(Error::InvalidJobRunInfo)?
                        }
                        None => SmallVec::new(),
                    };

                    let status = JobStatus {
                        id: external_id,
                        job_type: row.get(0).map_err(|e| Error::ColumnType(e, "job_type"))?,
                        status: row.get(1)?,
                        priority: row.get(2)?,
                        weight: row.get(3)?,
                        orig_run_at: OffsetDateTime::from_unix_timestamp(row.get(4)?)
                            .map_err(|_| Error::TimestampOutOfRange("orig_run_at"))?,
                        run_at: row
                            .get_ref(5)?
                            .as_i64_or_null()
                            .map_err(|e| Error::FromSql(e, "run_at"))?
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
