use serde::{de::DeserializeOwned, Deserialize, Serialize};
use serde_json::value::RawValue;
use smallvec::SmallVec;
use time::{Duration, OffsetDateTime};
use uuid::Uuid;

use crate::{Error, Queue, Result};

#[derive(Serialize, Deserialize)]
#[serde(tag = "status")]
pub struct RunInfo<T: Send> {
    pub success: bool,
    #[serde(with = "time::serde::timestamp")]
    pub start: OffsetDateTime,
    #[serde(with = "time::serde::timestamp")]
    pub end: OffsetDateTime,
    pub info: T,
}

pub struct JobStatus {
    pub id: Uuid,
    pub job_type: String,
    pub status: String,
    pub priority: i32,
    pub orig_run_at: OffsetDateTime,
    pub payload: Vec<u8>,
    pub max_retries: i32,
    pub backoff_multiplier: f64,
    pub backoff_randomization: f64,
    pub backoff_initial_interval: Duration,
    pub added_at: OffsetDateTime,
    pub default_timeout: Duration,
    pub heartbeat_increment: Duration,
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
    SELECT job_type, 'active' AS status,
        priority, orig_run_at, payload,
        max_retries, backoff_multiplier, backoff_randomization, backoff_initial_interval,
        added_at, default_timeout, heartbeat_increment, run_info
    FROM active_jobs
    WHERE external_id=$1
    UNION ALL
    SELECT job_type, status,
        priority, orig_run_at, payload,
        max_retries, backoff_multiplier, backoff_randomization, backoff_initial_interval,
        added_at, default_timeout, heartbeat_increment, run_info
    FROM done_jobs
    WHERE external_id=$1

                    "##,
                )?;

                let mut rows = stmt.query_and_then([external_id], |row| {
                    let run_info_str = row
                        .get_ref(12)?
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
                        orig_run_at: OffsetDateTime::from_unix_timestamp(row.get(3)?)
                            .map_err(|_| Error::TimestampOutOfRange("orig_run_at"))?,
                        payload: row.get(4)?,
                        max_retries: row.get(5)?,
                        backoff_multiplier: row.get(6)?,
                        backoff_randomization: row.get(7)?,
                        backoff_initial_interval: Duration::seconds(row.get(8)?),
                        added_at: OffsetDateTime::from_unix_timestamp(row.get(9)?)
                            .map_err(|_| Error::TimestampOutOfRange("added_at"))?,
                        default_timeout: Duration::seconds(row.get(10)?),
                        heartbeat_increment: Duration::seconds(row.get(11)?),
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
                    FROM active_jobs WHERE active_worker_id IS NULL"##,
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
