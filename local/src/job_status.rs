use serde::{Deserialize, Serialize};
use time::{Duration, OffsetDateTime};
use uuid::Uuid;

use crate::{Error, Queue, Result};

#[derive(Serialize, Deserialize)]
#[serde(tag = "status")]
pub struct RunInfo {
    sucess: bool,
    #[serde(with = "time::serde::timestamp")]
    start: OffsetDateTime,
    #[serde(with = "time::serde::timestamp")]
    end: OffsetDateTime,
    info: serde_json::Value,
}

pub struct JobStatus {
    pub id: Uuid,
    pub job_type: String,
    pub status: String,
    pub priority: i64,
    pub orig_run_at_time: OffsetDateTime,
    pub payload: Vec<u8>,
    pub max_retries: i64,
    pub backoff_multiplier: f64,
    pub backoff_randomization: f64,
    pub backoff_initial_interval: Duration,
    pub added_at: OffsetDateTime,
    pub default_timeout: Duration,
    pub heartbeat_increment: Duration,
    pub run_info: Vec<RunInfo>,
}

impl Queue {
    pub async fn get_job_status(&self, external_id: Uuid) -> Result<JobStatus> {
        let conn = self.state.read_conn_pool.get().await?;

        let status = conn
            .interact(move |conn| {
                let mut stmt = conn.prepare_cached(
                    r##"
    SELECT job_type, 'active' AS status,
        priority, orig_run_at_time, payload,
        max_retries, backoff_multiplier, backoff_randomization, backoff_initial_interval,
        added_at, default_timeout, heartbeat_increment, run_info
    FROM active_jobs
    WHERE external_id=$1
    UNION ALL
    SELECT job_type, status,
        priority, orig_run_at_time, payload,
        max_retries, backoff_multiplier, backoff_randomization, backoff_initial_interval,
        added_at, default_timeout, heartbeat_increment, run_info
    FROM done_jobs
    WHERE external_id=$1

                    "##,
                )?;

                let mut rows = stmt.query_and_then([external_id], |row| {
                    let blob = row.get_ref(12)?.as_blob_or_null()?;
                    let run_info: Vec<RunInfo> = match blob {
                        Some(blob) => {
                            serde_json::from_slice(blob).map_err(Error::InvalidJobRunInfo)?
                        }
                        None => Vec::new(),
                    };

                    let status = JobStatus {
                        id: external_id,
                        job_type: row.get(0)?,
                        status: row.get(1)?,
                        priority: row.get(2)?,
                        orig_run_at_time: OffsetDateTime::from_unix_timestamp(row.get(3)?)
                            .map_err(|_| Error::TimestampOutOfRange("orig_run_at_time"))?,
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

                let status = rows.next().ok_or(Error::NotFound)?;

                Ok::<_, Error>(status)
            })
            .await???;

        Ok(status)
    }

    pub async fn num_pending_jobs(&self) -> Result<u64> {
        let conn = self.state.read_conn_pool.get().await?;
        let count: u64 = conn
            .interact(move |conn| {
                let mut stmt = conn.prepare_cached("SELECT COUNT(*) FROM pending")?;
                stmt.query_row([], |row| row.get(0))
            })
            .await??;

        Ok(count)
    }

    pub async fn num_running_jobs(&self) -> Result<u64> {
        let conn = self.state.read_conn_pool.get().await?;
        let count: u64 = conn
            .interact(move |conn| {
                let mut stmt = conn.prepare_cached("SELECT COUNT(*) FROM running")?;
                stmt.query_row([], |row| row.get(0))
            })
            .await??;

        Ok(count)
    }
}
