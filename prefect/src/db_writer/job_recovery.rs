use rusqlite::Connection;
use tracing::instrument;

use super::{complete::do_complete_job, retry::do_retry_job};
use crate::{
    error::Result, job::RunningJobData, shared_state::SharedState, Error, JobRecoveryBehavior,
    RunInfo,
};

/// Handle jobs that had been running when the process quit last time.
#[instrument(skip_all)]
pub(crate) fn handle_active_jobs_at_startup(
    queue: &SharedState,
    behavior: JobRecoveryBehavior,
    conn: &mut Connection,
) -> Result<()> {
    let now = queue.time.now();
    let now_timestamp = now.unix_timestamp();
    let tx = conn.transaction()?;
    {
        let mut stmt = tx
            .prepare_cached(
                r##"SELECT job_id, active_worker_id, current_try, max_retries,
                backoff_multiplier, backoff_randomization, backoff_initial_interval,
                active_jobs.started_at
         FROM active_jobs
         JOIN jobs USING(job_id)
         WHERE active_worker_id IS NOT NULL"##,
            )
            .unwrap();

        let rows = stmt.query_map([], |row| {
            let job_id: i64 = row.get(0)?;
            let active_worker_id: u64 = row.get(1)?;
            let current_try: i32 = row.get(2)?;
            let max_retries: i32 = row.get(3)?;
            let backoff_multiplier: f64 = row.get(4)?;
            let backoff_randomization: f64 = row.get(5)?;
            let backoff_initial_interval: i32 = row.get(6)?;
            let started_at: i64 = row.get(7)?;

            Ok((
                job_id,
                active_worker_id,
                current_try,
                max_retries,
                backoff_multiplier,
                backoff_randomization,
                backoff_initial_interval,
                started_at,
            ))
        })?;

        for row in rows {
            let (
                job_id,
                active_worker_id,
                current_try,
                max_retries,
                backoff_multiplier,
                backoff_randomization,
                backoff_initial_interval,
                started_at,
            ) = row?;

            let run_info = serde_json::to_string(&RunInfo {
                success: false,
                start: time::OffsetDateTime::from_unix_timestamp(started_at)
                    .map_err(|_| Error::TimestampOutOfRange("started_at"))?,
                end: now,
                info: "Job failed due to unexpected process restart",
            })
            .unwrap();

            if current_try + 1 > max_retries {
                do_complete_job(
                    &tx,
                    job_id,
                    active_worker_id,
                    now_timestamp,
                    started_at,
                    false,
                    run_info,
                )?;
            } else {
                let next_time = match behavior {
                    JobRecoveryBehavior::FailAndRetryImmediately => now.unix_timestamp(),
                    JobRecoveryBehavior::FailAndRetryWithBackoff => {
                        RunningJobData::calculate_next_run_time(
                            &now,
                            current_try,
                            backoff_initial_interval,
                            backoff_multiplier,
                            backoff_randomization,
                        )
                    }
                };

                do_retry_job(&tx, active_worker_id, job_id, run_info, next_time)?;
            }
        }
    }

    tx.commit()?;

    Ok(())
}
