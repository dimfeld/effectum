use rusqlite::{named_params, params, Connection};
use time::OffsetDateTime;
use tokio::sync::oneshot;

use super::{
    add_job::INSERT_JOBS_QUERY, recurring::schedule_next_recurring_job, DbOperationResult,
};
use crate::{job_status::JobState, recurring::create_job_from_recurring_template, Error, Result};

pub(crate) struct CompleteJobArgs {
    pub job_id: i64,
    pub run_info: String,
    pub now: i64,
    pub started_at: i64,
    pub success: bool,
    pub result_tx: oneshot::Sender<Result<Option<OffsetDateTime>>>,
}

pub(super) fn do_complete_job(
    tx: &Connection,
    job_id: i64,
    worker_id: u64,
    now: i64,
    started_at: i64,
    success: bool,
    this_run_info: String,
) -> Result<Option<OffsetDateTime>> {
    let mut delete_stmt =
        tx.prepare_cached(r##"DELETE FROM active_jobs WHERE job_id=?1 AND active_worker_id=?2"##)?;

    let altered = delete_stmt.execute(params![job_id, worker_id])?;
    if altered == 0 {
        return Err(Error::Expired);
    }

    let mut stmt = tx.prepare_cached(
        r##"
        UPDATE jobs SET
            status = $status,
            run_info = json_array_append(run_info, $this_run_info),
            started_at = $started_at,
            finished_at = $now
        WHERE job_id=$job_id
        RETURNING orig_run_at, from_recurring_job
        "##,
    )?;

    let (orig_run_at, from_recurring) = stmt.query_row(named_params! {
        "$job_id": job_id,
        "$now": now,
        "$started_at": started_at,
        "$this_run_info": this_run_info,
        "$status": if success { JobState::Succeeded.as_str() } else { JobState::Failed.as_str() },
    }, |row| {
        let orig_run_at = row.get::<_, i64>(0)?;
        let from_recurring = row.get::<_, Option<i64>>(1)?;
        Ok((orig_run_at, from_recurring))
    })?;

    let next_run_at = if let Some(from_recurring) = from_recurring {
        let orig_run_at = OffsetDateTime::from_unix_timestamp(orig_run_at)
            .map_err(|_| Error::TimestampOutOfRange("orig_run_at"))?;
        let now = OffsetDateTime::from_unix_timestamp(now)
            .map_err(|_| Error::TimestampOutOfRange("now"))?;

        let mut insert_job_stmt = tx.prepare_cached(INSERT_JOBS_QUERY)?;
        let ids = vec![rusqlite::types::Value::from(from_recurring)];
        let jobs = create_job_from_recurring_template(tx, now, orig_run_at, ids)?;
        let job = jobs.into_iter().next().unwrap();

        let run_at = job.run_at;
        schedule_next_recurring_job(tx, now, &mut insert_job_stmt, job)?;
        run_at
    } else {
        None
    };

    Ok(next_run_at)
}

pub(super) fn complete_job(
    tx: &Connection,
    worker_id: u64,
    args: CompleteJobArgs,
) -> DbOperationResult {
    let CompleteJobArgs {
        job_id,
        run_info,
        now,
        started_at,
        success,
        result_tx,
    } = args;

    let result = do_complete_job(tx, job_id, worker_id, now, started_at, success, run_info);
    DbOperationResult::CompleteJob(super::OperationResult { result, result_tx })
}
