use rusqlite::{named_params, params, Connection};
use tokio::sync::oneshot;

use crate::{Error, Result};

use super::DbOperationResult;

pub(crate) struct RetryJobArgs {
    pub job_id: i64,
    pub run_info: String,
    pub next_time: i64,
    pub result_tx: oneshot::Sender<Result<()>>,
}

fn do_retry_job(
    tx: &Connection,
    worker_id: u64,
    job_id: i64,
    run_info: String,
    next_time: i64,
) -> Result<()> {
    let mut stmt = tx.prepare_cached(
        r##"UPDATE active_jobs SET
                    active_worker_id=null,
                    run_at=$next_run_time
                    WHERE job_id=$job_id AND active_worker_id=$worker_id"##,
    )?;

    let altered = stmt.execute(named_params! {
        "$job_id": job_id,
        "$worker_id": worker_id,
        "$next_run_time": next_time,
    })?;

    if altered == 0 {
        return Err(Error::Expired);
    }

    let mut update_run_into_stmt = tx.prepare_cached(
        r##"UPDATE jobs SET
            current_try = current_try + 1,
            run_info = json_array_append(run_info, ?1)"##,
    )?;

    update_run_into_stmt.execute(params![run_info])?;
    Ok(())
}

pub(super) fn retry_job(tx: &Connection, worker_id: u64, args: RetryJobArgs) -> DbOperationResult {
    let RetryJobArgs {
        job_id,
        run_info,
        next_time,
        result_tx,
    } = args;

    let result = do_retry_job(tx, worker_id, job_id, run_info, next_time);

    DbOperationResult::EmptyValue(super::OperationResult { result, result_tx })
}
