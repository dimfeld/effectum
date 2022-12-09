use rusqlite::{Connection, OptionalExtension};
use time::OffsetDateTime;
use tokio::sync::oneshot;
use uuid::Uuid;

use super::DbOperationResult;
use crate::{Error, Result};

pub(crate) struct CancelJobArgs {
    pub id: Uuid,
    pub now: OffsetDateTime,
    pub result_tx: oneshot::Sender<Result<()>>,
}

fn do_cancel_job(tx: &Connection, now: OffsetDateTime, external_id: Uuid) -> Result<()> {
    let mut find_job_stmt = tx.prepare_cached(
        r##"SELECT job_id, active_jobs.run_at IS NOT NULL, active_worker_id IS NOT NULL
        FROM jobs
        LEFT JOIN active_jobs USING(job_id)
        WHERE external_id = ?"##,
    )?;

    let (id, active, active_worker_id): (i64, bool, bool) = find_job_stmt
        .query_row([external_id], |row| {
            Ok((row.get(0)?, row.get(1)?, row.get(2)?))
        })
        .optional()?
        .ok_or(Error::NotFound)?;

    if active_worker_id {
        // Can't cancel a running job
        return Err(Error::JobRunning);
    } else if !active {
        // Can't cancel a job that already finished.
        return Err(Error::JobFinished);
    }

    let mut del_stmt = tx.prepare_cached("DELETE FROM active_jobs WHERE job_id = ?")?;
    let mut update_stmt = tx.prepare_cached(
        r##"UPDATE jobs
            SET status = 'cancelled',
                finished_at = ?
            WHERE job_id = ?"##,
    )?;

    del_stmt.execute([id])?;
    update_stmt.execute([now.unix_timestamp(), id])?;

    Ok(())
}

pub(super) fn cancel_job(tx: &Connection, args: CancelJobArgs) -> DbOperationResult {
    let CancelJobArgs { id, now, result_tx } = args;
    let result = do_cancel_job(tx, now, id);
    DbOperationResult::CancelJob(super::OperationResult { result, result_tx })
}
