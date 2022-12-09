use rusqlite::{params, Connection, OptionalExtension};
use tokio::sync::oneshot;

use super::DbOperationResult;
use crate::{add_job::JobUpdate, Error, Result};

pub(crate) struct UpdateJobArgs {
    pub job: JobUpdate,
    pub result_tx: oneshot::Sender<Result<String>>,
}

fn do_update_job(tx: &Connection, job: JobUpdate) -> Result<String> {
    let mut find_job_stmt = tx.prepare_cached(
        r##"SELECT job_id, job_type, active_jobs.run_at IS NOT NULL, active_worker_id IS NOT NULL
        FROM jobs
        LEFT JOIN active_jobs USING(job_id)
        WHERE external_id = ?"##,
    )?;

    let (id, job_type, active, active_worker_id): (i64, String, bool, bool) = find_job_stmt
        .query_row([job.id], |row| {
            Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?))
        })
        .optional()?
        .ok_or(Error::NotFound)?;

    if active_worker_id {
        // Can't update a running job
        return Err(Error::JobRunning);
    } else if !active {
        // Can't update a job that already finished.
        return Err(Error::JobFinished);
    }

    if job.run_at.is_some() || job.priority.is_some() {
        let mut active_jobs_update = tx.prepare_cached(
            r##"UPDATE active_jobs
            SET run_at = COALESCE(?, run_at),
                priority = COALESCE(?, priority)
            WHERE job_id = ?"##,
        )?;

        active_jobs_update.execute(params![
            job.run_at.map(|t| t.unix_timestamp()),
            job.priority,
            id
        ])?;
    }

    if job.weight.is_some() || job.priority.is_some() || job.payload.is_some() {
        let mut jobs_update = tx.prepare_cached(
            r##"UPDATE jobs
            SET weight = COALESCE(?, weight),
                priority = COALESCE(?, priority),
                payload = COALESCE(?, payload),
                checkpointed_payload = CASE
                    WHEN checkpointed_payload IS NOT NULL
                        THEN COALESCE(?, checkpointed_payload)
                    ELSE NULL END
            WHERE job_id = ?"##,
        )?;

        jobs_update.execute(params![
            job.weight,
            job.priority,
            &job.payload,
            if job.update_checkpointed_payload {
                job.payload.as_ref()
            } else {
                None
            },
            id
        ])?;
    }

    Ok(job_type)
}

pub(super) fn update_job(tx: &Connection, args: UpdateJobArgs) -> DbOperationResult {
    let UpdateJobArgs { job, result_tx } = args;
    let result = do_update_job(tx, job);
    DbOperationResult::UpdateJob(super::OperationResult { result, result_tx })
}
