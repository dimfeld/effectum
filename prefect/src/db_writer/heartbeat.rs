use rusqlite::{named_params, params, Connection, OptionalExtension};
use tokio::sync::oneshot;

use crate::Result;

use super::DbOperationResult;

pub(crate) struct WriteHeartbeatArgs {
    pub job_id: i64,
    pub new_expiration: i64,
    pub result_tx: oneshot::Sender<Result<Option<i64>>>,
}

fn do_write_heartbeat(
    tx: &Connection,
    job_id: i64,
    worker_id: u64,
    new_expire_time: i64,
) -> Result<Option<i64>> {
    let mut stmt = tx.prepare_cached(
        r##"UPDATE active_jobs
        SET expires_at = CASE
                WHEN expires_at > $new_expire_time THEN expires_at
                ELSE $new_expire_time
            END
        WHERE job_id=$job_id AND active_worker_id=$worker_id
        RETURNING expires_at"##,
    )?;

    let actual_new_expire_time: Option<i64> = stmt
        .query_row(
            named_params! {
                "$new_expire_time": new_expire_time,
                "$job_id": job_id,
                "$worker_id": worker_id,
            },
            |row| row.get::<_, i64>(0),
        )
        .optional()?;

    Ok(actual_new_expire_time)
}

pub(super) fn write_heartbeat(
    tx: &Connection,
    worker_id: u64,
    args: WriteHeartbeatArgs,
) -> DbOperationResult {
    let WriteHeartbeatArgs {
        job_id,
        new_expiration,
        result_tx,
    } = args;

    let result = do_write_heartbeat(tx, job_id, worker_id, new_expiration);
    DbOperationResult::NewExpirationResult(super::OperationResult { result, result_tx })
}

pub(crate) struct WriteCheckpointArgs {
    pub job_id: i64,
    pub new_expiration: i64,
    pub payload: Vec<u8>,
    pub result_tx: oneshot::Sender<Result<Option<i64>>>,
}

fn do_write_checkpoint(
    tx: &Connection,
    job_id: i64,
    worker_id: u64,
    new_expire_time: i64,
    payload: Vec<u8>,
) -> Result<Option<i64>> {
    let mut stmt = tx.prepare_cached(
        r##"UPDATE active_jobs
                SET expires_at = CASE
                        WHEN expires_at > $new_expire_time THEN expires_at
                        ELSE $new_expire_time
                    END
                WHERE job_id=$job_id AND active_worker_id=$worker_id
                RETURNING expires_at"##,
    )?;

    let actual_new_expire_time: Option<i64> = stmt
        .query_row(
            named_params! {
                "$new_expire_time": new_expire_time,
                "$job_id": job_id,
                "$worker_id": worker_id,
            },
            |row| row.get::<_, i64>(0),
        )
        .optional()?;

    let mut payload_update_stmt =
        tx.prepare_cached(r##"UPDATE jobs SET checkpointed_payload=?2 WHERE job_id=?1"##)?;
    payload_update_stmt.execute(params![job_id, payload])?;

    Ok(actual_new_expire_time)
}

pub(super) fn write_checkpoint(
    tx: &Connection,
    worker_id: u64,
    args: WriteCheckpointArgs,
) -> DbOperationResult {
    let WriteCheckpointArgs {
        job_id,
        new_expiration,
        payload,
        result_tx,
    } = args;

    let result = do_write_checkpoint(tx, job_id, worker_id, new_expiration, payload);
    DbOperationResult::NewExpirationResult(super::OperationResult { result, result_tx })
}
