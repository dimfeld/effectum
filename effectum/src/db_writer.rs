use rusqlite::Connection;
use tracing::{event, instrument, Level};
use uuid::Uuid;

use self::{
    add_job::{add_job, add_jobs, AddJobArgs, AddMultipleJobsArgs, AddMultipleJobsResult},
    cancel_job::{cancel_job, CancelJobArgs},
    complete::{complete_job, CompleteJobArgs},
    heartbeat::{write_checkpoint, write_heartbeat, WriteCheckpointArgs, WriteHeartbeatArgs},
    ready_jobs::{get_ready_jobs, GetReadyJobsArgs, ReadyJob},
    retry::{retry_job, RetryJobArgs},
    update_job::{update_job, UpdateJobArgs},
};
use crate::{error::Result, shared_state::SharedState, worker::log_error};

pub(crate) mod add_job;
pub(crate) mod cancel_job;
pub(crate) mod complete;
pub(crate) mod heartbeat;
pub(crate) mod job_recovery;
pub(crate) mod ready_jobs;
pub(crate) mod retry;
pub(crate) mod update_job;

pub(crate) use job_recovery::handle_active_jobs_at_startup;

pub(crate) struct DbOperation {
    pub worker_id: u64,
    pub operation: DbOperationType,
    pub span: tracing::Span,
}

pub(crate) enum DbOperationType {
    Close,
    CompleteJob(CompleteJobArgs),
    RetryJob(RetryJobArgs),
    GetReadyJobs(GetReadyJobsArgs),
    WriteCheckpoint(WriteCheckpointArgs),
    WriteHeartbeat(WriteHeartbeatArgs),
    AddJob(AddJobArgs),
    AddMultipleJobs(AddMultipleJobsArgs),
    UpdateJob(UpdateJobArgs),
    CancelJob(CancelJobArgs),
}

struct OperationResult<T> {
    result: Result<T>,
    result_tx: tokio::sync::oneshot::Sender<Result<T>>,
}

enum DbOperationResult {
    Close,
    EmptyValue(OperationResult<()>),
    NewExpirationResult(OperationResult<Option<i64>>),
    GetReadyJobs(OperationResult<Vec<ReadyJob>>),
    AddJob(OperationResult<Uuid>),
    AddMultipleJobs(OperationResult<AddMultipleJobsResult>),
    UpdateJob(OperationResult<String>),
    CancelJob(OperationResult<()>),
}

impl DbOperationResult {
    fn is_ok(&self) -> bool {
        match self {
            DbOperationResult::Close => true,
            DbOperationResult::EmptyValue(result) => result.result.is_ok(),
            DbOperationResult::NewExpirationResult(result) => result.result.is_ok(),
            DbOperationResult::GetReadyJobs(result) => result.result.is_ok(),
            DbOperationResult::AddJob(result) => result.result.is_ok(),
            DbOperationResult::AddMultipleJobs(result) => result.result.is_ok(),
            DbOperationResult::UpdateJob(result) => result.result.is_ok(),
            DbOperationResult::CancelJob(result) => result.result.is_ok(),
        }
    }

    fn send(self) {
        match self {
            DbOperationResult::Close => {}
            DbOperationResult::EmptyValue(result) => {
                result.result_tx.send(result.result).ok();
            }
            DbOperationResult::NewExpirationResult(result) => {
                result.result_tx.send(result.result).ok();
            }
            DbOperationResult::GetReadyJobs(result) => {
                result.result_tx.send(result.result).ok();
            }
            DbOperationResult::AddJob(result) => {
                result.result_tx.send(result.result).ok();
            }
            DbOperationResult::AddMultipleJobs(result) => {
                result.result_tx.send(result.result).ok();
            }
            DbOperationResult::UpdateJob(result) => {
                result.result_tx.send(result.result).ok();
            }
            DbOperationResult::CancelJob(result) => {
                result.result_tx.send(result.result).ok();
            }
        };
    }
}

#[instrument(level = "trace", skip_all, fields(count = %operations.len()))]
fn process_operations(
    conn: &mut Connection,
    state: &SharedState,
    operations: &mut Vec<DbOperation>,
) -> Result<bool> {
    let mut results = Vec::with_capacity(operations.len());
    let mut closed = false;

    let mut tx = conn.transaction()?;
    for op in operations.drain(..) {
        let _span = op.span.enter();
        // Use savepoints within the batch to allow rollback as needed, but still a single
        // transaction for the whole batch since it's many times faster.
        match tx.savepoint() {
            Ok(mut sp) => {
                let result = match op.operation {
                    DbOperationType::CompleteJob(args) => complete_job(&sp, op.worker_id, args),
                    DbOperationType::RetryJob(args) => retry_job(&sp, op.worker_id, args),
                    DbOperationType::GetReadyJobs(args) => {
                        get_ready_jobs(&sp, state, op.worker_id, args)
                    }
                    DbOperationType::WriteCheckpoint(args) => {
                        write_checkpoint(&sp, op.worker_id, args)
                    }
                    DbOperationType::WriteHeartbeat(args) => {
                        write_heartbeat(&sp, op.worker_id, args)
                    }
                    DbOperationType::AddJob(args) => add_job(&sp, args),
                    DbOperationType::AddMultipleJobs(args) => add_jobs(&sp, args),
                    DbOperationType::UpdateJob(args) => update_job(&sp, args),
                    DbOperationType::CancelJob(args) => cancel_job(&sp, args),
                    DbOperationType::Close => {
                        closed = true;
                        DbOperationResult::Close
                    }
                };

                let worked = result.is_ok();
                results.push(result);

                if worked {
                    log_error(sp.commit());
                } else {
                    log_error(sp.rollback());
                }
            }
            Err(e) => {
                event!(Level::ERROR, %e, "failed to create savepoint");
            }
        }
    }
    tx.commit()?;

    for result in results {
        result.send();
    }

    Ok(closed)
}

pub(crate) fn db_writer_worker(
    mut conn: Connection,
    state: SharedState,
    mut operations_rx: tokio::sync::mpsc::Receiver<DbOperation>,
) {
    const BATCH_SIZE: usize = 50;
    let mut operations = Vec::with_capacity(BATCH_SIZE);
    loop {
        operations.truncate(0);

        match operations_rx.blocking_recv() {
            Some(op) => operations.push(op),
            None => break,
        }

        // Get additional operations, if any are waiting.
        // This lets us process multiple operations in a batch for better efficiency.
        while operations.len() < BATCH_SIZE {
            match operations_rx.try_recv() {
                Ok(operation) => operations.push(operation),
                // Treat "empty" and "closed" as the same here. If it's closed then
                // we'll leave the loop next time around, after processing the last jobs below.
                Err(_) => break,
            }
        }

        match process_operations(&mut conn, &state, &mut operations) {
            Ok(true) => break,
            Ok(false) => {}
            Err(e) => event!(Level::ERROR, %e),
        }
    }

    log_error(conn.close().map_err(|(_, e)| e));
}
