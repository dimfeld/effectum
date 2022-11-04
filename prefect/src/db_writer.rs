use crate::error::{Error, Result};
use crate::shared_state::SharedState;
use crate::worker::log_error;
use rusqlite::{named_params, params, Connection, Transaction};
use tracing::{event, instrument, Level};

use self::add_job::{add_job, add_jobs, AddJobArgs, AddMultipleJobsArgs};
use self::complete::{complete_job, CompleteJobArgs};
use self::heartbeat::{write_checkpoint, write_heartbeat, WriteCheckpointArgs, WriteHeartbeatArgs};
use self::ready_jobs::{get_ready_jobs, GetReadyJobsArgs, ReadyJob};
use self::retry::{retry_job, RetryJobArgs};

pub(crate) mod add_job;
pub(crate) mod complete;
pub(crate) mod heartbeat;
pub(crate) mod ready_jobs;
pub(crate) mod retry;

pub(crate) struct DbOperation {
    pub worker_id: u64,
    pub operation: DbOperationType,
    pub span: tracing::Span,
}

pub(crate) enum DbOperationType {
    CompleteJob(CompleteJobArgs),
    RetryJob(RetryJobArgs),
    GetReadyJobs(GetReadyJobsArgs),
    WriteCheckpoint(WriteCheckpointArgs),
    WriteHeartbeat(WriteHeartbeatArgs),
    AddJob(AddJobArgs),
    AddMultipleJobs(AddMultipleJobsArgs),
}

#[instrument(level = "trace", skip_all, fields(count = %operations.len()))]
fn process_operations(
    conn: &mut Connection,
    state: &SharedState,
    operations: &mut Vec<DbOperation>,
) -> Result<()> {
    let mut tx = conn.transaction()?;
    for op in operations.drain(..) {
        let _span = op.span.enter();
        // Use savepoints within the batch to allow rollback as needed, but still a single
        // transaction for the whole batch since it's many times faster.
        match tx.savepoint() {
            Ok(mut sp) => {
                let worked = match op.operation {
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
                };

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

    Ok(())
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
            None => return,
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

        log_error(process_operations(&mut conn, &state, &mut operations));
    }
}
