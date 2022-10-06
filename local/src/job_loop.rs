use tokio::{
    select,
    time::{Duration, Instant},
};

use crate::{shared_state::SharedState, Error, NewJob, Queue, Result};

pub(crate) struct WaitingWorkers {}

async fn run_ready_jobs(state: SharedState) -> Result<i64> {
    // Get the task types which have waiting workers.
    // Get ready jobs from the database for those task types.
    // Send the jobs to workers, as available.
    // Once everything is done, return the time of the next job.
    // If we ran out of workers and there are still pending jobs, return the types of those jobs so
    // that when workers become available we can notify the loop.
    todo!()
}

pub(crate) async fn run_jobs_task(state: SharedState) -> Result<()> {
    let mut close = state.close.clone();
    loop {
        let updated = state.notify_updated.notified();
        tokio::pin!(updated);
        // Tell the `Notified` to start waiting for a notification even though we aren't
        // actually waiting yet.
        updated.as_mut().enable();

        let next_time = run_ready_jobs(state.clone()).await?;
        let now = time::OffsetDateTime::now_utc().unix_timestamp();
        let sleep_time = Instant::now() + Duration::from_secs((next_time - now) as u64);

        select! {
            _ = tokio::time::sleep_until(sleep_time), if next_time > now => {},
            // Jobs were added, so check again.
            _ = &mut updated => {},
            _ = close.changed() => break,
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use crate::Queue;

    #[tokio::test]
    pub async fn run_job() {}

    #[tokio::test]
    pub async fn run_job_in_future() {}

    #[tokio::test]
    pub async fn run_jobs_with_priority() {}
}
