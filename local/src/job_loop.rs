use std::sync::{atomic::AtomicU32, Arc};

use ahash::{HashMap, HashSet};
use tokio::{
    select,
    sync::{broadcast, Notify},
    time::{Duration, Instant},
};

use crate::SmartString;
use crate::{shared_state::SharedState, Error, NewJob, Queue, Result};

pub(crate) struct ListeningWorker {
    pub id: u64,
    pub notify_task_ready: Notify,
    pub job_types: Vec<SmartString>,
}

pub(crate) struct Workers {
    next_id: u64,
    workers: HashMap<u64, Arc<ListeningWorker>>,
    workers_by_type: HashMap<SmartString, Vec<Arc<ListeningWorker>>>,
}

impl Workers {
    pub fn new() -> Self {
        Workers {
            next_id: 0,
            workers: HashMap::default(),
            workers_by_type: HashMap::default(),
        }
    }

    /// Add a new worker, ready to accept jobs.
    pub(crate) fn add_worker(&mut self, job_types: &[SmartString]) -> Arc<ListeningWorker> {
        let worker_id = self.next_id;
        self.next_id += 1;

        let worker = Arc::new(ListeningWorker {
            id: worker_id,
            notify_task_ready: Notify::new(),
            job_types: job_types.to_vec(),
        });

        for job in job_types {
            self.workers_by_type
                .entry(job.clone())
                .or_default()
                .push(worker.clone());
        }

        self.workers.insert(worker.id, worker.clone());

        worker
    }

    pub(crate) fn remove_worker(&mut self, worker_id: u64) -> Result<()> {
        let worker = self
            .workers
            .remove(&worker_id)
            .ok_or(Error::WorkerNotFound(worker_id))?;

        for job in &worker.job_types {
            let type_workers = self.workers_by_type.get_mut(job);

            if let Some(type_workers) = type_workers {
                type_workers.retain(|w| !Arc::ptr_eq(w, &worker));
            }
        }

        Ok(())
    }

    pub(crate) fn new_job_available(&self, job_type: &str) {
        let workers = self.workers_by_type.get(job_type);
        if let Some(workers) = workers {
            for worker in workers {
                worker.notify_task_ready.notify_one();
            }
        }
    }
}

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
