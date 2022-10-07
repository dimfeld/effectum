use ahash::{HashMap, HashSet};
use tokio::{
    select,
    time::{Duration, Instant},
};

use crate::SmartString;
use crate::{shared_state::SharedState, Error, NewJob, Queue, Result};

struct ListeningWorker {
    job_types: Vec<SmartString>,
    running: bool,
}

pub(crate) struct Workers {
    next_id: u64,
    waiting_by_type: HashMap<SmartString, Vec<u64>>,
    workers: HashMap<u64, ListeningWorker>,
    running_workers: HashSet<u64>,
}

impl Workers {
    fn new() -> Self {
        Workers {
            next_id: 0,
            waiting_by_type: HashMap::default(),
            workers: HashMap::default(),
            running_workers: HashSet::default(),
        }
    }

    /// Add a new worker, ready to accept jobs.
    pub(crate) fn add_worker(&mut self, job_types: Vec<SmartString>) -> u64 {
        let worker_id = self.next_id;
        self.next_id += 1;

        for job_type in &job_types {
            self.waiting_by_type
                .entry(job_type.clone())
                .and_modify(|ids| ids.push(worker_id))
                .or_insert_with(|| vec![worker_id]);
        }

        self.workers.insert(
            worker_id,
            ListeningWorker {
                running: false,
                job_types,
            },
        );

        worker_id
    }

    pub(crate) fn set_worker_waiting(&mut self, worker_id: u64) -> Result<usize> {
        let worker = self
            .workers
            .get_mut(&worker_id)
            .ok_or(Error::WorkerNotFound(worker_id))?;
        self.running_workers.remove(&worker_id);
        for job_type in &worker.job_types {
            if let Some(ids) = self.waiting_by_type.get_mut(job_type) {
                ids.push(worker_id);
            }
        }

        Ok(self.running_workers.len())
    }

    pub(crate) fn set_worker_running(&mut self, worker_id: u64) -> Result<usize> {
        let worker = self
            .workers
            .get_mut(&worker_id)
            .ok_or(Error::WorkerNotFound(worker_id))?;
        self.remove_waiting_worker(&worker.job_types, worker_id);
        self.running_workers.insert(worker_id);

        Ok(self.running_workers.len())
    }

    pub(crate) fn remove_worker(&mut self, worker_id: u64) -> Result<usize> {
        let worker = match self.workers.remove(&worker_id) {
            Some(w) => w,
            None => return Err(Error::WorkerNotFound(worker_id)),
        };

        self.running_workers.remove(&worker_id);
        self.remove_waiting_worker(&worker.job_types, worker_id);

        Ok(self.running_workers.len())
    }

    fn remove_waiting_worker(&mut self, job_types: &[SmartString], worker_id: u64) {
        for job_type in job_types {
            if let Some(ids) = self.waiting_by_type.get_mut(job_type) {
                let pos = ids.iter().position(|id| *id == worker_id);
                if let Some(pos) = pos {
                    ids.remove(pos);
                }
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
