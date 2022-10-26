use std::sync::{atomic::AtomicU32, Arc};

use ahash::{HashMap, HashSet};
use tokio::{
    select,
    sync::{broadcast, watch, Notify},
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
    worker_count_tx: watch::Sender<usize>,
}

impl Workers {
    pub fn new(worker_count_tx: watch::Sender<usize>) -> Self {
        Workers {
            next_id: 0,
            workers: HashMap::default(),
            workers_by_type: HashMap::default(),
            worker_count_tx,
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
        self.worker_count_tx.send_replace(self.workers.len());

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

        self.worker_count_tx.send_replace(self.workers.len());

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
