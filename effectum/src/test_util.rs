use std::{
    fmt::Display,
    ops::Deref,
    path::PathBuf,
    sync::{atomic::AtomicUsize, Arc},
    time::Duration,
};

use futures::Future;
use once_cell::sync::Lazy;
use temp_dir::TempDir;
use time::OffsetDateTime;
use tokio::sync::Mutex;
use uuid::Uuid;

use crate::{
    job::RunningJob,
    job_registry::{JobRegistry, JobRunner},
    job_status::JobStatus,
    shared_state::Time,
    worker::{Worker, WorkerBuilder},
    JobState, Queue,
};

#[derive(Debug)]
pub struct TestContext {
    pub start_time: tokio::time::Instant,
    pub counter: AtomicUsize,
    pub max_count: Mutex<(usize, usize)>,
    pub value: Mutex<Vec<String>>,
    pub watch_rx: tokio::sync::watch::Receiver<usize>,
    pub watch_tx: tokio::sync::watch::Sender<usize>,
}

impl TestContext {
    pub fn new() -> Arc<TestContext> {
        let (watch_tx, watch_rx) = tokio::sync::watch::channel(0);
        Arc::new(TestContext {
            start_time: tokio::time::Instant::now(),
            counter: AtomicUsize::new(0),
            max_count: Mutex::new((0, 0)),
            value: Mutex::new(Vec::new()),
            watch_rx,
            watch_tx,
        })
    }

    pub async fn inc_max_count(&self) {
        let mut count = self.max_count.lock().await;
        count.0 += 1;
        count.1 = std::cmp::max(count.0, count.1);
    }

    pub async fn dec_max_count(&self) {
        let mut count = self.max_count.lock().await;
        count.0 -= 1;
    }

    pub async fn max_count(&self) -> usize {
        let count = self.max_count.lock().await;
        count.1
    }

    pub async fn push_str(&self, s: impl ToString) {
        let mut value = self.value.lock().await;
        value.push(s.to_string());
    }

    pub async fn get_values(&self) -> Vec<String> {
        let value = self.value.lock().await;
        value.clone()
    }
}

pub struct TestQueue {
    queue: Queue,
    pub path: PathBuf,
    #[allow(dead_code)]
    dir: TempDir,
}

impl Deref for TestQueue {
    type Target = Queue;

    fn deref(&self) -> &Self::Target {
        &self.queue
    }
}

impl TestQueue {
    pub async fn close_and_persist(self) -> TempDir {
        self.queue
            .close(Duration::from_secs(1))
            .await
            .expect("closing queue");
        self.dir
    }
}

pub fn queue_db_path(dir: &TempDir) -> PathBuf {
    dir.child("test.sqlite")
}

pub async fn create_test_queue(dir: TempDir) -> TestQueue {
    let path = queue_db_path(&dir);
    let queue = crate::Queue::new(&path).await.unwrap();

    TestQueue { queue, path, dir }
}

pub fn job_list() -> Vec<JobRunner<Arc<TestContext>>> {
    let count_task = JobRunner::builder("counter", |job, context: Arc<TestContext>| async move {
        let payload: usize = job.json_payload().unwrap_or(1);
        context
            .counter
            .fetch_add(payload, std::sync::atomic::Ordering::Relaxed);
        Ok::<_, String>("passed")
    })
    .build();

    let sleep_task = JobRunner::builder("sleep", sleep_task).build();

    let push_payload = JobRunner::builder(
        "push_payload",
        |job, context: Arc<TestContext>| async move {
            let payload = job.json_payload::<String>().expect("parsing payload");
            context.push_str(payload).await;
            Ok::<_, String>(())
        },
    )
    .build();

    let wait_for_watch_task = JobRunner::builder(
        "wait_for_watch",
        |job, context: Arc<TestContext>| async move {
            let watch_value = job
                .json_payload::<usize>()
                .expect("payload is not a number");
            let mut watch_rx = context.watch_rx.clone();
            while watch_rx.borrow().deref() != &watch_value {
                watch_rx.changed().await.expect("watch_rx changed");
            }
            Ok::<_, String>(())
        },
    )
    .build();

    let retry_task = JobRunner::builder("retry", |job, _context: Arc<TestContext>| async move {
        let succeed_after = job
            .json_payload::<usize>()
            .expect("payload is not a number");
        if job.current_try >= succeed_after as i32 {
            Ok(format!("success on try {}", job.current_try))
        } else {
            Err(format!("fail on try {}", job.current_try))
        }
    })
    .build();

    let max_count_task =
        JobRunner::builder("max_count", |_job, context: Arc<TestContext>| async move {
            context.inc_max_count().await;
            tokio::time::sleep(Duration::from_millis(100)).await;
            context.dec_max_count().await;
            Ok::<_, String>(())
        })
        .build();

    vec![
        count_task,
        sleep_task,
        push_payload,
        wait_for_watch_task,
        retry_task,
        max_count_task,
    ]
}

pub(crate) struct TestEnvironment {
    pub queue: TestQueue,
    pub time: Time,
    pub registry: JobRegistry<Arc<TestContext>>,
    pub context: Arc<TestContext>,
}

impl TestEnvironment {
    pub async fn new() -> Self {
        let dir = TempDir::new().unwrap();
        Self::from_path(dir).await
    }

    pub async fn from_path(dir: TempDir) -> Self {
        Lazy::force(&TRACING);
        let queue = create_test_queue(dir).await;

        let registry = JobRegistry::new(job_list());

        TestEnvironment {
            time: queue.state.time.clone(),
            queue,
            registry,
            context: TestContext::new(),
        }
    }

    pub fn worker(&self) -> WorkerBuilder<Arc<TestContext>> {
        Worker::builder(&self.queue, self.context.clone()).registry(&self.registry)
    }
}

// Keep this one as a seperate function to make sure that both full functions and closures can be
// used as task runners.
async fn sleep_task(job: RunningJob, _context: Arc<TestContext>) -> Result<(), String> {
    let duration = job.json_payload::<u64>().unwrap_or(5000);
    tokio::time::sleep(Duration::from_millis(duration)).await;
    Ok(())
}

pub async fn wait_for<F, Fut, T, E>(label: impl Display, f: F) -> T
where
    F: Fn() -> Fut,
    Fut: Future<Output = Result<T, E>>,
    E: std::fmt::Display,
{
    wait_for_timeout(label, Duration::from_secs(5), f).await
}

pub async fn wait_for_timeout<F, Fut, T, E>(label: impl Display, timeout: Duration, f: F) -> T
where
    F: Fn() -> Fut,
    Fut: Future<Output = Result<T, E>>,
    E: std::fmt::Display,
{
    let max_check = 1000;
    let mut check_interval = 10;
    let start_time = OffsetDateTime::now_utc();
    let final_time = start_time + timeout;
    let mut last_error: E;

    loop {
        tokio::task::yield_now().await;
        match f().await {
            Ok(value) => return value,
            Err(e) => {
                tracing::trace!(%label, %e, "Checking... not ready yet");
                last_error = e;
            }
        };

        let now = OffsetDateTime::now_utc();
        if now >= final_time {
            panic!(
                "Timed out waiting for {} after {}ms: {}",
                label,
                timeout.as_millis(),
                last_error
            );
        }

        check_interval = std::cmp::min(check_interval * 2, max_check);
        let sleep_time = std::cmp::min(
            (final_time - now).whole_milliseconds() as u64,
            check_interval,
        );

        tokio::time::sleep(Duration::from_millis(sleep_time)).await;
    }
}

pub async fn wait_for_job(label: impl Display, queue: &Queue, job_id: Uuid) -> JobStatus {
    wait_for_job_status(label, queue, job_id, JobState::Succeeded).await
}

pub async fn wait_for_job_fn(
    label: impl Display,
    queue: &Queue,
    job_id: Uuid,
    f: impl Fn(&JobStatus) -> bool,
) -> JobStatus {
    wait_for(label, || async {
        let status = match queue.get_job_status(job_id).await {
            Ok(status) => status,
            Err(crate::Error::NotFound) => return Err("Job status not found".to_string()),
            Err(e) => {
                panic!("{}", e);
            }
        };

        if f(&status) {
            Ok(status)
        } else {
            Err(format!("job status {:?}", status))
        }
    })
    .await
}

pub async fn wait_for_job_status(
    label: impl Display,
    queue: &Queue,
    job_id: Uuid,
    desired_status: JobState,
) -> JobStatus {
    wait_for_job_fn(label, queue, job_id, |status| {
        status.state == desired_status
    })
    .await
}

pub static TRACING: Lazy<()> = Lazy::new(|| {
    if std::env::var("TEST_LOG").is_ok() {
        configure_tracing();
    }
});

fn configure_tracing() {
    use tracing_subscriber::layer::SubscriberExt;
    let tree = tracing_tree::HierarchicalLayer::new(2)
        .with_targets(true)
        .with_bracketed_fields(true);

    let subscriber = tracing_subscriber::Registry::default().with(tree);

    tracing::subscriber::set_global_default(subscriber).unwrap();
}
