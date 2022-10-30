use std::{
    ops::Deref,
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
    job::Job,
    job_registry::{JobDef, JobRegistry},
    job_status::JobStatus,
    shared_state::Time,
    worker::{Worker, WorkerBuilder},
    Queue,
};

#[derive(Debug)]
pub struct TestContext {
    pub counter: AtomicUsize,
    pub value: Mutex<Vec<String>>,
    pub watch_rx: tokio::sync::watch::Receiver<usize>,
    pub watch_tx: tokio::sync::watch::Sender<usize>,
}

impl TestContext {
    pub fn new() -> Arc<TestContext> {
        let (watch_tx, watch_rx) = tokio::sync::watch::channel(0);
        Arc::new(TestContext {
            counter: AtomicUsize::new(0),
            value: Mutex::new(Vec::new()),
            watch_rx,
            watch_tx,
        })
    }

    pub async fn push_str(&self, s: impl ToString) {
        let mut value = self.value.lock().await;
        value.push(s.to_string());
    }
}

pub struct TestQueue {
    queue: Queue,
    dir: TempDir,
}

impl Deref for TestQueue {
    type Target = Queue;

    fn deref(&self) -> &Self::Target {
        &self.queue
    }
}

pub async fn create_test_queue() -> TestQueue {
    let dir = temp_dir::TempDir::new().unwrap();
    let queue = crate::Queue::new(&dir.child("test.sqlite")).await.unwrap();

    TestQueue { queue, dir }
}

pub(crate) struct TestEnvironment {
    pub queue: TestQueue,
    pub time: Time,
    pub registry: JobRegistry<Arc<TestContext>>,
    pub context: Arc<TestContext>,
}

impl TestEnvironment {
    pub async fn new() -> Self {
        Lazy::force(&TRACING);
        let queue = create_test_queue().await;

        let count_task = JobDef::builder("counter", |_job, context: Arc<TestContext>| async move {
            context
                .counter
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            Ok::<_, String>("passed")
        })
        .build();

        let sleep_task = JobDef::builder("sleep", sleep_task).build();

        let push_payload = JobDef::builder(
            "push_payload",
            |job, context: Arc<TestContext>| async move {
                let payload = job.json_payload::<String>().expect("parsing payload");
                context.push_str(payload).await;
                Ok::<_, String>(())
            },
        )
        .build();

        let wait_for_watch_task = JobDef::builder(
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

        let registry =
            JobRegistry::new(&[count_task, sleep_task, push_payload, wait_for_watch_task]);

        TestEnvironment {
            time: queue.state.time.clone(),
            queue,
            registry,
            context: TestContext::new(),
        }
    }

    pub fn worker(&self) -> WorkerBuilder<Arc<TestContext>> {
        Worker::builder(&self.registry, &self.queue, self.context.clone())
    }
}

// Keep this one as a sepatate function to make sure that both full functions and closures can be
// used as task runners.
async fn sleep_task(job: Job, _context: Arc<TestContext>) -> Result<(), String> {
    let duration = job.json_payload::<u64>().unwrap_or(5000);
    tokio::time::sleep(Duration::from_millis(duration)).await;
    Ok(())
}

pub async fn wait_for<F, Fut, T, E>(label: &str, f: F) -> T
where
    F: Fn() -> Fut,
    Fut: Future<Output = Result<T, E>>,
    E: std::fmt::Display,
{
    wait_for_timeout(label, Duration::from_secs(5), f).await
}

pub async fn wait_for_timeout<F, Fut, T, E>(label: &str, timeout: Duration, f: F) -> T
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
            final_time - now,
            time::Duration::milliseconds(check_interval),
        );

        // Since we're often using virtual time, we have to sleep here with the blocking
        // APIs to actually wait.
        tokio::task::spawn_blocking(move || std::thread::sleep(sleep_time.unsigned_abs()))
            .await
            .unwrap();
    }
}

pub async fn wait_for_job(label: &str, queue: &Queue, job_id: Uuid) -> JobStatus {
    wait_for(label, || async {
        let status = queue.get_job_status(job_id).await.unwrap();
        if status.status == "success" {
            Ok(status)
        } else {
            Err(format!("job status {:?}", status))
        }
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
