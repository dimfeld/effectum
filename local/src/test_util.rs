use std::{
    ops::Deref,
    sync::{atomic::AtomicUsize, Arc},
    time::Duration,
};

use futures::Future;
use once_cell::sync::Lazy;
use temp_dir::TempDir;
use tokio::sync::Mutex;
use uuid::Uuid;

use crate::{
    job::Job,
    job_registry::{JobDef, JobRegistry},
    job_status::JobStatus,
    worker::{Worker, WorkerBuilder},
    Queue,
};

#[derive(Debug)]
pub struct TestContext {
    pub counter: AtomicUsize,
    pub value: Mutex<Vec<String>>,
}

impl TestContext {
    pub fn new() -> Arc<TestContext> {
        Arc::new(TestContext {
            counter: AtomicUsize::new(0),
            value: Mutex::new(Vec::new()),
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

pub fn create_test_queue() -> TestQueue {
    let dir = temp_dir::TempDir::new().unwrap();
    let queue = crate::Queue::new(&dir.child("test.sqlite")).unwrap();

    TestQueue { queue, dir }
}

pub struct TestEnvironment {
    pub queue: TestQueue,
    pub registry: JobRegistry<Arc<TestContext>>,
    pub context: Arc<TestContext>,
}

impl TestEnvironment {
    pub fn worker(&self) -> WorkerBuilder<Arc<TestContext>> {
        Worker::builder(&self.registry, &self.queue, self.context.clone())
    }
}

impl Default for TestEnvironment {
    fn default() -> Self {
        Lazy::force(&TRACING);
        let queue = create_test_queue();

        let count_task = JobDef::builder("counter", |_job, context: Arc<TestContext>| async move {
            context
                .counter
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            Ok::<_, String>("passed")
        })
        .build();

        let sleep_task = JobDef::builder("sleep", sleep_task).build();

        let registry = JobRegistry::new(&[count_task, sleep_task]);

        TestEnvironment {
            queue,
            registry,
            context: TestContext::new(),
        }
    }
}

async fn sleep_task(job: Job, context: Arc<TestContext>) -> Result<(), String> {
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
    let start_time = tokio::time::Instant::now();
    let final_time = start_time + timeout;
    let mut last_error: E;

    loop {
        match f().await {
            Ok(value) => return value,
            Err(e) => {
                last_error = e;
            }
        };

        let now = tokio::time::Instant::now();
        if now >= final_time {
            panic!(
                "Timed out waiting for {} after {}ms: {}",
                label,
                timeout.as_millis(),
                last_error
            );
        }

        check_interval = std::cmp::min(check_interval * 2, max_check);
        let sleep_time = std::cmp::min(final_time - now, Duration::from_millis(check_interval));
        tokio::time::sleep(sleep_time).await;
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
