use std::{ops::Deref, time::Duration};

use futures::Future;
use temp_dir::TempDir;

use crate::Queue;

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

pub async fn wait_for<F, Fut, E>(label: &str, f: F)
where
    F: Fn() -> Fut,
    Fut: Future<Output = Result<(), E>>,
    E: std::fmt::Display,
{
    wait_for_timeout(label, Duration::from_secs(5), f).await
}

pub async fn wait_for_timeout<F, Fut, E>(label: &str, timeout: Duration, f: F)
where
    F: Fn() -> Fut,
    Fut: Future<Output = Result<(), E>>,
    E: std::fmt::Display,
{
    let max_check = 1000;
    let mut check_interval = 10;
    let start_time = tokio::time::Instant::now();
    let final_time = start_time + timeout;
    let mut last_error: E;

    loop {
        match f().await {
            Ok(()) => return,
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
