use std::ops::Deref;

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
