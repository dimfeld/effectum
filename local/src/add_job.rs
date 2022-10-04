use uuid::Uuid;

use crate::{Error, NewJob, Queue, Result};

impl Queue {
    fn add_job(
        &mut self,
        recurring_job_id: Option<i64>,
        job_config: &NewJob,
    ) -> Result<(i64, Uuid)> {
        todo!();
    }
}
