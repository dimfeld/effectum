use uuid::Uuid;

use crate::{Error, Queue, Result};

impl Queue {
    fn job_succeeded(&self, external_id: Uuid) -> Result<()> {
        todo!();
    }

    fn job_failed(&self, external_id: Uuid) -> Result<()> {
        todo!();
    }

    fn job_expired(&self, job_id: i64) -> Result<()> {
        todo!()
    }

    fn job_heartbeat(&self, external_id: Uuid) -> Result<()> {
        todo!();
    }

    fn job_checkpoint(&self, external_id: Uuid, new_payload: &[u8]) -> Result<()> {
        todo!();
    }

    fn cancel_pending_job(&self, external_id: Uuid) -> Result<bool> {
        todo!();
    }
}
