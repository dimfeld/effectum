ALTER TABLE recurring RENAME COLUMN recurrring_job_id to recurring_job_id;
ALTER TABLE jobs RENAME COLUMN from_recurring_job to from_base_job;
