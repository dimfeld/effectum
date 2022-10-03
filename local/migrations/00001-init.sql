CREATE TABLE jobs (
  job_id INTEGER PRIMARY KEY,
  external_id blob not null,
  job_type text not null,
  status text not null,
  status_changed bigint not null,
  from_recurring_job int references recurring_jobs(recurring_job_id),
  orig_run_at_time bigint not null,
  payload blob,
  max_retries int not null,
  backoff text not null,
  added_at bigint not null,
  default_timeout int not null,
  heartbeat_expiration_increment int not null,
  run_info text
);

CREATE UNIQUE INDEX jobs_external_id ON jobs(external_id);

CREATE TABLE pending (
  job_id INTEGER PRIMARY KEY,
  job_type text not null,
  run_at bigint not null,
  current_try int not null,
  checkpointed_payload blob
);

CREATE INDEX pending_run_at ON pending(run_at);

CREATE TABLE running (
  job_id INTEGER PRIMARY KEY,
  last_heartbeat bigint,
  started_at bigint not null,
  expires_at bigint not null,
  current_try int not null,
  checkpointed_payload blob
);

CREATE INDEX running_expires_at ON running(expires_at);

CREATE TABLE recurring (
  recurrring_job_id INTEGER PRIMARY KEY,
  external_id text not null,
  base_job_id bigint references jobs(job_id),
  schedule text not null
);

CREATE UNIQUE INDEX recurring_external_id ON recurring(external_id);
