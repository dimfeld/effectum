CREATE TABLE active_jobs (
  job_id INTEGER PRIMARY KEY,
  external_id blob not null,
  job_type text not null,
  priority int not null default 0,
  from_recurring_job int,
  orig_run_at_time bigint not null,
  payload blob,
  max_retries int not null,
  backoff_multiplier real not null,
  backoff_randomization real not null,
  backoff_initial_interval int not null,
  added_at bigint not null,
  default_timeout int not null,
  heartbeat_increment int not null,
  run_info text
);

CREATE UNIQUE INDEX active_jobs_external_id ON active_jobs(external_id);

CREATE TABLE done_jobs (
  job_id INTEGER PRIMARY KEY,
  external_id blob not null,
  job_type text not null,
  priority int not null default 0,
  status text not null,
  done_time bigint not null,
  from_recurring_job int,
  orig_run_at_time bigint not null,
  payload blob,
  max_retries int not null,
  backoff_multiplier real not null,
  backoff_randomization real not null,
  backoff_initial_interval int not null,
  added_at bigint not null,
  default_timeout int not null,
  heartbeat_increment int not null,
  run_info text
);

CREATE UNIQUE INDEX done_jobs_external_id ON done_jobs(external_id);
CREATE INDEX done_jobs_job_type ON done_jobs(done_time desc);
CREATE INDEX done_jobs_job_type_and_time ON done_jobs(job_type, done_time desc);

CREATE TABLE pending (
  job_id INTEGER PRIMARY KEY,
  priority int not null,
  job_type text not null,
  run_at bigint not null,
  current_try int not null,
  checkpointed_payload blob
);

CREATE INDEX pending_run_at ON pending(priority desc, run_at);

CREATE TABLE running (
  job_id INTEGER PRIMARY KEY,
  worker_id int not null,
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
