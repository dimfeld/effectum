CREATE TABLE jobs (
  job_id INTEGER PRIMARY KEY,
  external_id blob not null,
  job_type text not null,
  status text not null,
  status_changed text not null,
  recurring_job_id int references recurring_jobs(recurring_job_id),
  orig_run_at_time text not null,
  payload blob,
  max_retries int not null,
  backoff text not null,
  added_at text not null default current_timestamp,
  default_timeout int not null,
  heartbeat_expiration_increment int not null,
  run_info text
);
