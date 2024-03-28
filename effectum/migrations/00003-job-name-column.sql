ALTER TABLE jobs
  ADD COLUMN name text;

CREATE INDEX jobs_name_added_at ON jobs (name, added_at DESC)
WHERE
  name IS NOT NULL;
