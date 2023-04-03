CREATE TABLE IF NOT EXISTS envelopes (
  id              INTEGER PRIMARY KEY AUTOINCREMENT,
  own_key         TEXT,
  sampling_key    TEXT,
  envelope        BLOB
);

CREATE INDEX IF NOT EXISTS project_keys ON envelopes (own_key, sampling_key);
