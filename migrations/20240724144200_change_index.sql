DROP INDEX IF EXISTS project_keys;

CREATE INDEX IF NOT EXISTS project_keys_received_at ON envelopes (own_key, sampling_key, received_at);
