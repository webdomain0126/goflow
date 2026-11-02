-- The jobs table is both the queue and the permanent record of every job.

CREATE TABLE jobs (
    id              BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    type            TEXT        NOT NULL,
    payload         JSONB       NOT NULL DEFAULT '{}',
    priority        SMALLINT    NOT NULL DEFAULT 0 CHECK (priority BETWEEN 0 AND 100),
    status          TEXT        NOT NULL DEFAULT 'pending'
                    CHECK (status IN ('pending', 'running', 'succeeded', 'cancelled', 'dead')),
    attempts        INTEGER     NOT NULL DEFAULT 0 CHECK (attempts >= 0),
    max_attempts    INTEGER     NOT NULL DEFAULT 3 CHECK (max_attempts >= 1),
    run_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    locked_by       TEXT,
    locked_until    TIMESTAMPTZ,
    last_error      TEXT,
    idempotency_key TEXT UNIQUE,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    finished_at     TIMESTAMPTZ,

    -- A running job always has a lease; any other job never has one.
    CONSTRAINT lease_matches_status CHECK (
        (status = 'running') = (locked_by IS NOT NULL AND locked_until IS NOT NULL)
    )
);

-- Claim query: "highest priority, oldest run_at, among pending jobs".
-- A partial index only contains pending rows, so it stays small even
-- when the table holds millions of finished jobs.
CREATE INDEX jobs_claim_idx ON jobs (priority DESC, run_at, id) WHERE status = 'pending';

-- Reaper query: "running jobs whose lease has expired".
CREATE INDEX jobs_lease_idx ON jobs (locked_until) WHERE status = 'running';

-- Listing jobs, newest first, optionally filtered by status or type.
CREATE INDEX jobs_status_id_idx ON jobs (status, id DESC);
CREATE INDEX jobs_type_id_idx   ON jobs (type, id DESC);
