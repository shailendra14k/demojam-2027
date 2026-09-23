-- Casino Sentinel — operational DB + AML case management
-- Runs as the postgres superuser on first boot.

CREATE EXTENSION IF NOT EXISTS pgcrypto;

-- ---------------------------------------------------------------------------
-- CDC replication user (Debezium)
-- ---------------------------------------------------------------------------
DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'debezium') THEN
    CREATE ROLE debezium WITH LOGIN REPLICATION PASSWORD 'dbz';
  END IF;
END
$$;

GRANT CONNECT ON DATABASE casino TO debezium;
GRANT USAGE ON SCHEMA public TO debezium;

-- ---------------------------------------------------------------------------
-- Source tables (CDC-enabled)
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS players (
  player_id        VARCHAR(32) PRIMARY KEY,
  first_name       TEXT NOT NULL,
  last_name        TEXT NOT NULL,
  country          TEXT NOT NULL,
  date_of_birth    DATE,
  email            TEXT,
  phone            TEXT,
  status           TEXT NOT NULL DEFAULT 'ACTIVE',
  registered_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
  pep_flag         BOOLEAN NOT NULL DEFAULT FALSE,
  risk_tier        TEXT NOT NULL DEFAULT 'LOW',
  demo_label       TEXT
);

CREATE TABLE IF NOT EXISTS player_accounts (
  account_id       VARCHAR(32) PRIMARY KEY,
  player_id        VARCHAR(32) NOT NULL REFERENCES players(player_id),
  account_type     TEXT NOT NULL DEFAULT 'PLAYER',
  status           TEXT NOT NULL DEFAULT 'OPEN',
  balance_usd      NUMERIC(14,2) NOT NULL DEFAULT 0,
  opened_at        TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS kyc_profiles (
  kyc_id           VARCHAR(32) PRIMARY KEY,
  player_id        VARCHAR(32) NOT NULL REFERENCES players(player_id),
  kyc_status       TEXT NOT NULL DEFAULT 'VERIFIED',
  id_document_type TEXT,
  nationality      TEXT,
  occupation       TEXT,
  source_of_funds  TEXT,
  risk_rating      TEXT NOT NULL DEFAULT 'LOW',
  last_review_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
  watchlist_hit    BOOLEAN NOT NULL DEFAULT FALSE,
  prior_alerts     INTEGER NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS payment_methods (
  payment_id       VARCHAR(32) PRIMARY KEY,
  player_id        VARCHAR(32) NOT NULL REFERENCES players(player_id),
  method_type      TEXT NOT NULL,
  last4            TEXT,
  bank_name        TEXT,
  country          TEXT,
  created_at       TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS devices (
  device_id        VARCHAR(32) PRIMARY KEY,
  player_id        VARCHAR(32) NOT NULL REFERENCES players(player_id),
  device_type      TEXT NOT NULL,
  fingerprint      TEXT,
  last_seen_at     TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS transactions (
  transaction_id   VARCHAR(64) PRIMARY KEY,
  player_id        VARCHAR(32) NOT NULL REFERENCES players(player_id),
  txn_type         TEXT NOT NULL,
  amount           NUMERIC(12,2) NOT NULL,
  currency         TEXT NOT NULL DEFAULT 'USD',
  location         TEXT,
  payment_method   TEXT,
  device_id        VARCHAR(32),
  created_at       TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS wagers (
  wager_id         VARCHAR(64) PRIMARY KEY,
  player_id        VARCHAR(32) NOT NULL REFERENCES players(player_id),
  session_id       VARCHAR(64),
  game_type        TEXT NOT NULL,
  amount           NUMERIC(12,2) NOT NULL,
  win_amount       NUMERIC(12,2) NOT NULL DEFAULT 0,
  location         TEXT,
  created_at       TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS gaming_sessions (
  session_id       VARCHAR(64) PRIMARY KEY,
  player_id        VARCHAR(32) NOT NULL REFERENCES players(player_id),
  game_type        TEXT NOT NULL,
  location         TEXT,
  started_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
  ended_at         TIMESTAMPTZ,
  total_wagered    NUMERIC(12,2) NOT NULL DEFAULT 0,
  total_won        NUMERIC(12,2) NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS chip_transactions (
  chip_txn_id      VARCHAR(64) PRIMARY KEY,
  player_id        VARCHAR(32) NOT NULL REFERENCES players(player_id),
  direction        TEXT NOT NULL,
  amount           NUMERIC(12,2) NOT NULL,
  counterparty_id  VARCHAR(32),
  location         TEXT,
  created_at       TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS cage_transactions (
  cage_txn_id      VARCHAR(64) PRIMARY KEY,
  player_id        VARCHAR(32) NOT NULL REFERENCES players(player_id),
  direction        TEXT NOT NULL,
  amount           NUMERIC(12,2) NOT NULL,
  cage_window      TEXT,
  teller_id        TEXT,
  created_at       TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_txn_player_time ON transactions (player_id, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_wager_player_time ON wagers (player_id, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_cage_player_time ON cage_transactions (player_id, created_at DESC);

GRANT SELECT ON ALL TABLES IN SCHEMA public TO debezium;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO debezium;

-- Publication for Debezium pgoutput
DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_publication WHERE pubname = 'dbz_publication') THEN
    CREATE PUBLICATION dbz_publication FOR TABLE
      players, player_accounts, kyc_profiles, payment_methods, devices,
      transactions, wagers, gaming_sessions, chip_transactions, cage_transactions;
  END IF;
END
$$;

-- ---------------------------------------------------------------------------
-- AML case-management schema (not CDC-captured)
-- ---------------------------------------------------------------------------
CREATE SCHEMA IF NOT EXISTS aml;

CREATE TABLE IF NOT EXISTS aml.events (
  event_id         VARCHAR(64) PRIMARY KEY,
  player_id        VARCHAR(32) NOT NULL,
  event_type       TEXT NOT NULL,
  amount           NUMERIC(12,2),
  location         TEXT,
  status_tag       TEXT NOT NULL DEFAULT 'NORMAL',
  risk_score       INTEGER,
  payload          JSONB NOT NULL DEFAULT '{}'::jsonb,
  created_at       TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS aml.alerts (
  alert_id         VARCHAR(64) PRIMARY KEY,
  player_id        VARCHAR(32) NOT NULL,
  player_name      TEXT,
  alert_type       TEXT NOT NULL,
  priority         TEXT NOT NULL,
  risk_score       INTEGER NOT NULL,
  rule_score       INTEGER,
  ml_score         INTEGER,
  behavioral_score INTEGER,
  kyc_score        INTEGER,
  matched_rules    TEXT[] NOT NULL DEFAULT '{}',
  explanation      TEXT NOT NULL,
  factors          JSONB NOT NULL DEFAULT '[]'::jsonb,
  triggering_ids   TEXT[] NOT NULL DEFAULT '{}',
  status           TEXT NOT NULL DEFAULT 'OPEN',
  created_at       TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS aml.player_scores (
  player_id        VARCHAR(32) PRIMARY KEY,
  player_name      TEXT,
  country          TEXT,
  risk_score       INTEGER NOT NULL,
  risk_tier        TEXT NOT NULL,
  month_volume     NUMERIC(14,2) NOT NULL DEFAULT 0,
  explanation      TEXT,
  updated_at       TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS aml.cases (
  case_id          VARCHAR(64) PRIMARY KEY,
  player_id        VARCHAR(32) NOT NULL,
  player_name      TEXT,
  title            TEXT NOT NULL,
  priority         TEXT NOT NULL,
  status           TEXT NOT NULL DEFAULT 'NEW',
  assignee         TEXT,
  alert_ids        TEXT[] NOT NULL DEFAULT '{}',
  created_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at       TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS aml.case_notes (
  note_id          VARCHAR(64) PRIMARY KEY,
  case_id          VARCHAR(64) NOT NULL REFERENCES aml.cases(case_id),
  author           TEXT NOT NULL,
  content          TEXT NOT NULL,
  created_at       TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS aml.sars (
  sar_id           VARCHAR(64) PRIMARY KEY,
  case_id          VARCHAR(64),
  player_id        VARCHAR(32) NOT NULL,
  player_name      TEXT,
  status           TEXT NOT NULL DEFAULT 'PENDING',
  narrative        TEXT,
  amount           NUMERIC(14,2),
  created_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
  filed_at         TIMESTAMPTZ
);

CREATE TABLE IF NOT EXISTS aml.pipeline_status (
  component        TEXT PRIMARY KEY,
  status           TEXT NOT NULL,
  detail           TEXT,
  updated_at       TIMESTAMPTZ NOT NULL DEFAULT now()
);

INSERT INTO aml.pipeline_status (component, status, detail) VALUES
  ('transaction', 'ONLINE', 'Casino operational writes'),
  ('debezium',    'PENDING', 'Waiting for connector'),
  ('kafka',       'PENDING', 'Waiting for AMQ Streams'),
  ('rules',       'PENDING', 'Waiting for pipeline'),
  ('scoring',     'PENDING', 'Waiting for pipeline'),
  ('alert',       'PENDING', 'Waiting for aggregator'),
  ('investigation','ONLINE', 'Case management API')
ON CONFLICT (component) DO NOTHING;

CREATE INDEX IF NOT EXISTS idx_aml_events_time ON aml.events (created_at DESC);
CREATE INDEX IF NOT EXISTS idx_aml_alerts_time ON aml.alerts (created_at DESC);
CREATE INDEX IF NOT EXISTS idx_aml_alerts_player ON aml.alerts (player_id);

GRANT USAGE ON SCHEMA aml TO PUBLIC;
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA aml TO PUBLIC;
ALTER DEFAULT PRIVILEGES IN SCHEMA aml GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO PUBLIC;
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA public TO PUBLIC;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO PUBLIC;
