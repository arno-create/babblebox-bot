-- One-time idempotent backfill for legacy Babblebox admin schemas.
-- Safe to run before or during rollout of the schema-bootstrap ordering fix.

ALTER TABLE admin_guild_configs
ADD COLUMN IF NOT EXISTS verification_deadline_action TEXT NOT NULL DEFAULT 'auto_kick';

ALTER TABLE admin_followup_roles
ADD COLUMN IF NOT EXISTS review_pending BOOLEAN NOT NULL DEFAULT FALSE;

ALTER TABLE admin_followup_roles
ADD COLUMN IF NOT EXISTS review_version INTEGER NOT NULL DEFAULT 0;

ALTER TABLE admin_followup_roles
ADD COLUMN IF NOT EXISTS review_message_channel_id BIGINT NULL;

ALTER TABLE admin_followup_roles
ADD COLUMN IF NOT EXISTS review_message_id BIGINT NULL;

ALTER TABLE admin_verification_states
ADD COLUMN IF NOT EXISTS review_pending BOOLEAN NOT NULL DEFAULT FALSE;

ALTER TABLE admin_verification_states
ADD COLUMN IF NOT EXISTS review_version INTEGER NOT NULL DEFAULT 0;

ALTER TABLE admin_verification_states
ADD COLUMN IF NOT EXISTS review_message_channel_id BIGINT NULL;

ALTER TABLE admin_verification_states
ADD COLUMN IF NOT EXISTS review_message_id BIGINT NULL;

ALTER TABLE admin_verification_states
ADD COLUMN IF NOT EXISTS last_result_code TEXT NULL;

ALTER TABLE admin_verification_states
ADD COLUMN IF NOT EXISTS last_result_at TIMESTAMPTZ NULL;

ALTER TABLE admin_verification_states
ADD COLUMN IF NOT EXISTS last_notified_code TEXT NULL;

ALTER TABLE admin_verification_states
ADD COLUMN IF NOT EXISTS last_notified_at TIMESTAMPTZ NULL;

CREATE TABLE IF NOT EXISTS admin_verification_review_queues (
    guild_id BIGINT PRIMARY KEY,
    channel_id BIGINT NULL,
    message_id BIGINT NULL,
    updated_at TIMESTAMPTZ NULL
);

CREATE TABLE IF NOT EXISTS admin_verification_notification_snapshots (
    guild_id BIGINT NOT NULL,
    run_context TEXT NOT NULL,
    operation TEXT NOT NULL,
    outcome TEXT NOT NULL,
    reason_code TEXT NOT NULL,
    signature TEXT NULL,
    notified_at TIMESTAMPTZ NULL,
    PRIMARY KEY (guild_id, run_context, operation, outcome, reason_code)
);

CREATE INDEX IF NOT EXISTS ix_admin_ban_return_expires
ON admin_ban_return_candidates (expires_at);

CREATE INDEX IF NOT EXISTS ix_admin_followup_due
ON admin_followup_roles (due_at);

CREATE INDEX IF NOT EXISTS ix_admin_followup_review_pending
ON admin_followup_roles (review_pending, review_message_id);

CREATE INDEX IF NOT EXISTS ix_admin_verification_warning_due
ON admin_verification_states (warning_at);

CREATE INDEX IF NOT EXISTS ix_admin_verification_kick_due
ON admin_verification_states (kick_at);

CREATE INDEX IF NOT EXISTS ix_admin_verification_guild
ON admin_verification_states (guild_id);

CREATE INDEX IF NOT EXISTS ix_admin_verification_review_pending
ON admin_verification_states (review_pending, review_message_id);

CREATE INDEX IF NOT EXISTS ix_admin_verification_last_notified
ON admin_verification_states (guild_id, last_notified_at);

CREATE INDEX IF NOT EXISTS ix_admin_verification_snapshot_notified
ON admin_verification_notification_snapshots (guild_id, notified_at);
