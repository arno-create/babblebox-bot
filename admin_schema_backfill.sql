-- One-time idempotent backfill for legacy Babblebox admin schemas.
-- Safe to run before or during rollout of the admin schema-bootstrap ordering fix.

ALTER TABLE admin_followup_roles
ADD COLUMN IF NOT EXISTS review_pending BOOLEAN NOT NULL DEFAULT FALSE;

ALTER TABLE admin_followup_roles
ADD COLUMN IF NOT EXISTS review_version INTEGER NOT NULL DEFAULT 0;

ALTER TABLE admin_followup_roles
ADD COLUMN IF NOT EXISTS review_message_channel_id BIGINT NULL;

ALTER TABLE admin_followup_roles
ADD COLUMN IF NOT EXISTS review_message_id BIGINT NULL;

CREATE INDEX IF NOT EXISTS ix_admin_ban_return_expires
ON admin_ban_return_candidates (expires_at);

CREATE INDEX IF NOT EXISTS ix_admin_followup_due
ON admin_followup_roles (due_at);

CREATE INDEX IF NOT EXISTS ix_admin_followup_review_pending
ON admin_followup_roles (review_pending, review_message_id);
