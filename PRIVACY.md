# Babblebox Privacy Policy

Effective date: April 10, 2026

This document is the repository copy of the Babblebox privacy policy. It explains what Babblebox may access, what it stores, how that information is used, and the product limits intended to keep storage and exposure bounded.

Babblebox is intentionally compact and privacy-aware by design:

- no general-purpose message archive
- no media or attachment blob storage in Postgres
- no durable quote-feed archive for Moment Cards
- no always-on AI scanning by default
- private-first handling for sensitive utility flows such as Watch, reminders, Later, Capture, anonymous confessions, and Shield configuration

## Scope

This policy applies to:

- the Babblebox website
- the Babblebox Discord bot
- Babblebox-managed feature state stored to support bot functionality

This policy does not replace Discord's own privacy practices. Discord controls the platform account, direct platform messaging, guild membership, and many visibility rules outside Babblebox itself.

## Information Babblebox May Collect

Depending on the feature being used, Babblebox may process or store:

- Discord identifiers such as user IDs, guild IDs, channel IDs, message IDs, role IDs, and timestamps
- compact feature configuration and state, including Watch preferences, ignored channels or users, Later markers, reminders, AFK state, AFK schedules, Daily Arcade results, Buddy or profile state, and Shield or admin configuration
- limited message or attachment context needed to respond to commands, build Moment Cards from visible messages, deliver Watch alerts, process Capture requests, or evaluate locally flagged Shield events, including visible message text, embed text, forwarded message snapshots, and attachment labels
- if a server enables suspicious-member review, compact account-age, avatar-state, display-name, and locally flagged message or link context needed to score that review lane
- anonymous confession or reply submission text, Confessions link fields, compact review metadata, bot-private author mappings, bot-private owner reply opportunities, and limited private appeals or reports needed to run staff-blind confession moderation when a server has Confessions enabled
- for Confessions, Babblebox now protects sensitive content and identity linkage with application-level encryption and separate lookup domains before those fields reach durable Postgres storage

## Information Babblebox Intentionally Avoids Storing Durably

Babblebox is designed not to keep certain high-churn or high-risk data as long-term durable records. Examples include:

- a general-purpose message-content archive
- media or attachment blobs in Postgres
- raw attachment filenames or raw Discord CDN URLs in staff-visible confession records
- a durable quote-feed database for Moment Cards
- a full deleted-message archive table
- long-term archives of DM bodies or Capture transcript bodies
- heavy moderation warehouse-style event history

## How Information Is Used

Babblebox uses information to operate the bot and its features, including:

- running commands and building Discord responses
- delivering Watch alerts, reminders, Later markers, Capture output, AFK behavior, and similar utilities
- maintaining Daily Arcade progress, compact identity state, and other restart-safe feature state
- applying optional Shield live-message moderation plus bounded private Shield feature-surface checks for utilities and Confessions where those protections are part of the product
- accepting and moderating anonymous confessions without exposing the author to server staff
- keeping the service reliable on constrained infrastructure through compact, purpose-bound persistence

Babblebox is not designed as an ad network, data brokerage system, or marketing profile builder.

## Public and Private Behavior

Babblebox intentionally uses different visibility defaults depending on the feature:

- some features are public-friendly by design, such as profile-style surfaces or Daily sharing
- Moment Cards are built from visible Discord messages and remain tied to visible message context instead of becoming a hidden archive
- Watch alerts are DM-only
- Capture transcripts are delivered privately rather than kept as long-term database archives
- Later markers, reminders, and sensitive setup flows are private-first
- AFK reasons, reminder text, public reminder delivery, watch keywords, and Confessions link checks now use bounded private Shield feature-surface evaluation instead of bypassing Babblebox core safety entirely
- anonymous confessions are optional, are submitted privately when enabled, keep the author hidden from staff, and let staff review by confession ID and case ID only while Babblebox still enforces safety internally
- that privacy model is meant to make raw database browsing and accidental exposure materially harder, not to claim that the service operator has been removed from the trust boundary
- deploying the Confessions privacy code and keys is not enough by itself; Babblebox now tracks a privacy-hardening readiness state because legacy rows remain weaker until the Confessions backfill finishes
- Confessions, anonymous replies, owner replies, and pending self-edits allow up to 4000 characters; appeals and reports stay capped at 1800
- Confessions link policy can be `Disabled`, `Trusted Only`, or `Allow All Safe`, but Babblebox Shield still blocks unsafe, malicious, suspicious, adult-blocked, shortener, link-in-bio, storefront, and guild-blocked destinations in every mode
- the latest live confession post can show `Create a confession`, and `Reply anonymously` is off by default, stays text-only when enabled, routes top-level replies into one reusable thread when Discord allows it, keeps reply-confessions inside that same thread without nested threads, and does not expose the author
- owner reply opportunities are bot-private, only trigger from explicit Discord replies to a published confession or first public owner reply, and can be opened from a DM prompt or `/confess reply-to-user`
- public owner replies post as `Anonymous Owner Reply`, stay text-only, and do not expose the confession owner or the responding member in public or staff-facing moderation surfaces
- self-edit is off by default and limited to pending submissions when enabled; self-delete is available privately to the original author through Babblebox's internal ownership check
- images are off by default for Confessions and only work after admins explicitly enable them; enabled image confessions always route through private review
- appeals or reports can be sent privately to a configured support channel without exposing the author's Discord identity to staff
- Babblebox hides the account behind a confession, but a self-identifying link destination or image content can still reveal the sender if they choose to include it
- Shield and admin configuration flows are intended for administrators or moderators rather than general public display
- suspicious-member review is private/admin-facing, uses bounded local signals, and does not read Discord profile bios or about-me text in the current implementation

Server administrators still influence visibility through Discord permissions, log channels, review channels, and command usage in their own server.

## Infrastructure and Third Parties

Babblebox may rely on necessary service providers to operate, including:

- Discord, for platform delivery and bot operation
- Supabase or Postgres-backed storage, for durable feature state

### Optional AI-assisted Shield review

Babblebox does not perform always-on AI scanning by default.

If optional AI-assisted Shield review is enabled where available, it only runs after local Shield logic has already flagged live-message content. In that flow, only minimal, sanitized, and truncated flagged text intended for that review should be sent to the configured AI provider, even when the flagged signal came from scanned embed text, attachment labels, or forwarded message snapshots instead of the raw message body alone. Shield's private feature-surface checks for AFK, reminders, watch keywords, and Confessions link parity stay AI-free in this release.

Babblebox is not designed to sell personal information.

## Retention

Babblebox keeps durable data small and tied to feature needs.

Examples:

- Daily Arcade raw result rows are designed to prune after 180 days while streak and lifetime totals remain in profile-level storage
- short-lived admin lifecycle rows remain only while they are operationally relevant
- short-lived suspicious-member review rows remain only while review, snooze, or queue state is still operationally relevant
- ban-return candidate records are intended to have a bounded purge window
- terminal anonymous confession rows scrub previews, body text, link fields, and attachment metadata after resolution while the bot-private author mapping and compact keyed duplicate signatures are retained only for moderation continuity and abuse prevention
- keyed duplicate-abuse signals are guild-scoped instead of global across every server
- Watch settings, reminders, AFK settings, and Later markers remain until changed, cleared, expired, or removed

Deletion timing may depend on the feature. Some state expires naturally, some is replaced by newer state, and some is removed when a user or administrator clears it.

## Security

Babblebox follows a data-minimization approach because reducing durable storage also reduces privacy risk. Even so, no internet-connected service can promise absolute security.

Babblebox is intended to:

- keep persistence compact and purpose-limited
- avoid large archive behavior where possible
- use private-first flows for sensitive utilities
- protect sensitive Confessions content and identity linkage with separate application-managed encryption domains and keyed lookup hashes
- expose operator-facing warnings when Confessions privacy hardening is only partial and the backfill or key rotation cleanup is still incomplete
- rely on server administrators to configure Discord permissions and channels responsibly

Babblebox does not claim to be zero-knowledge or operator-proof. Infrastructure operators with code, runtime, and key control are still part of the trust model even after these privacy hardening measures.

## User and Admin Controls

Users and administrators can often control Babblebox directly through the bot by changing or clearing relevant feature state, such as:

- removing reminders
- clearing Later markers
- changing Watch settings
- disabling Shield live moderation
- reconfiguring admin log or review behavior

If you have a privacy-related request about Babblebox-managed data, include enough detail to identify the relevant server, user, and feature state.

## Contact

For privacy questions or support:

- GitHub: https://github.com/arno-create/babblebox-bot
- Support server: https://discord.com/servers/inevitable-friendship-1322933864360050688

## Policy Updates

This policy may be updated if Babblebox's product behavior, storage model, or privacy practices materially change. When that happens, the effective date at the top of this file should be updated as well.
