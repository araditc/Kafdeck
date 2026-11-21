# RFC-0012 — Dashboard onboarding and encrypted Fleet credentials

Status: product-owner design accepted on 2026-11-21; implementation admission and specialist review remain required. The owner explicitly requested the complete dashboard workflow, encrypted database storage, temporary administrative setup, and restricted ongoing access. This is not a release or signing authorization.

## Problem and decision

The released 1.1 workflow requires an administrator to prepare Linux helpers and configure secret references before Fleet can be used. Source builds implementing this RFC let the installation owner configure networks, permissions and SSH credentials in Fleet, inspect actual services, and prepare restricted access and the bundled monitoring agent without editing application configuration files.

Linux/systemd without Docker and Linux/Docker Compose are equal acceptance targets. Docker is required only by the Compose adapter. Temporary administrative SSH credentials are used only for the fixed setup program; they are never persisted. The owner reviews a pinned host fingerprint, observed Kafka identities, selected services, package digest, ingestion certificate and maintenance window before preparation. Preparation does not restart Kafka. Applying the staged agent uses the existing separate health check, restart confirmation and recovery protocol, including fresh JVM telemetry before completion.

## Storage and authority

An installation-local SQLite database, beside managed identity state under `<StateFile>.fleet/settings.sqlite`, stores an authenticated encrypted Fleet document. It includes configuration, credential material, registry, audit outbox and recoverable setup payloads. Settings and registry revisions are independent. All writes use an immediate transaction and compare the appropriate revision. APIs return only credential metadata. Replacing a credential requires new secret input; revocation erases the stored secret and blocks future use. Revocation in Kafdeck does not remove a separately copied key from a Linux host's authorized-key file.

Data Protection keys live outside the database, in `<StateFile>.fleet/keys`. Windows wraps them with current-service-account DPAPI. Linux uses a local wrapping certificate and private directory/file permissions. Database-only copies cannot decrypt credentials. Full host compromise, or a Linux backup containing both the database and wrapping keys, is outside the database-at-rest confidentiality boundary. No external vault is required; installations may retain existing server-side secret references.

Only the current managed installation owner can manage settings and onboarding. Browser session authentication, CSRF, body bounds, no-store responses, CIDR admission, pinned SSH host identity and durable secret-free audit are mandatory. Log readers, agent installers and Kafka configuration managers retain separate grants. Existing configured installations are imported once; their legacy credential references remain a fallback unless shadowed or revoked in the database. Historical sidecars are retained for rollback, never used as the new authoritative registry.

## Host preparation and recovery

The server sends its embedded, fixed Python setup program, not user-authored shell commands. It inventories running systemd services and Compose services and verifies broker/cluster identity through the running process and storage metadata. Preparation creates dedicated forced-command accounts, root-owned helpers and catalogs, restricted sudo rules, a staged package, monitoring source tokens and an argument-file integration. Systemd uses a drop-in; Compose uses a generated override and verifies original Compose input hashes before recreation. Existing unrelated JVM settings are preserved. Unsupported or conflicting launch layouts are blocked with a reason rather than overwritten.

Restricted discovery and lifecycle keys are generated separately and encrypted before remote work. A durable reservation precedes monitoring enrollment; the complete payload is committed before SSH dispatch. Lost replies remain pending. Recovery can verify a root-owned receipt and hashes of the prepared artifacts through the restricted account, without requiring administrative credentials again. A partial preparation requires re-entry of temporary access to resume the same operation. An unused reservation can be cleared because it cannot have dispatched a remote payload. Unresolved operations are retained rather than evicted by completed history.

## Acceptance

- Owner/CSRF boundaries, network changes, revision conflicts, rotation/revocation, encrypted persistence and key-matched restore.
- Temporary administrator secrets absent from database, public responses and audit; no secret export API.
- Partial/lost setup replies cannot register a completed host without an intact receipt.
- A real rendered dashboard prepares a bare systemd host without Docker, installs the staged agent, and verifies Kafka health plus a fresh JVM metric.
- The same flow on an isolated Compose pilot at Arad Office preserves operational brokers and unrelated configuration.
- Existing read-only discovery, bounded logs, lifecycle recovery and Kafka mutation safety continue to pass relevant regression checks.
