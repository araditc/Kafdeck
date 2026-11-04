# W68 G3 schema operations and recovery

The declarative runtime remains explicitly enabled. Starting an API or reading
status never installs, adopts or advances the declarative schema. The operator
uses the reviewed API binary with the deployment's existing server configuration
and notification persistence connection. W68 and notification subscriptions must
share that same database and schema for atomic typed effects.
One admitted deployment identity is supported per notification persistence
schema: the underlying product subscription key has no deployment column.
Different deployments use separate databases or explicitly configured PostgreSQL
schemas; independent logical ownership keys do not partition the product table.

## Explicit schema installation and upgrade

Stop writers and retain a verified backup first. Run with the deployment's DBA
credential, never a credential supplied through a manifest or HTTP request:

```powershell
./scripts/ops/w68-runtime-migrate.ps1 -ApiDll <absolute-reviewed-api.dll> -WorkingDirectory <absolute-configured-deployment-directory> -PrivateLogDirectory <fresh-absolute-private-directory>
```

The API command is `--declarative-schema-migrate`. It builds the configured
services, executes the schema operation, and exits before starting HTTP or
workers. Ordinary database credentials retain only runtime rights afterwards.
The wrapper keeps native diagnostics private and prints a finite success code.

Fresh SQLite/PostgreSQL installs and verified PostgreSQL baseline version 1
upgrade to runtime version 2. A partial schema, unsupported ledger, altered
column/constraint/index, unvalidated foreign key, unexpected trigger/RLS, or
ownership/lease high-water inconsistency refuses transactionally. PostgreSQL
compares against a separately generated reference schema within the same
transaction; failed verification also rolls back the provisional adoption
ledger and the temporary reference schema. SQLite accepts the exact supported
schema shape, without rewriting an unknown legacy definition.

## Existing complete five-table installation without a ledger

This is a distinct, reviewed operator action:

```powershell
./scripts/ops/w68-runtime-migrate.ps1 -ApiDll <absolute-reviewed-api.dll> -WorkingDirectory <absolute-configured-deployment-directory> -PrivateLogDirectory <fresh-absolute-private-directory> -AdoptExisting -AdoptionEvidenceSha256 <64-lowercase-hex-reviewed-backup-and-schema-proof>
```

The process adds `--declarative-adopt-existing` and supplies the evidence hash
through server configuration. All five known tables must exist and match the
supported structure; no missing table or unsupported ownership state is silently
replaced. Adoption records an immutable schema audit reference. It does not
register an owner/source/controller or authorize resource adoption.

The protected version 1 ownership and audit column shapes remain intact. They
store a domain separated SHA256 key for a new full OIDC principal. The immutable
plan and runtime audit retain the exact server-derived principal, joined by
operation ID. A principal hash never authenticates or grants permission.

## Transaction and recovery boundaries

An immutable plan ID is the existing governed mutation operation ID. Approval,
confirmation, risk and operation lifecycle remain in that mutation engine.
Confirm/approve use the same locked active-plan preconditions as Apply: exact
source registration revision, generation, ownership and subscription snapshots,
current authority and database-time lease validity. This read never renews a
lease or manufactures an effect.
Typed subscription write, ownership revision/generation high-water, audit and
commit receipt share one database transaction. A committed receipt resolves an
acknowledgement lost after commit without repeating the subscription CAS.
Without a receipt, an ambiguous operation remains Unknown for operator review.
Raw historical receipt lookup is not recovery authority. A positive recovery
proof separately verifies the exact requester/plan hash, current deployment
generation, enabled owner/controller, exact source generation and registration
revision, and current server authorization before and after reading the receipt.
Its original commit must precede the immutable plan expiry. It does not require
the old lease to remain active, and never reacquires one or reissues an effect.
Revoked/re-enabled source bindings and old-generation receipts cannot become
positive recovery proofs simply because their historical rows still exist.

Leases use database time and permanent epoch high-water. Their holder is a
server-bound operation identity; durable bindings also pin the current
registered controller and exact authenticated principal. A successful effect
expires its lease atomically so the next plan can acquire a newer epoch.
Revoked current authority or expiry during an awaited check rolls the transaction
back. SQLite uses an immediate writer transaction; PostgreSQL combines database
serialization with row locks and final fence checks.

## Outside backup authority and controlled restore admission

Before first activation, configure an outside file containing exactly:

```json
{"version":1,"deploymentId":"reviewed-deployment","generation":"canonical-guid","state":"quarantined"}
```

Set `Kafdeck:Declarative:DeploymentGeneration`, absolute `RecoveryAnchorFile`,
and exact byte SHA256 `RecoveryAnchorSha256` in trusted server configuration.
The file must remain outside all database/data-directory backups. Schema
installation does not enroll a generation. With HTTP and writers still stopped,
use `-AdmitGeneration -TargetGeneration <guid> -FenceEvidenceSha256 <proof>` on
the wrapper above; omit `-PreviousGeneration` only for initial enrollment. The
API mode verifies the exact pinned **quarantined** anchor, performs the durable
generation CAS, and exits without changing that file or starting a worker.

For restore, first publish a new quarantined generation and pin its new SHA256;
physically fence both original writer/database nodes and retain the reviewed
fence evidence. Verify and restore the physical backup into quarantine. Admit a
writable maintenance database only after that fencing, then invoke:

```powershell
./scripts/ops/w68-runtime-migrate.ps1 -ApiDll <absolute-reviewed-api.dll> -WorkingDirectory <configured-deployment-directory> -PrivateLogDirectory <fresh-private-directory> -AdmitGeneration -PreviousGeneration <restored-generation> -TargetGeneration <new-outside-generation> -FenceEvidenceSha256 <reviewed-fence-proof-sha256>
```

The store expires all restored leases in that same transaction, keeps permanent
high-water/history and immutable plans intact, and records admission audit.
Every old-generation plan stays fenced; editing its identity/fingerprint cannot
make it a new-generation operation. Current approved source content and current
OIDC/RBAC are still required for a fresh plan. Only after reviewed admission may
the operator publish an active anchor, pin its new byte SHA256 and restart.

A prematurely active new anchor over an old database generation refuses both
normal initialization and writes. An old configured generation, or an unprotected
constructor over an enrolled database, also refuses. Generation admission is
explicit and idempotent for the same expected/target/proof tuple; runtime and
status reads never advance it. This protocol does not automatically detect an
undeclared rollback or recall an external effect. Missing restored commit
receipts remain Unknown and never authorize automatic replay.

Physical proof class: `V08G3DeclarativePostgreSqlRecoveryTests`. Required safe
artifacts are `g3-postgresql-lease-failover-evidence.json` and
`g3-postgresql-physical-restore-fence-evidence.json`. These storage/production
anchor scenarios are separate from the genuine OIDC and multi-process API/CLI
acceptance suite. No release or protected-main acceptance is implied by this
runbook or an unexecuted test.
