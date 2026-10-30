# W69 — Destroy and compensation boundaries

Status: **candidate / W68/W71 dependency**.

Terraform Destroy is not equivalent to Kafka deletion. Each admitted resource family must advertise its own supported lifecycle: Retire, Relinquish, Delete or None. Missing support means plan diagnostic and blocked destructive mutation, not generic delete.
Every destructive request must go through Kafdeck backend authorization, fresh state, risk evaluation, approval and audit. Critical operations may remain PendingApproval. Retry requires an idempotency key and persisted operation reference. Compensation is a *new governed effect*, never an implicit rollback, and only for explicitly safe inverses.
Prevent secret propagation, cross-tenant destroy, externally managed resource takeover, and broad delete on unsupported families.

## Acceptance
Irreversible resources denied, unauthorized actor denied, mismatch owner generation denied, changed state denied, PendingApproval preserved, failure-after-dispatch reconciled, audit present.

**No destructive API is implemented by this document.**

## Stored-plan and authorization requirements

Destroy requires the *original immutable server-stored* `planId` and `planFingerprint`, exact initiating principal, owner/source generation, current resource revision, policy version and policy-input fingerprint. Missing, tampered, expired, replayed or policy-stale plans are rejected and require fresh planning even if resource data is otherwise unchanged. Execution requires `declarative.apply` **AND** every underlying effect permission. CRITICAL actions require an eligible approver distinct from the immutable initiator; self-approval and any missing individual permission are denied.

The durable backend states `AwaitingApproval` and `ExecutionUnknown` remain **non-success**. A Terraform diagnostic may surface pending approval but must not report completed Destroy or discard the operation reference. Unknown effects are reconciled by observing the persisted operation and must never be blindly redispatched, even with an idempotency key. Acceptance tests include stolen plan, policy changed without resource change, missing declarative/apply vs underlying grant, initiator self-approval, restart/resume while awaiting approval and unknown effect after dispatch.
