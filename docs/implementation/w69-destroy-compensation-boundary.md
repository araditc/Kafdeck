# W69 — Destroy and compensation boundaries

Status: **candidate / W68/W71 dependency**.

Terraform Destroy is not equivalent to Kafka deletion. Each admitted resource family must advertise its own supported lifecycle: Retire, Relinquish, Delete or None. Missing support means plan diagnostic and blocked destructive mutation, not generic delete.
Every destructive request must go through Kafdeck backend authorization, fresh state, risk evaluation, approval and audit. Critical operations may remain PendingApproval. Retry requires an idempotency key and persisted operation reference. Compensation is a *new governed effect*, never an implicit rollback, and only for explicitly safe inverses.
Prevent secret propagation, cross-tenant destroy, externally managed resource takeover, and broad delete on unsupported families.

## Acceptance
Irreversible resources denied, unauthorized actor denied, mismatch owner generation denied, changed state denied, PendingApproval preserved, failure-after-dispatch reconciled, audit present.

**No destructive API is implemented by this document.**
