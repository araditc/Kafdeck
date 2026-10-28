# W69 — Governed plan / apply contract

Status: **candidate / W68 API dependency**.

Terraform plan is advisory and immutable, bound to deployment, resource identity, desired SHA256, observed revision, source generation and actor principal. Apply must call Kafdeck's authorized versioned Apply API; must not synthesize direct Kafka effects.
A response of PendingApproval is durable pending state, not success. Return the operation reference as a diagnostic/observable pending result, never forge approval. Expired/stale plans require fresh read and re-plan. A service principal may perform only explicitly delegated permissions; an approver must be distinct where required by policy.

## Negative tests required
Replayed plan, changed revision, stolen operation ID, wrong principal, changed tenant, missing approvals, unknown risk classification, timeout after uncertain effect, idempotent retries, lost acknowledgements.

The provider must not automatically create or approve requests that require human approval.

**Provider apply implementation cannot be admitted until W68 governed lifecycle is available.**
