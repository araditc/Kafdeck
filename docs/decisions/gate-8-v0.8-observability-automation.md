# Gate 8 — v0.8 Observability, Automation & Platform APIs Scope

- **Date:** 2026-09-27
- **Status:** SCOPE ACCEPTED; W61–W71 COMPLETE; v0.8 PUBLISHED (delivery update 2026-11-05)
- **Authority:** Project Owner, Ammar Heidari
- **Scope issue:** #208
- **Tracker:** #209
- **Planning workstream:** #210 / W61
- **RFC:** RFC-0008 planning package

## Owner approval

> **v0.8 scope در Issue #208 طبق متن فعلی تأیید است؛ W61 governed planning package را شروع کن. implementation فقط بعد از protected-main planning admission مجاز است.**

## Accepted scope

v0.8 is **Observability, Automation & Platform APIs**.

Accepted workstreams are W61–W71 as recorded in #209.

## Accepted hard boundaries

1. Telemetry is bounded/cardinality-controlled and excludes secrets/raw protected payloads.
2. Historical metrics use an explicit provider; Kafka is not silently Kafdeck's metrics DB.
3. Data-quality payload monitoring is opt-in, separately authorized and bounded.
4. Notifier endpoints are configured typed profiles; no arbitrary outbound HTTP proxy.
5. CLI/GitOps/Terraform/MCP use the same versioned governed backend.
6. Automation has explicit principals and current effect-time revalidation.
7. CRITICAL effects retain distinct-principal approval.
8. Declarative ownership/import/adoption is explicit.
9. MCP model/prompt output is never authorization or approval.
10. No shell/arbitrary SQL/provider HTTP/raw Kafka/DB automation tool.
11. Compensation exists only for explicit safe inverses and is a fresh governed mutation.
12. Irreversible effects are never advertised as reversible.
13. No mandatory public SaaS/data-plane proxy/public runtime plugins.
14. v0.9 governance/cost-attribution/self-service is not pulled forward.
15. Release publication remains separately owner-gated.

## Planning authorization

W61 may define detailed contracts, numerical ceilings, provider/client matrices, APIs, risk, threats, tests and dependencies without activating product code.

## Implementation gate

The original rule below was satisfied by W61 PR #221 / protected main `f206c31eed3c5c87bf40ec308520c48ac422898c`; W62–W71 subsequently completed. Before implementation, W61 had to receive exact-head CI, substantive architecture/security review, fresh CODEOWNER approval, zero unresolved required threads, expected-head guarded protected-main merge and post-merge tracker activation.

## Release gate

Planning admission and implementation completion do not themselves authorize v0.8 publication.

## Delivery and separate publication completion

G5 and tracker #209/#220 closed at protected main `cbedd025ab97db10fb21407ea5188c08d6a4d8ce`. The owner explicitly approved publication of that source and archive SHA256 `6c333ee26e8d433ea49faf7d578238b9db5577656959c31404ce032fbad6bd34` on 2026-11-05. [Publication run41986146327](https://github.com/araditc/Kafdeck/actions/runs/41986146327) created immutable tag/Release v0.8 and verified signed GHCR digest `sha256:7b64f67b7f050b3c8cb5c2678b6ce94f6886e854cebfca6d6730170164fedb45`, with six checked package assets. GHCR remains private. See [accepted v0.8 release evidence](../implementation/evidence/v0.8-g5-accepted-20261105.json). This record does not approve detailed v0.9 scope or implementation.
