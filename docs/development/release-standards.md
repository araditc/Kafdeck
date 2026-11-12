# Release standards

## Versioning and accepted source

Before v1.0, releases may evolve rapidly but must document breaking changes. From v1.0 onward, Kafdeck follows Semantic Versioning. A release requires an accepted protected-main product commit and successful applicable checks. Signing and official publication require separate explicit owner decisions for concrete source/artifact identities.

## Current distribution

The [official v0.9 release](https://github.com/araditc/Kafdeck/releases/tag/v0.9) includes:

- immutable source tag and release notes;
- API/UI, CLI and MCP native archives for the five tested RIDs;
- amd64/arm64 OCI media, a two-architecture registry index and Helm chart;
- independently versioned Terraform 0.8.0 packages and MCP 0.9.0 package;
- SBOMs, scan reports, source/platform acceptance and build provenance;
- the signed full-distribution manifest, Sigstore bundle, trusted-root material, checksums and offline mirror;
- accepted W80/W81 evidence and the final W82 readiness decision.

The [published record](../implementation/evidence/v0.9-release-published-20261112.json) binds actual release assets, source and digests. GHCR retains its private access policy. Terraform packages in GitHub Releases do not establish public Terraform Registry availability. Component versions and versioned API contracts do not automatically advance with the application version.

## Maintainer release process

Use [prepublication signing](../releases/v0.9-prepublication-signing.md), [readiness](../releases/v0.9-release-readiness.md) and the [publication driver record](../../publication/v09/README.md) as the historical v0.9 process. The product source and separately locked publisher revision are distinct identities; transport does not rebuild signed media. `.github/release/release.json` is an immutable preparation snapshot: its `candidate` status and v0.8 published baseline describe build-time inputs, not the current published version. Current status comes from GitHub Releases and the final publication record. Do not rewrite historical input metadata or move a released tag when main advances.

Required failing CI, unresolved critical security findings affecting the release, failed supported compatibility/upgrade checks, missing mandatory evidence, or invalid signatures/provenance block release. Successful source-bound installations, load/soak tests and signatures are reused according to the admission contract; no repeated benchmark is needed merely because documentation changes.

## Operator verification and recovery

Operators should follow [installation and signed-media verification](../operator/v0.9-installation-guide.md) and [upgrade/backup/restore](../operations/v0.9-distribution-recovery.md), not dispatch maintainer publication workflows. Verify the manifest signature, approved signer/root and all selected media hashes before installation. Scope is the tested platform/provider cell; cross-compilation does not prove runtime acceptance. Keep the actual deployment's egress test and retained KAF-02 condition.

## Supply-chain limits

Least-privilege permissions, full-SHA actions, locked dependency graphs, protected owner admission and source-bound CI provenance form the current baseline. They are not a claim of fully hermetic, independently reproducible builds or an unqualified higher SLSA level. Stronger guarantees require their own demonstrated evidence and governed admission. A source or dependency change needs applicable new checks; unchanged successful historical evidence must not be relabeled as a new run.
