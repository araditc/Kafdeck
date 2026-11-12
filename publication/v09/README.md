# Guarded v0.9 publication driver

This branch contains a reviewed transport/publisher, not a new product build.
Accepted product source remains `01c7553425fbc61ba4a7a21918265c562086ba02` and the
official signed manifest remains `b63864259a8a5e1c3c9d8dc3312d35a56e82749d62e2f6a05b88112f98af29ba`.
The frozen public W80 proposal is `5e86572f5983aca73cbdd28136194267db6ef323c8e95907d4fdeb45f7a1b01f`;
bundling it does not assert owner adoption before the explicit decision.

`v09-publication` push/manual preparation jobs have contents/actions/packages read
permission. They fetch the exact successful official signed artifact, verify its
ZIP digest, Sigstore bundle and expected identity/source/root, verify all media,
and use the immutable main's pinned Skopeo container to convert two Docker archives
to OCI without a build. Config bytes and every uncompressed layer diff ID are
checked against the signed archive/config. A canonical two-architecture index and
selection list record exact converted manifests/compressed blobs and release asset
hashes. A local-only pinned registry proves digest transport, remote bytes and
foreign-tag preservation. Unit tests prove actor/protection/approval/tamper rejection.

The original official signed CI ZIP is the full offline mirror. Every individual
CI asset is preserved byte-for-byte. A deterministic W80 public-evidence ZIP
preserves the original packet-relative paths; its SHA256 is in the selection.
The packet, public README/scope, 29 scripts and 23 evidence files were explicitly
curated by the root agent; no private database, credential or raw secret is copied.

## Before a publication decision

1. Review this exact driver commit and the successful read-only preparation/local
   transport proof. Root must protect/lock `codex/v09-publication` at that reviewed
   commit. The runtime checks the actual GitHub branch protection and SHA.
2. Run read-only preparation again at the protected commit. Review the exact
   `publication-selection.json` SHA256, source/manifest/W80 bindings, index and
   platform digests, asset list and public-evidence ZIP. Preparation always reports
   `releaseReady: false` and makes no external publication.
3. Obtain one explicit owner decision adopting the exact W80 proposal and
   authorizing publication of this exact selection/driver **conditional on** an
   actual successful matching protected-main readiness run. No future readiness
   hash is predicted or manufactured. The exact publication input phrase is:

   ```text
   PUBLISH v0.9 SOURCE 01c7553425fbc61ba4a7a21918265c562086ba02 MANIFEST b63864259a8a5e1c3c9d8dc3312d35a56e82749d62e2f6a05b88112f98af29ba W80 5e86572f5983aca73cbdd28136194267db6ef323c8e95907d4fdeb45f7a1b01f PUBLISHER <locked-driver-sha> SELECTION <reviewed-selection-sha256> AND SIGN REGISTRY INDEX AND ARCHITECTURES WITH LOCKED PUBLISHER IDENTITY ONLY AFTER VERIFIED MAIN READINESS
   ```

The final manual dispatch supplies that phrase, `publisher_commit`,
`selection_sha256` and the real `readiness_run_id`. Both environment actors and
the original/triggering actors from the actual GitHub run must be immutable
release owners. The write job first refuses an unprotected/advanced publisher,
wrong decision or context, then deterministically reproduces the approved
selection. Actual main must still be protected at the accepted source.

## Conditional publication behavior

Only the explicit final dispatch can reach the read-only admission job. The
contents/packages/id-token-write publication job starts only after that job
successfully checks actual owner/branch protection/SHA/decision and readiness.
The actual successful readiness producer must be the
official protected-main workflow, on the exact source, dispatched by owners, with
`releaseReady: true`, no missing gate, and the exact signed manifest/root/W80
packet hash. Its immutable artifact is downloaded and hash-checked. The approved
method conditionally adds only that genuine decision JSON/checksum to release
assets; the final SHA256SUMS covers all fixed and conditional assets.

Publication writes the two-platform index by digest with `--preserve-digests`,
reads all remote configs/layers back, and checks registry tags/privacy unchanged.
No version/latest/sha image tag is written. Tag `v0.9` is created only at accepted
source; existing foreign/ambiguous tags or release assets fail without overwrite.
A matching release's immutable tags/assets retain non-clobber checks. A pre-existing
reserved proof-discovery tag, including a bare `sha256-<digest>` runtime image or
previous metadata, is never inferred to be owned: publication fails **before any
registry mutation**. Automatic resume after partial proof writes is blocked if
such reserved tags remain; explicit verification/cleanup and a separate reviewed
decision are required. New release metadata starts as a
draft; every uploaded asset is independently downloaded and hashed before the
draft is published. External writes are never attempted by read-only preparation.

The original main01c offline-manifest signature stays unchanged. The separately
adopted locked publisher identity signs the exact converted index and both
architecture manifests, attests the actual readiness decision to the index and
the original SPDX documents to their architecture images. Remote verification
pins the publisher workflow identity, branch and reviewed commit H; it also checks
attestation predicate contents and subject digests. This is explicitly the new
publisher identity, not the old protected-main signing identity. Public verification
proofs are retained in the publication result artifact. Cosign uses new-bundle OCI
referrers; no application version/latest tag is written. GHCR remains private.
No accepted offline media is rebuilt or re-signed and protected main is not advanced.

The pinned upstream [sign](https://github.com/sigstore/cosign/blob/v3.0.6/doc/cosign_sign.md),
[attest](https://github.com/sigstore/cosign/blob/v3.0.6/doc/cosign_attest.md) and
[verify](https://github.com/sigstore/cosign/blob/v3.0.6/doc/cosign_verify.md) flag
contracts define this narrowly authorized registry-signing operation. The existing
manifest-signing authorization alone does not permit these new writes.

## Frozen supplementary W81 review evidence

The original 45 fixed asset entries must remain byte-identical to the successful
preparation baseline in `fixed-assets.json`; W80 packet `5e865...` is unchanged.
The final curated W81 `w81-public/index.json` binds the same application source and
signed manifest and inventories every public raw file by relative path/SHA256.
Missing, changed, symlinked or unlisted evidence fails closed. A deterministic
W81 ZIP and exact index JSON are additional `reviewAssets`; their hashes are bound
in `reviewEvidence.w81` and the owner-reviewed selection. They do not alter any
product package or pretend to change the immutable main readiness schema.

The bounded `local-proof` job tests the reserved runtime-image collision and
Cosign3.0.6 sign/attest/verify with an ephemeral local key in a loopback-only
namespace. No OIDC, tlog upload, external registry or product image is involved.
Its private-infrastructure/tlog opt-outs are confined to that fixture; production
verification retains all issuer/identity/commit/transparency constraints. Heavy
mirror preparation runs only on manual dispatch, after final review/protection.
