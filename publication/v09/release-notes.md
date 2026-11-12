# Kafdeck v0.9

Source: `01c7553425fbc61ba4a7a21918265c562086ba02`.

This release includes the accepted v0.9 governance, catalog, policy, requests and
measured-attribution capabilities, dashboard workflows, hardening/accessibility,
native distribution and upgrade/recovery work. The exact release decision and
public W80 evidence are attached with package checksums. Synthetic migration
approval fixtures are identified separately from genuine authenticated reads.

The application version is `0.9.0`; the independently versioned Terraform provider
retains `0.8.0`. Native API/UI, CLI and MCP packages cover Windows x64, Linux x64
and arm64, and macOS x64 and arm64. The image index covers Linux amd64 and arm64.

## Verify the offline mirror and image

The original official CI signed offline mirror ZIP is retained unchanged. Verify
the GitHub-OIDC signature of `manifest.json` using its Sigstore bundle, independently
reviewed trusted root, expected repository/workflow identity and source SHA. Then
verify every selected package/archive hash. The manifest SHA256 is
`b63864259a8a5e1c3c9d8dc3312d35a56e82749d62e2f6a05b88112f98af29ba`;
the reviewed root SHA256 is
`0ba58f6c09c271f664f0f8c1fa62e2065844ca89a0397c0178ed5188f6096724`.

Registry transport uses a reviewed, no-build conversion of those signed OCI
archives. Exact image config bytes and each uncompressed layer diff ID remain
bound to the signed archive/config; the publication selection records converted
manifest/compressed-layer hashes and the final two-platform index digest. Remote
readback verifies index, config and layer bytes. The original official full-manifest
signature remains under the main01c identity. A separately owner-adopted protected
publisher workflow signs the registry index and both architecture manifests and
attests readiness/SPDX. Its identity is
`https://github.com/araditc/Kafdeck/.github/workflows/v09-publication.yml@refs/heads/codex/v09-publication`;
verification must additionally pin the reviewed publisher commit recorded in the
publication selection. This new publisher identity is not the original main signer.

GHCR remains private and requires authorized read access. Publication writes only
content-addressed application image destinations; Cosign appends OCI-referrer proof
metadata. Existing application version/latest registry tags are preserved.
Git tag `v0.9` resolves to the exact accepted source; foreign tags/assets are never
overwritten. Release assets preserve original CI package bytes, native archives,
Terraform/MCP, Helm, SBOM/scan evidence, signed metadata and full offline mirror.
