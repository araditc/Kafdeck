# Dependency Policy

## Goals

Dependencies must have a clear product/engineering purpose, compatible licensing, an acceptable maintenance/security posture, and deterministic versioning.

## Runtime dependency review

Every new runtime dependency requires the PR to state:

- why it is needed,
- why an existing platform/library capability is insufficient,
- license,
- maintenance/security status,
- transitive-risk considerations,
- whether it affects air-gapped operation or artifact size.

## License posture

Normally acceptable: Apache-2.0, MIT, BSD-family, ISC and similarly permissive licenses.

Requires explicit review: MPL/LGPL and licenses with file-level/weak-copyleft or unusual obligations.

Not accepted without explicit project-owner approval: GPL/AGPL strong-copyleft dependencies, non-commercial restrictions, source-available licenses presented as open source, unknown/custom licenses, or dependencies with incompatible redistribution terms.

## Versioning and lock files

- NuGet dependencies must participate in deterministic restore and a committed lock strategy once the backend scaffold is added.
- Frontend dependencies require a committed lockfile and reproducible install command.
- Container base images use explicit version tags; release hardening may additionally pin digests.
- GitHub Actions use immutable 40-character commit SHAs.

## Update automation

Dependabot initially tracks GitHub Actions. NuGet and frontend package ecosystems are enabled in the same PR that introduces their authoritative manifests so update jobs never point at nonexistent package roots.
