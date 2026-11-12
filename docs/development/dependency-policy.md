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

- NuGet dependencies use committed `packages.lock.json` files and `dotnet restore Kafdeck.slnx --locked-mode`; native RID-specific locks remain explicit build inputs.
- Frontend dependencies use the committed `package-lock.json` and `npm ci`; Go uses the committed `go.mod`/`go.sum` and the pinned provider toolchain.
- Application build/runtime base images are digest-pinned in the [Dockerfile](../../Dockerfile). Changes must preserve reproducible image inputs; an upstream tag alone is not a release identity.
- GitHub Actions use immutable 40-character commit SHAs.

## Update automation

[Dependabot](../../.github/dependabot.yml) tracks GitHub Actions, NuGet, frontend npm and the Terraform provider Go modules. Related CodeQL actions, release supply-chain actions and React dependencies have explicit groups. Lockfile regeneration, relevant tests, scans and required review still apply to bot PRs; an automated update is not automatically admitted. Deferred updates are recorded in the [maintenance disposition](maintenance-pr-disposition-20261112.md).
