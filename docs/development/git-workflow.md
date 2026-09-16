# Git and Pull Request Workflow

## Branch model

Kafdeck uses trunk-based development through short-lived branches.

Allowed branch patterns include:
- `feature/*`
- `fix/*`
- `docs/*`
- `refactor/*`
- `security/*`
- `chore/*`

`main` must remain releasable.

## Direct pushes

Direct pushes to `main` are prohibited after the repository bootstrap commit. Repository rules should enforce this technically.

The initial `README.md` commit is a documented one-time bootstrap exception because the repository was created without any commit/branch object and a first commit was required before a PR branch could exist.

## Pull requests

A PR should be focused and describe:
- Why
- What
- Architecture impact
- Security impact
- Breaking/compatibility impact
- Testing evidence
- Documentation impact
- Migration impact

Sections may be marked Not Applicable with a reason.

## Review requirements

- No unresolved required review threads.
- Architecture-sensitive changes require architecture review.
- Security-sensitive changes require security review.
- Critical governance decisions require the defined approval gate.

## Exact-head validation

The commit SHA reviewed and validated must be the commit SHA merged. If the PR head changes, required checks/reviews must be refreshed according to repository rules.

## Merge strategy

Default: squash merge.

The final squash title follows Conventional Commits when practical.

## Commit convention

Use Conventional Commits:
- `feat:`
- `fix:`
- `refactor:`
- `perf:`
- `test:`
- `docs:`
- `build:`
- `ci:`
- `chore:`
- `security:`

## Merge gate checklist

Before merge:
- exact head identified,
- required CI green,
- tests green,
- security checks green,
- documentation updated,
- required approvals present,
- no unresolved required review thread,
- no unreviewed head movement,
- migrations/compatibility implications accepted.
