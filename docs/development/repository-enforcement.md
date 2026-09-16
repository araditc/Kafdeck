# Repository Enforcement Baseline

This document defines the GitHub repository controls that implement the approved governance baseline.

## Main branch ruleset

Target: `main`.

Required controls:

- block branch deletion and non-fast-forward updates,
- require changes through pull requests,
- require at least one approving review,
- dismiss stale approvals when new commits are pushed,
- require review conversation resolution,
- require Code Owner review for governed/security-sensitive paths when GitHub plan/settings support it,
- require the `quality-gate` status check,
- require the `dependency-review` status check for dependency-changing PRs when supported by the ruleset configuration,
- prevent bypass for normal contributors; emergency/admin bypass must be explicit and auditable,
- require branches to be up to date before merge once the CI set is stable,
- block force pushes.

`main` must remain releasable. Direct pushes are prohibited after the repository-bootstrap exception recorded in the approval log.

## Merge policy

- Squash merge is the default.
- The PR exact head SHA must be validated immediately before merge.
- Unresolved review threads block merge.
- Required checks must be green on the exact head.
- Security-sensitive changes follow the additional review rules in `SECURITY.md` and the architecture/security documents.

## Required repository settings

- Issues enabled.
- Actions enabled with least-privilege workflow permissions.
- Default `GITHUB_TOKEN` permission should be read-only unless a workflow explicitly requires more.
- Secret scanning, push protection, dependency graph, Dependabot alerts and private vulnerability reporting should be enabled where the GitHub organization/plan supports them.
- Auto-merge may be enabled after required checks and rulesets are stable.

## Connector limitation

Repository rulesets/branch-protection are GitHub administrative settings. If the automation identity does not expose an administrative write action, the file-based controls can be installed automatically but the GitHub ruleset must be enabled through repository settings by an authorized administrator. That condition is tracked as a Phase 0 infrastructure gate, not silently ignored.
