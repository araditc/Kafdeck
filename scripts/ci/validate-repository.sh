#!/usr/bin/env bash
set -euo pipefail

required=(
  README.md LICENSE CONTRIBUTING.md SECURITY.md GOVERNANCE.md SUPPORT.md MAINTAINERS.md ROADMAP.md
  .github/CODEOWNERS .github/PULL_REQUEST_TEMPLATE.md .github/dependabot.yml
  docs/architecture/architecture.md docs/architecture/security.md docs/architecture/threat-model.md
  docs/development/engineering-standards.md docs/development/testing-standards.md docs/development/git-workflow.md
)

for path in "${required[@]}"; do
  if [[ ! -f "$path" ]]; then
    echo "Required repository file is missing: $path" >&2
    exit 1
  fi
done

# Third-party GitHub Actions must be immutable-SHA pinned. Local actions are exempt.
while IFS= read -r line; do
  ref="${line#*@}"
  ref="${ref%%[[:space:]#]*}"
  if [[ ! "$ref" =~ ^[0-9a-f]{40}$ ]]; then
    echo "Unpinned GitHub Action reference: $line" >&2
    exit 1
  fi
done < <(grep -RhoE 'uses:[[:space:]]+[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+(/[^@[:space:]]+)?@[A-Za-z0-9_.-]+' .github/workflows || true)

echo "Repository policy validation passed."
