#!/usr/bin/env bash
set -euo pipefail

: "${BASE_SHA:?BASE_SHA is required}"

mapfile -t commits < <(git rev-list "${BASE_SHA}..HEAD")
if [[ ${#commits[@]} -eq 0 ]]; then
  echo "No PR commits found." >&2
  exit 1
fi

pattern='^(feat|fix|refactor|perf|test|docs|build|ci|chore|security)(\([a-z0-9._/-]+\))?(!)?: .+'

for commit in "${commits[@]}"; do
  subject="$(git show -s --format=%s "$commit")"
  if [[ ! "$subject" =~ $pattern ]]; then
    echo "Commit $commit has a non-conventional subject: $subject" >&2
    exit 1
  fi

  if ! git show -s --format=%B "$commit" | grep -Eiq '^Signed-off-by: .+ <[^>]+>$'; then
    echo "Commit $commit is missing a DCO Signed-off-by trailer." >&2
    exit 1
  fi
done

echo "Conventional Commit and DCO validation passed for ${#commits[@]} commit(s)."
