#!/usr/bin/env bash
set -euo pipefail

mapfile -t projects < <(find src tests -type f -name '*.csproj' -print 2>/dev/null | sort)
if [[ ${#projects[@]} -eq 0 ]]; then
  echo "No backend projects found; backend CI is not active yet."
  exit 0
fi

solution="$(find . -maxdepth 2 -type f \( -name '*.sln' -o -name '*.slnx' \) -print -quit)"
if [[ -z "$solution" ]]; then
  echo "Backend projects exist but no solution file was found." >&2
  exit 1
fi

mapfile -t locks < <(find src tests -type f -name 'packages.lock.json' -print 2>/dev/null | sort)
if [[ ${#locks[@]} -lt ${#projects[@]} ]]; then
  echo "Bootstrap mode: generating NuGet lock files for the new W01 scaffold."
  dotnet restore "$solution" --use-lock-file
  echo "BEGIN_GENERATED_NUGET_LOCKFILES"
  while IFS= read -r lock; do
    echo "===== $lock ====="
    cat "$lock"
  done < <(find src tests -type f -name 'packages.lock.json' -print | sort)
  echo "END_GENERATED_NUGET_LOCKFILES"
else
  dotnet restore "$solution" --locked-mode
fi

dotnet build "$solution" --configuration Release --no-restore -warnaserror
dotnet test "$solution" --configuration Release --no-build --no-restore
