#!/usr/bin/env bash
set -euo pipefail

mapfile -t projects < <(find src tests -type f -name '*.csproj' -print 2>/dev/null | sort)
if [[ ${#projects[@]} -eq 0 ]]; then
  echo "No backend projects found; backend CI is not active yet."
  exit 0
fi

solution="$(find . -maxdepth 2 -type f \( -name '*.sln' -o -name '*.slnx' \) -print -quit)"
if [[ -n "$solution" ]]; then
  dotnet restore "$solution" --locked-mode
  dotnet build "$solution" --configuration Release --no-restore -warnaserror
  dotnet test "$solution" --configuration Release --no-build --no-restore
else
  echo "Backend projects exist but no solution file was found." >&2
  exit 1
fi
