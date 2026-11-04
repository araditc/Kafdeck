#!/usr/bin/env bash
# Source this wrapper. Only immutable official tooling is executed; version is recorded.
skopeo_pinned() {
  local pin config args
  pin="$(jq -r .skopeoImage .github/release/publication-tooling.json)"
  [[ "$pin" =~ ^quay.io/skopeo/stable@sha256:[0-9a-f]{64}$ ]]
  args=(run --rm --network host --user "$(id -u):$(id -g)" --volume "$PWD:/work" --workdir /work --env HOME=/tmp --entrypoint /usr/bin/skopeo)
  config="${DOCKER_CONFIG:-$HOME/.docker}/config.json"
  if [[ -f "$config" ]]; then args+=(--volume "$config:/auth/config.json:ro" --env REGISTRY_AUTH_FILE=/auth/config.json); fi
  docker "${args[@]}" "$pin" "$@"
}
