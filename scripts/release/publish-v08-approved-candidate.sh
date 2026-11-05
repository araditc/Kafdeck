#!/usr/bin/env bash
set -euo pipefail
source scripts/release/load-v08-publication-candidate.sh
source scripts/release/skopeo-v08-tool.sh
root=artifacts/v08-publication-selected
image="$(jq -r .image "$root/publication-selection.json")"
[[ "$image" == "ghcr.io/${GITHUB_REPOSITORY,,}" ]]
[[ "$(jq -r .version .github/release/release.json)" == v0.8 && "$(jq -r .status .github/release/release.json)" == candidate ]]
version=v0.8
scratch="$(mktemp -d)"; trap 'rm -rf "$scratch"' EXIT
check_source_tag() {
  source_tag_exists=false
  if gh api "repos/$GITHUB_REPOSITORY/git/ref/tags/$version" > "$scratch/git-tag.json" 2> "$scratch/git-error"; then
    source_tag_exists=true
    [[ "$(jq -r .object.type "$scratch/git-tag.json")" == commit && "$(jq -r .object.sha "$scratch/git-tag.json")" == "$SOURCE_SHA" ]] || { echo 'Existing source tag differs or is ambiguous; no retarget permitted.' >&2; exit 1; }
  elif ! grep -q 'HTTP 404' "$scratch/git-error"; then echo 'Cannot verify source tag ownership.' >&2; exit 1; fi
}
check_source_tag
release_exists=false
if gh api "repos/$GITHUB_REPOSITORY/releases/tags/$version" > "$scratch/release.json" 2> "$scratch/release-error"; then
  [[ "$source_tag_exists" == true && "$(jq -r .tag_name "$scratch/release.json")" == "$version" ]]
  release_exists=true
elif ! grep -q 'HTTP 404' "$scratch/release-error"; then echo 'Cannot verify release ownership.' >&2; exit 1; fi
layout=.artifacts/v08-digest-publication
bash scripts/release/convert-v08-publication-image.sh "$root/candidate.tar.gz" "$image_id" "$layout"
digest="$(cat "$layout/manifest-digest.txt")"; [[ "$digest" =~ ^sha256:[0-9a-f]{64}$ ]]
[[ "$(gh api "repos/$GITHUB_REPOSITORY/git/ref/heads/main" --jq .object.sha)" == "$SOURCE_SHA" ]]
printf '%s' "$REGISTRY_TOKEN" | docker login ghcr.io --username "$GITHUB_ACTOR" --password-stdin
# A digest destination is content-addressed. No mutable image tag is written.
skopeo_pinned copy --preserve-digests "dir:/work/$layout/image" "docker://$image@$digest"
[[ "$(skopeo_pinned inspect --format '{{.Digest}}' "docker://$image@$digest")" == "$digest" ]]
skopeo_pinned inspect --raw "docker://$image@$digest" > "$scratch/remote-manifest.json"
[[ "$(jq -r .config.digest "$scratch/remote-manifest.json")" == "$image_id" ]]
cosign sign --yes --new-bundle-format=true "$image@$digest"
cosign attest --yes --new-bundle-format=true --type https://kafdeck.io/attestations/main-readiness/v1 --predicate "$root/publication-selection.json" "$image@$digest"
cosign attest --yes --new-bundle-format=true --type spdxjson --predicate "$root/sbom.spdx.json" "$image@$digest"
identity="$GITHUB_SERVER_URL/$GITHUB_REPOSITORY/.github/workflows/publish-release.yml@refs/heads/main"
cosign verify --certificate-identity "$identity" --certificate-oidc-issuer https://token.actions.githubusercontent.com "$image@$digest" > "$root/cosign-signature.json"
cosign verify-attestation --type https://kafdeck.io/attestations/main-readiness/v1 --certificate-identity "$identity" --certificate-oidc-issuer https://token.actions.githubusercontent.com "$image@$digest" > "$root/cosign-readiness.json"
cosign verify-attestation --type spdxjson --certificate-identity "$identity" --certificate-oidc-issuer https://token.actions.githubusercontent.com "$image@$digest" > "$root/cosign-sbom.json"
check_source_tag
[[ "$(gh api "repos/$GITHUB_REPOSITORY/git/ref/heads/main" --jq .object.sha)" == "$SOURCE_SHA" ]]
if [[ "$source_tag_exists" == false ]]; then
  gh api --method POST "repos/$GITHUB_REPOSITORY/git/refs" -f "ref=refs/tags/$version" -f "sha=$SOURCE_SHA" > "$scratch/created-tag.json"
  check_source_tag; [[ "$source_tag_exists" == true ]]
fi
if [[ "$release_exists" == false ]]; then
  notes="$(jq -r .notes .github/release/release.json)"; [[ "$notes" == docs/releases/v0.8.md && -f "$notes" ]]
  cp "$notes" "$scratch/release-notes.md"
  {
    printf '\n## Owner-authorized immutable publication\n\nSource: `%s`\n\nImage: `%s@%s`\n\n' "$SOURCE_SHA" "$image" "$digest"
    printf 'Archive SHA256: `%s`. Registry publication is digest-only; no mutable version/latest/sha image tags are created.\n' "$ARCHIVE_SHA256"
  } >> "$scratch/release-notes.md"
  gh release create "$version" --repo "$GITHUB_REPOSITORY" --verify-tag --title "$(jq -r .title .github/release/release.json)" --notes-file "$scratch/release-notes.md"
fi
# Upload approved package bytes without --clobber; existing assets must match.
while IFS= read -r asset; do
  [[ "$asset" =~ ^[A-Za-z0-9_.-]+$ ]]
  expected="$(jq -r --arg file "$asset" '.assets[]|select(.file==$file)|.sha256' "$root/publication-selection.json")"
  [[ "$expected" =~ ^[0-9a-f]{64}$ ]]
  gh api "repos/$GITHUB_REPOSITORY/releases/tags/$version" > "$scratch/release-assets.json"
  if jq -e --arg name "$asset" '.assets[]|select(.name==$name)' "$scratch/release-assets.json" >/dev/null; then
    mkdir -p "$scratch/existing"
    gh release download "$version" --repo "$GITHUB_REPOSITORY" --pattern "$asset" --dir "$scratch/existing"
    printf '%s  %s\n' "$expected" "$scratch/existing/$asset" | sha256sum --check
  else
    check_source_tag; [[ "$(gh api "repos/$GITHUB_REPOSITORY/git/ref/heads/main" --jq .object.sha)" == "$SOURCE_SHA" ]]
    gh release upload "$version" --repo "$GITHUB_REPOSITORY" "$root/release-assets/$asset"
  fi
done < <(jq -r '.assets[].file' "$root/publication-selection.json")
# The checksum list is itself bound by the selection and uploaded without overwrite.
asset=SHA256SUMS
if jq -e --arg name "$asset" '.assets[]|select(.name==$name)' "$scratch/release-assets.json" >/dev/null; then
  gh release download "$version" --repo "$GITHUB_REPOSITORY" --pattern "$asset" --dir "$scratch/existing"
  printf '%s  %s\n' "$(jq -r .assetsManifestSha256 "$root/publication-selection.json")" "$scratch/existing/$asset" | sha256sum --check
else
  gh release upload "$version" --repo "$GITHUB_REPOSITORY" "$root/release-assets/$asset"
fi
check_source_tag; [[ "$source_tag_exists" == true ]]
jq -n --arg source "$SOURCE_SHA" --arg image "$image" --arg digest "$digest" --arg config "$image_id" --arg archive "$ARCHIVE_SHA256" --arg tool "$(cat "$layout/skopeo-version.txt")" \
  '{schemaVersion:1,sourceSha:$source,image:$image,digest:$digest,imageId:$config,archiveSha256:$archive,skopeoVersion:$tool,published:true,digestOnly:true,noMutableImageTags:true,noCandidateRebuild:true}' > "$root/publication-result.json"
