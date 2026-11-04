#!/usr/bin/env bash
set -euo pipefail
source scripts/release/assert-v08-release-owner.sh
root=artifacts/v08-publication-selected
selection="$root/publication-selection.json"
[[ "$GITHUB_EVENT_NAME" == workflow_dispatch && "$GITHUB_REF" == refs/heads/main ]]
[[ "$SOURCE_SHA" =~ ^[0-9a-f]{40}$ && "$SOURCE_SHA" == "$GITHUB_SHA" ]]
[[ "$ARCHIVE_SHA256" =~ ^[0-9a-f]{64}$ ]]
[[ "$OWNER_APPROVAL" == "PUBLISH v0.8 FROM $SOURCE_SHA" ]]
[[ "$(jq -r .sourceSha "$selection")" == "$SOURCE_SHA" ]]
[[ "$(jq -r .ownerApproval "$selection")" == "$OWNER_APPROVAL" ]]
[[ "$(jq -r .version "$selection")" == v0.8 && "$(jq -r .published "$selection")" == false ]]
[[ "$(jq -r .archiveSha256 "$selection")" == "$ARCHIVE_SHA256" ]]
[[ "$(gh api "repos/$GITHUB_REPOSITORY/git/ref/heads/main" --jq .object.sha)" == "$SOURCE_SHA" ]]
[[ "$(git rev-parse HEAD)" == "$SOURCE_SHA" ]]
printf '%s  %s\n' "$ARCHIVE_SHA256" "$root/candidate.tar.gz" | sha256sum --check
printf '%s  %s\n' "$(jq -r .sbomSha256 "$selection")" "$root/sbom.spdx.json" | sha256sum --check
printf '%s  %s\n' "$(jq -r .packetSha256 "$selection")" "$root/v0.8-readiness-packet.json" | sha256sum --check
[[ "$(jq -r .sourceSha "$root/candidate-provenance.json")" == "$SOURCE_SHA" ]]
image_id="$(jq -r .imageId "$selection")"
[[ "$image_id" =~ ^sha256:[0-9a-f]{64}$ ]]
[[ "$(jq -r .imageId "$root/candidate-provenance.json")" == "$image_id" ]]
[[ "$(jq -r .imageArchiveSha256 "$root/candidate-provenance.json")" == "$ARCHIVE_SHA256" ]]
[[ "$(jq -r .sourceSha "$root/v0.8-readiness-packet.json")" == "$SOURCE_SHA" ]]
[[ "$(jq -r .status "$root/v0.8-readiness-packet.json")" == EvidenceCompleteAwaitingOwnerPublicationDecision ]]
(cd "$root/release-assets" && sha256sum --check SHA256SUMS)
printf '%s  %s\n' "$(jq -r .assetsManifestSha256 "$selection")" "$root/release-assets/SHA256SUMS" | sha256sum --check
docker image load --input "$root/candidate.tar.gz"
[[ "$(docker image inspect --format '{{.Id}}' "$image_id")" == "$image_id" ]]
docker image tag "$image_id" "kafdeck:owner-verified-$SOURCE_SHA"
printf 'CANDIDATE_IMAGE_ID=%s\nCANDIDATE_SCAN_REF=%s\n' "$image_id" "kafdeck:owner-verified-$SOURCE_SHA" >> "$GITHUB_ENV"
printf 'Verified candidate loaded by exact image ID; no rebuild or registry write.\n'
