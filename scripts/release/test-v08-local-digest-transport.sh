#!/usr/bin/env bash
set -euo pipefail
root=artifacts/g6-local-transport
mkdir -p "$root/guards/bin" "$root/accepted" "$root/foreign"
# These adapters make any forbidden IO observable; owner rejection must precede it.
for tool in gh docker; do
  printf '#!/usr/bin/env bash\nprintf "forbidden IO\\n" >> "%s/forbidden-io.log"\nexit 98\n' "$PWD/$root" > "$root/guards/bin/$tool"
  chmod +x "$root/guards/bin/$tool"
done
for pair in 'nonowner ammarheidari' 'ammarheidari nonowner'; do
  read -r actor trigger <<< "$pair"
  if PATH="$PWD/$root/guards/bin:$PATH" GITHUB_ACTOR="$actor" GITHUB_TRIGGERING_ACTOR="$trigger" bash scripts/release/load-v08-publication-candidate.sh > "$root/guard.log" 2>&1; then echo 'Nonowner load accepted.' >&2; exit 1; fi
  if PATH="$PWD/$root/guards/bin:$PATH" GITHUB_ACTOR="$actor" GITHUB_TRIGGERING_ACTOR="$trigger" pwsh -NoProfile -File scripts/release/prepare-v08-publication.ps1 > "$root/prepare-guard.log" 2>&1; then echo 'Nonowner selection accepted.' >&2; exit 1; fi
  grep -Fq 'Release action requires an approved original and triggering owner identity.' "$root/guard.log"
  grep -Fq 'Original and triggering release actors must both be approved owners.' "$root/prepare-guard.log"
  [[ ! -f "$root/forbidden-io.log" ]]
done
GITHUB_ACTOR=ammarheidari GITHUB_TRIGGERING_ACTOR=akhiabanchian bash scripts/release/assert-v08-release-owner.sh
source scripts/release/skopeo-v08-tool.sh
registry_image="$(jq -r .localTestRegistryImage .github/release/publication-tooling.json)"
[[ "$registry_image" =~ ^registry:3\.0\.0@sha256:[0-9a-f]{64}$ ]]
name="kafdeck-g6-local-$GITHUB_RUN_ID"
cleanup() { docker rm -f "$name" >/dev/null 2>&1 || true; }
trap cleanup EXIT
docker run --detach --name "$name" --publish 127.0.0.1::5000 "$registry_image"
endpoint="$(docker port "$name" 5000/tcp | head -1)"; [[ "$endpoint" =~ ^127\.0\.0\.1:[0-9]+$ ]]
for attempt in $(seq 1 30); do if curl --fail --silent "http://$endpoint/v2/" >/dev/null; then break; fi; sleep 1; done
curl --fail --silent "http://$endpoint/v2/" >/dev/null
printf 'accepted image fixture\n' > "$root/accepted/payload"
printf 'foreign version tag fixture\n' > "$root/foreign/payload"
for kind in accepted foreign; do
  printf 'FROM scratch\nCOPY payload /payload\n' > "$root/$kind/Dockerfile"
  docker build --provenance=false --sbom=false --tag "kafdeck-g6-$kind:$GITHUB_RUN_ID" "$root/$kind"
done
archive="$root/candidate.tar.gz"
docker image save "kafdeck-g6-accepted:$GITHUB_RUN_ID" | gzip -n > "$archive"
# Docker's containerd store reports a manifest/index digest as .Id. The
# digest-only transport contract instead binds the fixture's config bytes.
config_entry="$(tar -xOzf "$archive" manifest.json | jq -er 'if length == 1 then .[0].Config else error("Expected one fixture image") end')"
[[ "$config_entry" =~ ^([0-9a-f]{64}\.json|blobs/sha256/[0-9a-f]{64})$ ]]
accepted_id="sha256:$(tar -xOzf "$archive" "$config_entry" | sha256sum | cut -d' ' -f1)"
[[ "$config_entry" == "${accepted_id#sha256:}.json" || "$config_entry" == "blobs/sha256/${accepted_id#sha256:}" ]]
bash scripts/release/convert-v08-publication-image.sh "$archive" "$accepted_id" "$root/layout"
digest="$(cat "$root/layout/manifest-digest.txt")"
repo="$endpoint/fixture"
docker image tag "kafdeck-g6-foreign:$GITHUB_RUN_ID" "$repo:v0.8"
docker image push "$repo:v0.8"
foreign_before="$(skopeo_pinned inspect --tls-verify=false --format '{{.Digest}}' "docker://$repo:v0.8")"
tags_before="$(curl --fail --silent "http://$endpoint/v2/fixture/tags/list" | jq -c '.tags|sort')"
skopeo_pinned copy --dest-tls-verify=false --preserve-digests "dir:/work/$root/layout/image" "docker://$repo@$digest"
[[ "$(skopeo_pinned inspect --tls-verify=false --format '{{.Digest}}' "docker://$repo@$digest")" == "$digest" ]]
skopeo_pinned inspect --tls-verify=false --raw "docker://$repo@$digest" > "$root/remote-manifest.json"
[[ "$(jq -r .config.digest "$root/remote-manifest.json")" == "$accepted_id" ]]
foreign_after="$(skopeo_pinned inspect --tls-verify=false --format '{{.Digest}}' "docker://$repo:v0.8")"
tags_after="$(curl --fail --silent "http://$endpoint/v2/fixture/tags/list" | jq -c '.tags|sort')"
[[ "$foreign_before" == "$foreign_after" && "$tags_before" == "$tags_after" && "$tags_after" == '["v0.8"]' ]]
jq -n --arg digest "$digest" --arg config "$accepted_id" --arg tool "$(cat "$root/layout/skopeo-version.txt")" --arg registry "$registry_image" \
 '{localRegistry:true,digestDestination:true,sourceConfigPreserved:true,noMutableTagWrites:true,foreignVersionTagUnchanged:true,originalAndTriggeringNonownersRejectedBeforeIO:true,manifestDigest:$digest,imageId:$config,skopeoVersion:$tool,registryFixture:$registry,externalPublication:false}' > "$root/g6-local-transport-evidence.json"
