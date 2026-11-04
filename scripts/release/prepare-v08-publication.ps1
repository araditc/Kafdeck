param()
$ErrorActionPreference='Stop'
$owners=Get-Content .github/release/release-owners.json -Raw|ConvertFrom-Json
$rule=[regex]::Match((Get-Content .github/CODEOWNERS -Raw),'(?m)^/\.github/\s+([^\r\n]+)$')
$expected=@($rule.Groups[1].Value.Split(' ',[StringSplitOptions]::RemoveEmptyEntries)|Where-Object {$_ -like '@*'}|ForEach-Object {$_.Substring(1).ToLowerInvariant()}|Sort-Object -Unique)
if($owners.schemaVersion -ne 1 -or -not $expected.Count -or (@($owners.actors|Sort-Object -Unique)-join ',') -cne ($expected-join ',')){throw 'Protected release owner policy differs from CODEOWNERS.'}
foreach($actor in @($env:GITHUB_ACTOR,$env:GITHUB_TRIGGERING_ACTOR)){if(-not $actor -or $actor -notin $owners.actors){throw 'Original and triggering release actors must both be approved owners.'}}
$source=$env:SOURCE_SHA; $archiveHash=$env:ARCHIVE_SHA256
if($env:GITHUB_EVENT_NAME -ne 'workflow_dispatch' -or $env:GITHUB_REF -ne 'refs/heads/main'){throw 'Publication selection requires explicit main workflow dispatch.'}
if($source -notmatch '^[0-9a-f]{40}$' -or $source -ne $env:GITHUB_SHA -or $archiveHash -notmatch '^[0-9a-f]{64}$'){throw 'Invalid exact source/archive selection.'}
$manifest=Get-Content .github/release/release.json -Raw|ConvertFrom-Json
if($manifest.version -ne 'v0.8' -or $manifest.status -ne 'candidate' -or $env:OWNER_APPROVAL -cne "PUBLISH $($manifest.version) FROM $source"){throw 'Explicit owner approval must name this candidate version and exact SHA.'}
if($manifest.notes -notmatch '^docs/releases/v[0-9]+\.[0-9]+(?:\.[0-9]+)?\.md$' -or -not (Test-Path -LiteralPath $manifest.notes)){throw 'Candidate release notes are invalid.'}
$main=& gh api "repos/$env:GITHUB_REPOSITORY/git/ref/heads/main" --jq .object.sha
if($LASTEXITCODE -ne 0 -or $main -cne $source){throw 'The selected source is no longer current main.'}
$head=& git rev-parse HEAD
if($LASTEXITCODE -ne 0 -or $head -cne $source){throw 'Workflow checkout differs from owner-authorized source.'}
if($env:VERIFIED_RUN_IDS -notmatch '^[0-9]+(?:,[0-9]+){0,39}$'){throw 'Run IDs must be a bounded comma-separated numeric list.'}
$ids=@($env:VERIFIED_RUN_IDS.Split(',')|ForEach-Object {[long]$_}|Sort-Object -Unique)
if(@($ids|Where-Object {$_ -le 0 -or $_ -eq [long]$env:GITHUB_RUN_ID}).Count){throw 'Producer IDs must be positive prior completed runs.'}
$root='artifacts/v08-publication-readiness'
& "$PSScriptRoot/assemble-v08-readiness.ps1" -SourceSha $source -RunId $ids -Repository $env:GITHUB_REPOSITORY -Mode Main -OutputDirectory $root
if($LASTEXITCODE -ne 0){throw 'Strict Main packet verification failed.'}
$packetFile=Join-Path $root 'v0.8-readiness-packet.json';$packet=Get-Content $packetFile -Raw|ConvertFrom-Json
if($packet.status -ne 'EvidenceCompleteAwaitingOwnerPublicationDecision' -or $packet.mode -ne 'Main' -or $packet.sourceSha -ne $source -or $packet.missing.Count -ne 0){throw 'Main evidence packet is incomplete.'}
$archives=@($packet.files|Where-Object { $_.name -like '*candidate*.tar.gz' -and $_.sha256 -ceq $archiveHash })
if($archives.Count -ne 1){throw 'Select exactly one owner-reviewed candidate archive from the verified packet.'}
$archive=$archives[0]
$provenances=@($packet.files|Where-Object { $_.runId -eq $archive.runId -and $_.name -eq 'candidate-provenance.json' })
if($provenances.Count -ne 1){throw 'Unique same-run candidate provenance required.'}
$provenance=Get-Content (Join-Path $root $provenances[0].path) -Raw|ConvertFrom-Json
$sboms=@($packet.files|Where-Object { $_.runId -eq $archive.runId -and $_.name -eq 'sbom.spdx.json' -and $_.sha256 -ceq $provenance.sbomSha256 })
if($sboms.Count -ne 1 -or $provenance.sourceSha -cne $source -or $provenance.published -ne $false -or $provenance.imageArchiveSha256 -cne $archiveHash -or $provenance.imageId -notmatch '^sha256:[0-9a-f]{64}$'){throw 'Candidate provenance/SBOM/archive binding differs.'}
$out='artifacts/v08-publication-selected';New-Item -ItemType Directory -Force $out|Out-Null
$assetDirectory=Join-Path $out 'release-assets';New-Item -ItemType Directory -Force $assetDirectory|Out-Null
$assetNames=@('terraform-provider-kafdeck_0.8.0_linux_amd64.zip','terraform-provider-kafdeck_0.8.0_linux_arm64.zip','terraform-provider-kafdeck_0.8.0_windows_amd64.zip','terraform-provider-kafdeck_0.8.0_darwin_amd64.zip','terraform-provider-kafdeck_0.8.0_darwin_arm64.zip','Kafdeck.Mcp.0.8.0.nupkg')
$assets=@();foreach($name in $assetNames){$items=@($packet.files|Where-Object { $_.name -ceq $name -and $_.artifact -eq 'g4-external-integration-packages' });if($items.Count -ne 1){throw 'Unique exact-main client package is missing.'};$item=$items[0];$file=Join-Path $root $item.path;if((Get-FileHash $file -Algorithm SHA256).Hash.ToLowerInvariant() -cne $item.sha256){throw 'Client package hash differs from Main packet.'};Copy-Item -LiteralPath $file -Destination (Join-Path $assetDirectory $name);$assets+=[ordered]@{file=$name;sha256=$item.sha256}}
$assets|Sort-Object file|ForEach-Object {$_.sha256+'  '+$_.file}|Set-Content (Join-Path $assetDirectory 'SHA256SUMS') -Encoding utf8
Copy-Item -LiteralPath (Join-Path $root $archive.path) -Destination (Join-Path $out 'candidate.tar.gz')
Copy-Item -LiteralPath (Join-Path $root $provenances[0].path) -Destination (Join-Path $out 'candidate-provenance.json')
Copy-Item -LiteralPath (Join-Path $root $sboms[0].path) -Destination (Join-Path $out 'sbom.spdx.json')
Copy-Item -LiteralPath $packetFile -Destination (Join-Path $out 'v0.8-readiness-packet.json')
[ordered]@{schemaVersion=1;sourceSha=$source;version=$manifest.version;image=('ghcr.io/'+$env:GITHUB_REPOSITORY.ToLowerInvariant());imageId=$provenance.imageId;archiveSha256=$archiveHash;sbomSha256=$provenance.sbomSha256;packetSha256=(Get-FileHash $packetFile -Algorithm SHA256).Hash.ToLowerInvariant();producerRunId=$archive.runId;ownerApproval=$env:OWNER_APPROVAL;dispatchActor=$env:GITHUB_ACTOR;dispatchRun=$env:GITHUB_RUN_ID;published=$false;assets=$assets;assetsManifestSha256=(Get-FileHash (Join-Path $assetDirectory 'SHA256SUMS') -Algorithm SHA256).Hash.ToLowerInvariant()}|ConvertTo-Json -Depth 8|Set-Content (Join-Path $out 'publication-selection.json') -Encoding utf8
"verified=true" >> $env:GITHUB_OUTPUT
"source_sha=$source" >> $env:GITHUB_OUTPUT
Write-Output 'Exact Main packet and owner-selected candidate verified; nothing published.'
