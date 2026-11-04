param(
    [Parameter(Mandatory)][ValidatePattern('^[0-9a-f]{40}$')][string]$SourceSha,
    [Parameter(Mandatory)][long[]]$RunId,
    [ValidatePattern('^[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+$')][string]$Repository = 'araditc/Kafdeck',
    [ValidateSet('Candidate','Main')][string]$Mode = 'Main',
    [string]$OutputDirectory = 'artifacts/v08-readiness',
    [string]$RequiredGates = (Join-Path $PSScriptRoot 'v08-required-gates.json'),
    [string]$Gh = 'gh'
)
$ErrorActionPreference = 'Stop'
$root = [IO.Path]::GetFullPath($OutputDirectory)
New-Item -ItemType Directory -Force -Path $root | Out-Null
$required = Get-Content -LiteralPath $RequiredGates -Raw | ConvertFrom-Json
$runs = @(); $artifacts = @(); $files = @(); $missing = [Collections.Generic.List[string]]::new()
function Read-GhJson([string[]]$Arguments) {
    $raw = & $Gh @Arguments
    if ($LASTEXITCODE -ne 0) { throw 'Read-only GitHub evidence query failed.' }
    return ($raw -join "`n" | ConvertFrom-Json)
}
foreach ($id in ($RunId | Sort-Object -Unique)) {
    $run = Read-GhJson @('run','view',"$id",'--repo',$Repository,'--json','databaseId,headSha,headBranch,event,status,conclusion,name,url,jobs')
    if ($run.headSha -ne $SourceSha) { throw "Run $id is not bound to the selected immutable SHA." }
    if ($Mode -eq 'Main' -and ($run.headBranch -ne 'main' -or $run.event -notin @('push','workflow_dispatch'))) {
        throw "Run $id is not a protected-main push/manual run. PR evidence cannot satisfy Main mode."
    }
    if ($run.status -ne 'completed' -or $run.conclusion -ne 'success') { $missing.Add("run:$id:not-successful") }
    $runs += $run
    $catalog = Read-GhJson @('api',"repos/$Repository/actions/runs/$id/artifacts?per_page=100")
    foreach ($artifact in $catalog.artifacts) {
        if ($artifact.expired) { $missing.Add("artifact:$id/$($artifact.name):expired"); continue }
        if ($artifact.name -notmatch '^[A-Za-z0-9_.-]{1,160}$' -or $artifact.name -in @('.','..')) { throw 'Unsafe artifact identity.' }
        $directory = Join-Path $root "downloads/$id/$($artifact.name)"
        # Never reuse a previous extraction as evidence for a new query/run.
        if (Test-Path -LiteralPath $directory) { throw 'Choose a fresh output directory; existing evidence is preserved.' }
        & $Gh run download "$id" --repo $Repository --name $artifact.name --dir $directory
        if ($LASTEXITCODE -ne 0) { throw "Artifact download failed for run $id." }
        $artifacts += [pscustomobject]@{ runId=$id; name=$artifact.name; apiDigest=$artifact.digest; digestMeaning='GitHub-reported archive digest, not independently verified by this extracted-file collector' }
        foreach ($file in Get-ChildItem -LiteralPath $directory -File -Recurse -Force) {
            $relative = [IO.Path]::GetRelativePath($directory,$file.FullName).Replace('\','/')
            if ($relative -match '(?i)(private|credential|connection-string|appsettings|lab\.json)' -or $file.Extension -in @('.key','.pfx','.p12','.db')) {
                throw 'An artifact contains disallowed private laboratory material. Do not publish the packet.'
            }
            $allowed = $file.Extension -in @('.trx','.sarif','.zip','.nupkg') -or $file.Name -like '*-evidence.json' -or $file.Name -like '*candidate*.tar.gz' -or
                $file.Name -in @('sbom.spdx.json','candidate-provenance.json','build-modules.txt') -or $file.Name -like '*SHA256SUMS'
            if (-not $allowed) { continue }
            $entry = [ordered]@{runId=$id;artifact=$artifact.name;path=[IO.Path]::GetRelativePath($root,$file.FullName).Replace('\','/');name=$file.Name;sha256=(Get-FileHash -LiteralPath $file.FullName -Algorithm SHA256).Hash.ToLowerInvariant()}
            if ($file.Extension -eq '.trx') {
                $xml = [Xml.XmlDocument]::new(); $xml.XmlResolver=$null; $xml.Load($file.FullName)
                $counter=$xml.SelectSingleNode("/*[local-name()='TestRun']/*[local-name()='ResultSummary']/*[local-name()='Counters']")
                if ($null -eq $counter) { throw 'TRX counters missing.' }
                $entry.counters=[ordered]@{total=[int]$counter.total;passed=[int]$counter.passed;failed=[int]$counter.failed;notExecuted=[int]$counter.notExecuted}
            }
            $files += [pscustomobject]$entry
        }
    }
}
foreach ($name in $required.requiredJobs) {
    if (-not @($runs.jobs | Where-Object { $_.name -eq $name -and $_.conclusion -eq 'success' }).Count) { $missing.Add("job:$name") }
}
foreach ($name in $required.requiredZeroSkipTrx) {
    $matching=@($files | Where-Object { $_.name -eq $name -and $_.counters.total -gt 0 -and $_.counters.passed -eq $_.counters.total -and $_.counters.failed -eq 0 -and $_.counters.notExecuted -eq 0 })
    if ($matching.Count -eq 0) { $missing.Add("zero-skip-trx:$name") }
}
foreach ($broker in $required.requiredKafkaMatrix) {
    # Exact artifact/job identity binds the broker version and tier. A generic
    # successful job or another broker's identically named TRX cannot substitute.
    $matchingArtifacts=@($artifacts|Where-Object name -eq $broker.artifact)
    $complete=$false
    foreach($artifact in $matchingArtifacts) {
        $run=@($runs|Where-Object databaseId -eq $artifact.runId)
        if($run.Count -ne 1 -or -not @($run[0].jobs|Where-Object { $_.name -eq $broker.job -and $_.conclusion -eq 'success' }).Count){continue}
        $valid=$true
        foreach($batch in $required.requiredKafkaBatches) {
            $matching=@($files|Where-Object { $_.runId -eq $artifact.runId -and $_.artifact -eq $broker.artifact -and $_.name -eq $batch.file })
            if($matching.Count -ne 1){$valid=$false;continue}
            $item=$matching[0];$c=$item.counters
            if($c.total -lt 1 -or $c.passed -ne $c.total -or $c.failed -ne 0 -or $c.notExecuted -ne 0){$valid=$false;continue}
            $xml=[Xml.XmlDocument]::new();$xml.XmlResolver=$null;$xml.Load((Join-Path $root $item.path))
            $results=@($xml.SelectNodes("//*[local-name()='UnitTestResult']"))
            $definitions=@($xml.SelectNodes("//*[local-name()='TestDefinitions']/*[local-name()='UnitTest']"))
            if($results.Count -ne $c.total -or @($results|Where-Object outcome -ne 'Passed').Count){$valid=$false;continue}
            $observedClasses=[Collections.Generic.HashSet[string]]::new([StringComparer]::Ordinal)
            foreach($result in $results) {
                $definition=@($definitions|Where-Object id -eq $result.testId)
                $method=if($definition.Count -eq 1){$definition[0].SelectSingleNode("*[local-name()='TestMethod']")}else{$null}
                $matchingClass=@()
                if($null -ne $method){$matchingClass=@($batch.testClasses|Where-Object {$method.className.EndsWith('.'+$_,[StringComparison]::Ordinal)})}
                if($matchingClass.Count -ne 1){$valid=$false}else{[void]$observedClasses.Add($matchingClass[0])}
            }
            foreach($class in $batch.testClasses){if(-not $observedClasses.Contains($class)){$valid=$false}}
        }
        if($valid){$complete=$true}
    }
    if(-not $complete){$missing.Add("kafka-matrix:$($broker.version)/$($broker.tier):eight-positive-zero-skip-batches-with-exact-classes")}
}
foreach ($name in @($required.requiredFiles)+@($required.requiredProviderEvidence)) {
    if (-not @($files | Where-Object name -eq $name).Count) { $missing.Add("file:$name") }
}
foreach($group in $required.requiredTrxTestPatterns) {
    foreach($pattern in $group.patterns) {
        $found=$false
        foreach($item in @($files|Where-Object name -eq $group.file)) {
            $xml=[Xml.XmlDocument]::new();$xml.XmlResolver=$null;$xml.Load((Join-Path $root $item.path))
            if(@($xml.SelectNodes("//*[local-name()='UnitTestResult']")|Where-Object { $_.outcome -eq 'Passed' -and $_.testName -match $pattern }).Count){$found=$true}
        }
        if(-not $found){$missing.Add("test-family:$($group.file)/$pattern")}
    }
}
foreach($item in @($files|Where-Object { $_.name -in $required.requiredProviderEvidence })) {
    $data=Get-Content -LiteralPath (Join-Path $root $item.path) -Raw|ConvertFrom-Json
    $expected=($item.name -replace '^notification-governance-','' -replace '-evidence.json$','')
    if($data.provider.ToLowerInvariant() -ne $expected -or $data.actualPostgreSql -ne $true -or $data.physicalAuthenticatedSends -lt 1){$missing.Add("provider:physical-authenticated-proof:$expected")}
}
foreach($item in @($files|Where-Object name -eq 'offline-startup-evidence.json')) {
    $data=Get-Content -LiteralPath (Join-Path $root $item.path) -Raw|ConvertFrom-Json
    if($data.networkNone -ne $true -or $data.health -ne $true -or $data.staticFrontend -ne $true){$missing.Add('offline:network-none-health-frontend')}
}
$sbom=@($files|Where-Object name -eq 'sbom.spdx.json')
foreach($item in $sbom) {
    $data=Get-Content -LiteralPath (Join-Path $root $item.path) -Raw|ConvertFrom-Json
    if($data.spdxVersion -notlike 'SPDX-*' -or -not $data.documentNamespace -or -not $data.packages.Count){$missing.Add('sbom:invalid-or-empty')}
}
foreach($item in @($files|Where-Object name -eq 'candidate-provenance.json')) {
    $data=Get-Content -LiteralPath (Join-Path $root $item.path) -Raw|ConvertFrom-Json
    if($data.sourceSha -ne $SourceSha -or -not $data.imageId -or $data.published -ne $false){$missing.Add('provenance:source/image/nonpublication-binding')}
    if(-not @($files|Where-Object { $_.runId -eq $item.runId -and $_.name -eq 'sbom.spdx.json' -and $_.sha256 -eq $data.sbomSha256 }).Count){$missing.Add('provenance:exact-sbom-hash-binding')}
    if(-not @($files|Where-Object { $_.runId -eq $item.runId -and $_.name -like '*candidate*.tar.gz' -and $_.sha256 -eq $data.imageArchiveSha256 }).Count){$missing.Add('provenance:exact-offline-archive-hash-binding')}
}
$packet=[ordered]@{schemaVersion=1;sourceSha=$SourceSha;repository=$Repository;mode=$Mode;status=if($missing.Count){'Incomplete'}elseif($Mode -eq 'Candidate'){'CandidateEvidenceCompleteAwaitingMainAcceptance'}else{'EvidenceCompleteAwaitingOwnerPublicationDecision'};publicationAuthorized=$false;requiredGatesSha256=(Get-FileHash -LiteralPath $RequiredGates -Algorithm SHA256).Hash.ToLowerInvariant();runs=$runs;artifacts=$artifacts;files=$files;missing=@($missing);limits=@('Evidence is not summed across overlapping suites.','API-reported artifact archive digests and locally computed extracted-file hashes are distinct.','No release identity, tag, registry upload, deployment or publication action is performed.','A successful packet never substitutes for the explicit G6 owner decision.')}
$packet|ConvertTo-Json -Depth 30|Set-Content -LiteralPath (Join-Path $root 'v0.8-readiness-packet.json') -Encoding utf8
Write-Output "Readiness packet: $($packet.status); missing gates: $($missing.Count); publication authorized: false"
if($missing.Count){exit 1}
