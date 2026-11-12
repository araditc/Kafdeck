#requires -Version 7.4
param(
    [Parameter(Mandatory)][string]$MediaDirectory,
    [Parameter(Mandatory)][string]$VerifierWorktree,
    [Parameter(Mandatory)][ValidatePattern('^[0-9a-f]{40}$')][string]$SourceSha,
    [Parameter(Mandatory)][ValidatePattern('^[0-9a-f]{64}$')][string]$ManifestSha256,
    [Parameter(Mandatory)][ValidatePattern('^[0-9a-f]{64}$')][string]$TrustedRootSha256,
    [ValidateSet('Sqlite','SqliteFence','Postgres','PostgresInterrupted','Physical','PhysicalInterrupted')][string]$Scenario = 'SqliteFence',
    [ValidatePattern('^pg-physical(?:-[a-z0-9]+)?$')][string]$LabName = 'pg-physical',
    [string]$Cosign = 'cosign', [switch]$VerifyOnly
)
$ErrorActionPreference = 'Stop'
if (!$IsWindows) { throw 'This adapter retains the proven Windows fixture topology.' }
$media = (Resolve-Path -LiteralPath $MediaDirectory).Path
$verifier = (Resolve-Path -LiteralPath $VerifierWorktree).Path
if ((& git -C $verifier rev-parse HEAD).Trim() -cne $SourceSha) { throw 'Verifier must be the exact signed protected source.' }
& git -C $verifier diff --quiet HEAD --
if ($LASTEXITCODE) { throw 'Verifier worktree has tracked changes.' }
if ((& git -C "$PSScriptRoot/released-v08" rev-parse HEAD).Trim() -cne 'cbedd025ab97db10fb21407ea5188c08d6a4d8ce') {
    throw 'The existing immutable released-v0.8 fixture source differs.'
}
$manifestFile = Join-Path $media 'manifest.json'
$rootFile = Join-Path $media 'trusted-root.json'
if ((Get-FileHash -LiteralPath $manifestFile).Hash.ToLowerInvariant() -cne $ManifestSha256 -or
    (Get-FileHash -LiteralPath $rootFile).Hash.ToLowerInvariant() -cne $TrustedRootSha256) { throw 'Reviewed manifest/trust-root identity differs.' }
& $Cosign verify-blob --bundle (Join-Path $media 'manifest.sigstore.json') --trusted-root $rootFile `
    --certificate-identity 'https://github.com/araditc/Kafdeck/.github/workflows/v09-prepublication-signing.yml@refs/heads/main' `
    --certificate-oidc-issuer 'https://token.actions.githubusercontent.com' `
    --certificate-github-workflow-repository 'araditc/Kafdeck' --certificate-github-workflow-name 'v09-prepublication-signing' `
    --certificate-github-workflow-ref 'refs/heads/main' --certificate-github-workflow-trigger 'workflow_dispatch' `
    --certificate-github-workflow-sha $SourceSha $manifestFile
if ($LASTEXITCODE) { throw 'Official signed-media verification failed.' }
$manifest = Get-Content -LiteralPath $manifestFile -Raw | ConvertFrom-Json
if ($manifest.repository -cne 'araditc/Kafdeck' -or $manifest.version -cne '0.9.0' -or
    $manifest.scope -cne 'full-distribution' -or $manifest.sourceSha -cne $SourceSha -or $manifest.workflowSourceSha -cne $SourceSha) {
    throw 'Only the exact final signed full-distribution candidate is admitted.'
}
$entry = @($manifest.content.files | Where-Object path -CEQ 'native/win-x64/kafdeck-api-ui-win-x64.tar.gz')
if ($entry.Count -ne 1) { throw 'Signed Windows API archive is missing or ambiguous.' }
$archive = Join-Path $media "media/$($entry[0].path)"
if ((Get-FileHash -LiteralPath $archive).Hash.ToLowerInvariant() -cne $entry[0].sha256) { throw 'Signed Windows archive changed.' }
$installed = Join-Path $PSScriptRoot ('signed-candidate-' + [Guid]::NewGuid().ToString('N'))
$extract = 'import pathlib,sys; sys.path.insert(0,sys.argv[1]); from v09_offline_install import extract_archive; extract_archive(pathlib.Path(sys.argv[2]),pathlib.Path(sys.argv[3]))'
& python -B -c $extract (Join-Path $verifier 'scripts/release') $archive $installed
if ($LASTEXITCODE) { throw 'Safe extraction of the signed native archive failed.' }
$assembly = Join-Path $installed 'Kafdeck.Api.exe'
$assemblyHash = (Get-FileHash -LiteralPath $assembly).Hash.ToLowerInvariant()
$adapter = if ($Scenario.StartsWith('Physical',[StringComparison]::OrdinalIgnoreCase)) { 'run-signed-physical-pitr.ps1' } else { 'run-signed-provider.ps1' }
@{schemaVersion=1;sourceSha=$SourceSha;signedManifestSha256=$ManifestSha256;trustedRootSha256=$TrustedRootSha256;
    candidateArchiveSha256=$entry[0].sha256;candidateAssemblySha256=$assemblyHash;signatureVerified=$true;
    adapter=$adapter;adapterSha256=(Get-FileHash -LiteralPath (Join-Path $PSScriptRoot $adapter)).Hash.ToLowerInvariant();
    scope='signature and selected Windows API archive verified locally; complete media verified by official preparation; recovery and disconnected authenticated journey not yet asserted'} |
    ConvertTo-Json | Set-Content -LiteralPath (Join-Path $installed 'verified-signed-input.json') -Encoding utf8
if ($VerifyOnly) { Write-Output "Verified signed fixture input retained: $installed"; return }
$arguments = @{CandidateAssembly=$assembly;ExpectedCandidateHash=$assemblyHash;SourceSha=$SourceSha;CandidateArchiveSha256=$entry[0].sha256}
if ($Scenario.StartsWith('Physical',[StringComparison]::OrdinalIgnoreCase)) { $arguments.LabName=$LabName; $arguments.InterruptMigration=($Scenario -eq 'PhysicalInterrupted') }
else { $arguments.Provider= $(if ($Scenario.StartsWith('Sqlite',[StringComparison]::OrdinalIgnoreCase)) {'sqlite'} else {'postgres'}); $arguments.Fence=($Scenario -eq 'SqliteFence'); $arguments.Interrupt=($Scenario -eq 'PostgresInterrupted') }
& (Join-Path $PSScriptRoot $adapter) @arguments
