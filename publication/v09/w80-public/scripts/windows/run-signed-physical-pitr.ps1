#requires -Version 7.4
param([ValidatePattern('^pg-physical(?:-[a-z0-9]+)?$')][string]$LabName = 'pg-physical',
    [switch]$InterruptMigration,
    [Parameter(Mandatory)][string]$CandidateAssembly,
    [Parameter(Mandatory)][ValidatePattern('^[0-9a-f]{64}$')][string]$ExpectedCandidateHash,
    [Parameter(Mandatory)][ValidatePattern('^[0-9a-f]{40}$')][string]$SourceSha,
    [Parameter(Mandatory)][ValidatePattern('^[0-9a-f]{64}$')][string]$CandidateArchiveSha256)
$ErrorActionPreference = 'Stop'
$repo = (Resolve-Path "$PSScriptRoot/../..").Path
$lab = (Resolve-Path "$PSScriptRoot/$LabName").Path
$archive = (Resolve-Path "$PSScriptRoot/$LabName-archive").Path
$bin = (Resolve-Path "$repo/deploy/dev/.generated/g2/postgres/pgsql/bin").Path
$control = (Resolve-Path "$PSScriptRoot/physical-signed-lab-control.ps1").Path
$restoreCommand = (Get-Content -LiteralPath "$PSScriptRoot/$LabName-restore-command.txt" -Raw).Trim()
$data = Get-Content -LiteralPath "$lab/lab.json" -Raw | ConvertFrom-Json
if ($data.state -ne 'StreamingSynchronous' -or $data.bindAddress -ne '127.0.0.1' -or
    $data.primary.port -ne 25533 -or $data.standby.port -ne 25534 -or $data.restore.port -ne 25535 -or
    $data.systemIdentifier -notmatch '^\d{10,22}$') { throw 'Invalid dedicated W80 lab.' }
$run = Join-Path $PSScriptRoot ('postgres-pitr-' + [Guid]::NewGuid().ToString('N'))
$null = New-Item -ItemType Directory -Path $run
$oldAssembly = (Resolve-Path "$PSScriptRoot/v08-build/Kafdeck.Api.dll").Path
# Signed-only adapter: no source-build or unsigned prototype fallback.
$candidate = (Resolve-Path -LiteralPath $CandidateAssembly).Path
if ([IO.Path]::GetFileName($candidate) -cne 'Kafdeck.Api.exe' -or
    (Get-FileHash -LiteralPath $candidate).Hash.ToLowerInvariant() -cne $ExpectedCandidateHash) {
    throw 'Exact signed-candidate executable identity mismatch.'
}
$candidateSource = $SourceSha
$archiveHash = $CandidateArchiveSha256
$probe = (Resolve-Path "$PSScriptRoot/probe/bin/Release/net10.0/W80.Probe.dll").Path
$keyFile = Join-Path $run 'synthetic-material-key.txt'
[IO.File]::WriteAllText($keyFile, [Convert]::ToHexString([Security.Cryptography.RandomNumberGenerator]::GetBytes(32)))
$operatorAnchor = Join-Path $lab 'operator-anchor.json'
$originalGeneration = [Guid]::NewGuid().ToString('D')
$restoredGeneration = [Guid]::NewGuid().ToString('D')
$point = 'w80_' + [Guid]::NewGuid().ToString('N')
$apiPort = 25551
$apiUrl = "http://127.0.0.1:$apiPort"
$ownedRestore = Join-Path $lab 'restore'
$progress = Join-Path $run 'stages.log'
$apps = [Collections.Generic.List[Diagnostics.Process]]::new()

function Stage([string]$Stage) {
    "$([DateTimeOffset]::UtcNow.ToString('O')) $Stage" | Add-Content -LiteralPath $progress
    Write-Output $Stage
}

function Run-Control([string]$Action, [string[]]$Other = @()) {
    & pwsh -NoProfile -File $control -Action $Action -OutputDirectory $lab -BinDirectory $bin @Other
    if ($LASTEXITCODE -ne 0) { throw "W80 PostgreSQL control action failed: $Action." }
}

function Invoke-Sql([int]$Port, [string]$Sql, [switch]$WriteDenied) {
    $before = $env:PGPASSWORD
    try {
        $env:PGPASSWORD = [IO.File]::ReadAllText((Join-Path $lab 'admin-password.txt')).Trim()
        $result = & "$bin/psql.exe" -XAt -v ON_ERROR_STOP=1 -v VERBOSITY=verbose -w -h 127.0.0.1 -p $Port -U g2_recovery_owner -d postgres -c $Sql 2>&1
        $exit = $LASTEXITCODE
        $message = ($result | Out-String).Trim()
        if ($WriteDenied) {
            if ($exit -eq 0 -or $message -notmatch '\b25006\b') { throw 'Physical restore did not reject a write under read-only quarantine.' }
            return '25006'
        }
        if ($exit -ne 0) { throw "Owned PostgreSQL query failed on port $Port." }
        return $message
    } finally { $env:PGPASSWORD = $before }
}

function Invoke-Probe([string]$Action, [string]$ConnectionFile) {
    $json = & dotnet $probe $Action postgres $ConnectionFile
    if ($LASTEXITCODE -ne 0) { throw "Fixture probe $Action failed." }
    return ($json | ConvertFrom-Json)
}

function Assert-States($Expected, $Observed) {
    if (($Expected.States | ConvertTo-Json -Compress) -cne ($Observed.States | ConvertTo-Json -Compress)) {
        throw 'Mutation snapshot or state changed across physical recovery.'
    }
}

function Start-Api([string]$Executable, [string]$Name, [string]$ConnectionFile, [string]$Generation = '',
    [string]$PreviousGeneration = '', [string]$FenceHash = '', [string]$RestoreHash = '') {
    $configuration = @{
        Kafdeck__Deployment__ListenUrls__0 = $apiUrl
        Kafdeck__Deployment__AccessMode = 'Oidc'
        Kafdeck__Deployment__Oidc__Issuer = 'https://localhost:25561/realms/w80-pitr'
        Kafdeck__Deployment__Oidc__ClientId = 'w80-pitr'
        Kafdeck__Administration__Mutations__Enabled = 'true'
        Kafdeck__Administration__Mutations__BackgroundDataWorkersEnabled = 'false'
        Kafdeck__Administration__Mutations__MaterialDigestKey = "file:$keyFile"
        Kafdeck__Administration__Mutations__Persistence__Provider = 'PostgreSql'
        Kafdeck__Administration__Mutations__Persistence__ExecutionMode = 'Standalone'
        Kafdeck__Administration__Mutations__Persistence__ConnectionString = "file:$ConnectionFile"
    }
    $arguments = @('--contentRoot', ('"' + (Split-Path $Executable) + '"'))
    if ($Generation) { $configuration.Kafdeck__Governance__DeploymentGeneration = $Generation }
    if ($PreviousGeneration) {
        $arguments += '--governance-generation-admit'
        $configuration.Kafdeck__Governance__RestoreMaintenanceMode = 'true'
        $configuration.Kafdeck__Governance__PreviousDeploymentGeneration = $PreviousGeneration
        $configuration.Kafdeck__Governance__FencedWorkersEvidenceSha256 = $FenceHash
        $configuration.Kafdeck__Governance__RestoreEvidenceSha256 = $RestoreHash
    }
    $entry = $Executable
    if ($Executable.EndsWith('.dll')) {
        $entry = (Get-Command dotnet).Source
        $arguments = @('"' + $Executable + '"') + $arguments
    }
    $process = Start-Process -FilePath $entry -ArgumentList $arguments -WorkingDirectory $run -PassThru -WindowStyle Hidden -Environment $configuration -RedirectStandardOutput (Join-Path $run "$Name.out.log") -RedirectStandardError (Join-Path $run "$Name.err.log")
    $apps.Add($process)
    return $process
}

function Stop-Api($Process) {
    if (!$Process.HasExited) {
        Stop-Process -InputObject $Process
        if (!$Process.WaitForExit(10000)) { throw 'Owned API process did not stop.' }
    }
}

function Wait-Api($Process, [string]$Name) {
    $until = [DateTimeOffset]::UtcNow.AddSeconds(45)
    while ([DateTimeOffset]::UtcNow -lt $until -and !$Process.HasExited) {
        try {
            if ((Invoke-WebRequest -Uri "$apiUrl/healthz" -NoProxy -TimeoutSec 2).StatusCode -eq 200) { return }
        } catch { Start-Sleep -Milliseconds 250 }
    }
    throw "Owned API $Name did not become healthy."
}

function Assert-PortClosed([int]$Port) {
    $tcp = [Net.Sockets.TcpClient]::new()
    try {
        $open = $false
        try {
            $tcp.ConnectAsync('127.0.0.1',$Port).WaitAsync([TimeSpan]::FromSeconds(1)).GetAwaiter().GetResult()
            $open = $true
        } catch { }
        if ($open) { throw "Owned port $Port was unexpectedly open." }
    } finally { $tcp.Dispose() }
}

function Assert-InterruptedMigration($Baseline) {
    $lockerName = 'w80_lock_' + [Guid]::NewGuid().ToString('N')
    $lockSql = Join-Path $run 'hold-catalog-migration.sql'
    [IO.File]::WriteAllText($lockSql, "SET statement_timeout=0;`nSELECT pg_advisory_lock(64964007301);`nSELECT pg_sleep(75);`n")
    $args = @('-h','127.0.0.1','-p',[string]$data.primary.port,'-U','g2_recovery_owner','-w','-X','-d','postgres','-f',('"' + $lockSql + '"'))
    $locker = Start-Process -FilePath "$bin/psql.exe" -ArgumentList $args -WindowStyle Hidden -PassThru -RedirectStandardOutput "$run/lock.out.log" -RedirectStandardError "$run/lock.err.log" -Environment @{
        PGAPPNAME=$lockerName; PGPASSWORD=[IO.File]::ReadAllText((Join-Path $lab 'admin-password.txt')).Trim()
    }
    try {
        $deadline = [DateTimeOffset]::UtcNow.AddSeconds(12)
        $heldSql = "SELECT count(*) FROM pg_locks JOIN pg_stat_activity USING(pid) WHERE application_name='$lockerName' AND locktype='advisory' AND (classid::bigint << 32) + objid::bigint=64964007301 AND granted;"
        do {
            $held = (Invoke-Sql $data.primary.port $heldSql) -ceq '1'
            if (!$held) { Start-Sleep -Milliseconds 100 }
        } while (!$held -and !$locker.HasExited -and [DateTimeOffset]::UtcNow -lt $deadline)
        if (!$held -or $locker.HasExited) { throw 'Named fixture advisory lock was not held.' }
        $interrupted = Start-Api $candidate 'interrupted' $data.primary.connectionStringFile $originalGeneration
        $deadline = [DateTimeOffset]::UtcNow.AddSeconds(25)
        $waitingSql = "SELECT count(*) FROM pg_stat_activity WHERE datname=current_database() AND wait_event='advisory' AND query='SELECT pg_advisory_xact_lock(64964007301)';"
        do {
            $waiting = (Invoke-Sql $data.primary.port $waitingSql) -ceq '1'
            if (!$waiting) { Start-Sleep -Milliseconds 100 }
        } while (!$waiting -and !$interrupted.HasExited -and [DateTimeOffset]::UtcNow -lt $deadline)
        if (!$waiting -or $interrupted.HasExited) { throw 'API did not wait at the selected migration boundary.' }
        Stop-Api $interrupted
        $afterInterruption = Invoke-Probe 'observe' $data.primary.connectionStringFile
        Assert-States $Baseline $afterInterruption
        if ($afterInterruption.Tables -notcontains 'governance_generation_schema' -or $afterInterruption.Tables -contains 'catalog_owned_schema') {
            throw 'Expected physical interruption after generation, before catalog schema.'
        }
        Stage 'Exact candidate process killed after generation schema commit, before catalog migration'
    } finally {
        $null = Invoke-Sql $data.primary.port "SELECT pg_terminate_backend(pid,5000) FROM pg_stat_activity WHERE datname=current_database() AND application_name='$lockerName';"
        Stop-Api $locker
    }
}

try {
    Assert-PortClosed $apiPort
    Assert-PortClosed $data.restore.port
    $legacy = Start-Api $oldAssembly 'v08' $data.primary.connectionStringFile
    Wait-Api $legacy 'v08'
    Stop-Api $legacy
    $baseline = Invoke-Probe 'seed' $data.primary.connectionStringFile
    if (($baseline.States.State -join ',') -cne '3,4,9' -or $baseline.Tables -contains 'governance_deployment_generation') {
        throw 'Legacy test snapshots and schema are not established.'
    }
    $baseline | ConvertTo-Json -Depth 6 | Set-Content -LiteralPath "$run/baseline-snapshots.json"
    Stage 'Actual v0.8 startup and populated legacy fixture verified'
    if ($InterruptMigration) { Assert-InterruptedMigration $baseline }
    $upgraded = Start-Api $candidate 'upgraded' $data.primary.connectionStringFile $originalGeneration
    Wait-Api $upgraded 'upgraded'
    Stop-Api $upgraded
    Assert-States $baseline (Invoke-Probe 'observe' $data.primary.connectionStringFile)
    $reopened = Start-Api $candidate 'reopened' $data.primary.connectionStringFile $originalGeneration
    Wait-Api $reopened 'reopened'
    Stop-Api $reopened
    Assert-States $baseline (Invoke-Probe 'observe' $data.primary.connectionStringFile)
    $null = Invoke-Sql $data.primary.port 'CREATE TABLE w80_pitr_markers(label text PRIMARY KEY);'
    Stage 'Candidate upgrade and reopen retained all legacy operation snapshots'

    Run-Control 'Backup'
    $backup = Get-Content -LiteralPath $data.latestBackupFile -Raw | ConvertFrom-Json
    $backupName = [IO.Path]::GetFileName($backup.backupDirectory)
    $verifiedBackup = (Resolve-Path "$lab/backups/$backupName").Path
    if ($backup.backupDirectory -cne $verifiedBackup -or !$backup.verified -or $backup.systemIdentifier -cne $data.systemIdentifier -or
        (Get-FileHash -LiteralPath "$verifiedBackup/backup_manifest").Hash.ToLowerInvariant() -cne $backup.manifestSha256) {
        throw 'Physical base backup identity or verification is invalid.'
    }
    $null = Invoke-Sql $data.primary.port 'SELECT pg_switch_wal();'
    $null = Invoke-Sql $data.primary.port "INSERT INTO w80_pitr_markers VALUES ('before-target');"
    $targetLsn = Invoke-Sql $data.primary.port "SELECT pg_create_restore_point('$point');"
    if ($targetLsn -notmatch '^[0-9A-F]+/[0-9A-F]+$') { throw 'Invalid native restore-point LSN.' }
    $walSegment = Invoke-Sql $data.primary.port "SELECT pg_walfile_name('$targetLsn');"
    if ($walSegment -notmatch '^[0-9A-F]{24}$') { throw 'Invalid native restore-point WAL segment.' }
    $null = Invoke-Sql $data.primary.port "INSERT INTO w80_pitr_markers VALUES ('after-target');"
    if ((Invoke-Sql $data.primary.port 'SELECT count(*) FROM w80_pitr_markers;') -cne '2') { throw 'Post-target commit absent on source.' }
    $null = Invoke-Sql $data.primary.port 'SELECT pg_switch_wal();'
    $deadline = [DateTimeOffset]::UtcNow.AddSeconds(50)
    while (!(Test-Path -LiteralPath "$archive/$walSegment" -PathType Leaf) -and [DateTimeOffset]::UtcNow -lt $deadline) {
        Start-Sleep -Milliseconds 200
    }
    $walHash = (Get-FileHash -LiteralPath "$archive/$walSegment").Hash.ToLowerInvariant()
    [ordered]@{targetName=$point;targetLsn=$targetLsn;walSegment=$walSegment;walSha256=$walHash;originalGeneration=$originalGeneration} |
        ConvertTo-Json | Set-Content -LiteralPath "$run/pitr-target.json"
    Run-Control 'Sync' @('-TargetLsn',$targetLsn)
    Run-Control 'FenceOriginalNodes'
    Assert-PortClosed $data.primary.port
    Assert-PortClosed $data.standby.port
    Assert-PortClosed $apiPort
    $fencePath = Join-Path $lab 'original-nodes-fenced.json'
    $fenceHash = (Get-FileHash -LiteralPath $fencePath).Hash.ToLowerInvariant()
    Stage 'Verified physical backup and WAL archive; source primary and standby fenced'

    if (Test-Path -LiteralPath $ownedRestore) { throw 'PITR destination already exists.' }
    & "$bin/pg_verifybackup.exe" $verifiedBackup
    if ($LASTEXITCODE -ne 0) { throw 'Pre-restore physical backup verification failed.' }
    Copy-Item -LiteralPath $verifiedBackup -Destination $ownedRestore -Recurse
    [IO.File]::AppendAllText("$ownedRestore/postgresql.auto.conf", @"

port = $($data.restore.port)
synchronous_standby_names = ''
synchronous_commit = on
default_transaction_read_only = on
archive_mode = off
restore_command = '$restoreCommand'
recovery_target_name = '$point'
recovery_target_timeline = 'current'
recovery_target_action = 'promote'
"@)
    [IO.File]::WriteAllText("$ownedRestore/recovery.signal", '')
    [ordered]@{
        formatVersion=1; state='Quarantined'; restoredPort=$data.restore.port
        backupManifestSha256=$backup.manifestSha256; backupWalRanges=$backup.walRanges
        automaticSenderActivationAllowed=$false; databaseDefaultReadOnly=$true
    } | ConvertTo-Json -Depth 6 | Set-Content -LiteralPath $data.restoreQuarantineFile
    $pgctl = Start-Process -FilePath "$bin/pg_ctl.exe" -ArgumentList @('-D',('"' + $ownedRestore + '"'),'-l',('"' + (Join-Path $lab 'restore.log') + '"'),'-w','-t','75','start') -WindowStyle Hidden -PassThru -RedirectStandardOutput "$run/restore-control.out.log" -RedirectStandardError "$run/restore-control.err.log"
    if (!$pgctl.WaitForExit(80000) -or $pgctl.ExitCode -ne 0) { throw 'Owned PITR PostgreSQL node did not start.' }
    # pg_ctl -w returns as soon as the hot standby accepts connections, before named-point promotion finishes.
    $deadline = [DateTimeOffset]::UtcNow.AddSeconds(60)
    do {
        $promoted = (Invoke-Sql $data.restore.port 'SELECT pg_is_in_recovery();') -ceq 'f'
        if (!$promoted) { Start-Sleep -Milliseconds 200 }
    } while (!$promoted -and [DateTimeOffset]::UtcNow -lt $deadline)
    if (!$promoted -or
        (Invoke-Sql $data.restore.port 'SELECT system_identifier::text FROM pg_control_system();') -cne $data.systemIdentifier -or
        (Invoke-Sql $data.restore.port 'SHOW default_transaction_read_only;') -cne 'on') {
        throw 'Native PITR was not promoted to the same identified system in read-only quarantine.'
    }
    if ((Invoke-Sql $data.restore.port 'SELECT label FROM w80_pitr_markers ORDER BY label;') -cne 'before-target') {
        throw 'Recovered marker does not match the exact target window.'
    }
    $deniedSqlState = Invoke-Sql $data.restore.port "INSERT INTO w80_pitr_markers VALUES ('quarantine-denied');" -WriteDenied
    Assert-States $baseline (Invoke-Probe 'observe' $data.restore.connectionStringFile)
    $restoreEvidencePath = Join-Path $run 'pitr-evidence.json'
    [ordered]@{
        physicalBackupVerified=$true; backupManifestSha256=$backup.manifestSha256
        targetLsn=$targetLsn; targetName=$point; archivedWalSegment=$walSegment; archivedWalSha256=$walHash
        targetMarkerPresent=$true; postTargetMarkerAbsent=$true; sameSystemIdentifier=$true
        quarantinedWriteDeniedSqlState=$deniedSqlState
    } | ConvertTo-Json -Depth 5 | Set-Content -LiteralPath $restoreEvidencePath
    $restoreHash = (Get-FileHash -LiteralPath $restoreEvidencePath).Hash.ToLowerInvariant()
    Stage 'Native PITR replay reached only the named WAL target and rejected writes in quarantine'

    [ordered]@{version=1;generation=$restoredGeneration;state='quarantined'} |
        ConvertTo-Json -Compress | Set-Content -LiteralPath $operatorAnchor -NoNewline -Encoding utf8NoBOM
    $anchorHash = (Get-FileHash -LiteralPath $operatorAnchor).Hash.ToLowerInvariant()
    Run-Control 'AdmitRestoreMaintenance' @('-TargetGeneration',$restoredGeneration,'-ExternalAnchorSha256',$anchorHash)
    $admit = Start-Api $candidate 'generation-admission' $data.restore.connectionStringFile $restoredGeneration $originalGeneration $fenceHash $restoreHash
    if (!$admit.WaitForExit(45000) -or $admit.ExitCode -ne 0 -or
        (Get-Content -LiteralPath "$run/generation-admission.out.log" -Raw) -notmatch 'HTTP and workers were not started') {
        throw 'Candidate generation admission did not complete in offline maintenance mode.'
    }
    Assert-PortClosed $apiPort
    $stale = Start-Api $candidate 'stale-generation' $data.restore.connectionStringFile $originalGeneration
    if (!$stale.WaitForExit(45000) -or $stale.ExitCode -eq 0 -or
        (Get-Content -LiteralPath "$run/stale-generation.err.log" -Raw) -notmatch 'GovernanceGenerationException') {
        throw 'Old generation refusal was not confirmed.'
    }
    $fresh = Start-Api $candidate 'fresh-generation' $data.restore.connectionStringFile $restoredGeneration
    Wait-Api $fresh 'fresh-generation'
    Stop-Api $fresh
    Assert-States $baseline (Invoke-Probe 'observe' $data.restore.connectionStringFile)
    if ((Invoke-Sql $data.restore.port "SELECT count(*) FROM governance_generation_admissions WHERE generation='$restoredGeneration' AND previous_generation='$originalGeneration' AND fenced_workers_sha256='$fenceHash' AND restore_sha256='$restoreHash';") -cne '1') {
        throw 'Fresh generation ledger did not bind exact fence and restore evidence.'
    }
    Assert-PortClosed $data.primary.port
    Assert-PortClosed $data.standby.port
    [ordered]@{
        originalSource=(& git -C "$PSScriptRoot/released-v08" rev-parse HEAD)
        candidateSource=$candidateSource; candidateArchiveSha256=$archiveHash
        oldRuntimeSha256=(Get-FileHash -LiteralPath $oldAssembly).Hash.ToLowerInvariant()
        candidateRuntimeSha256=(Get-FileHash -LiteralPath $candidate).Hash.ToLowerInvariant()
        postgresVersion=$data.postgresVersion; baselineSnapshotHashes=$baseline.States
        upgradedAndReopened=$true; migrationInterruptedAndRetried=[bool]$InterruptMigration
        physicalBackupVerified=$true; nativeWalTargetRecovered=$true; bothOriginalDatabasePortsFenced=$true
        quarantineWriteDeniedSqlState=$deniedSqlState; offlineAdmissionProducedNoHttp=$true
        staleGenerationRefused=$true; freshGenerationReady=$true; legacySnapshotHashesPreserved=$true
        priorUnknownRemainedUnknown=$true; kafkaWorkDispatched=$false
        fenceEvidenceSha256=$fenceHash; restoreEvidenceSha256=$restoreHash
    } | ConvertTo-Json -Depth 8 | Set-Content -LiteralPath "$run/evidence.json"
    Stage "W80 physical PITR evidence passed: $run/evidence.json"
} finally {
    foreach ($process in $apps) { Stop-Api $process }
    Run-Control 'Stop'
}

