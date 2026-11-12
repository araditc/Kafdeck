#requires -Version 7.4
param([Parameter(Mandatory)][ValidateSet('sqlite','postgres')][string]$Provider, [switch]$Interrupt, [switch]$Fence,
    [Parameter(Mandatory)][string]$CandidateAssembly,
    [Parameter(Mandatory)][ValidatePattern('^[0-9a-f]{64}$')][string]$ExpectedCandidateHash,
    [Parameter(Mandatory)][ValidatePattern('^[0-9a-f]{40}$')][string]$SourceSha,
    [Parameter(Mandatory)][ValidatePattern('^[0-9a-f]{64}$')][string]$CandidateArchiveSha256)
$ErrorActionPreference = 'Stop'
$repo = (Resolve-Path "$PSScriptRoot/../..").Path
$run = Join-Path $PSScriptRoot "$Provider-$([Guid]::NewGuid().ToString('N'))"
$null = New-Item -ItemType Directory -Path $run
$probe = Join-Path $PSScriptRoot 'probe/bin/Release/net10.0/W80.Probe.dll'
$oldAssembly = Join-Path $PSScriptRoot 'v08-build/Kafdeck.Api.dll'
# Signed-only adapter: the wrapper verified signature, manifest and complete archive.
$newAssembly = (Resolve-Path -LiteralPath $CandidateAssembly).Path
if ([IO.Path]::GetFileName($newAssembly) -cne 'Kafdeck.Api.exe' -or
    (Get-FileHash -LiteralPath $newAssembly).Hash.ToLowerInvariant() -cne $ExpectedCandidateHash) {
    throw 'Exact signed-candidate executable identity mismatch.'
}
$candidateSource = $SourceSha
$candidateArchiveHash = $CandidateArchiveSha256
$dotnet = (Get-Command dotnet).Source
$port = if ($Fence) { 25453 } elseif ($Provider -eq 'sqlite') { 25451 } else { 25452 }
$pgConnection = @('-h','127.0.0.1','-p','25433','-U','g2_recovery_owner','-w')
$base = "http://127.0.0.1:$port"
$generation = [Guid]::NewGuid().ToString('D')
$keyFile = Join-Path $run 'synthetic-material-key.txt'
[IO.File]::WriteAllText($keyFile, [Convert]::ToHexString([Security.Cryptography.RandomNumberGenerator]::GetBytes(32)))
$location = if ($Provider -eq 'sqlite') { Join-Path $run 'source.db' } else { Join-Path $PSScriptRoot 'pg-lab/primary-connection-string.txt' }

function Start-Runtime([string]$Assembly, [string]$Name, [string]$Database, [string]$Generation = '',
    [string]$PreviousGeneration = '', [string]$FenceHash = '', [string]$RestoreHash = '') {
    $settings = @{
        Kafdeck__Deployment__ListenUrls__0 = $base
        Kafdeck__Deployment__AccessMode = 'Oidc'
        Kafdeck__Deployment__Oidc__Issuer = 'https://localhost:25461/realms/w80-fixture'
        Kafdeck__Deployment__Oidc__ClientId = 'w80-fixture'
        Kafdeck__Administration__Mutations__Enabled = 'true'
        Kafdeck__Administration__Mutations__BackgroundDataWorkersEnabled = 'false'
        Kafdeck__Administration__Mutations__MaterialDigestKey = "file:$keyFile"
        Kafdeck__Administration__Mutations__Persistence__Provider = if ($Provider -eq 'sqlite') { 'Sqlite' } else { 'PostgreSql' }
        Kafdeck__Administration__Mutations__Persistence__ExecutionMode = 'Standalone'
    }
    if ($Provider -eq 'sqlite') { $settings.Kafdeck__Administration__Mutations__Persistence__SqliteDatabasePath = $Database }
    else { $settings.Kafdeck__Administration__Mutations__Persistence__ConnectionString = "file:$Database" }
    if ($Generation) { $settings.Kafdeck__Governance__DeploymentGeneration = $Generation }
    $arguments = @('--contentRoot', ('"' + (Split-Path $Assembly) + '"'))
    if ($PreviousGeneration) {
        $arguments += '--governance-generation-admit'
        $settings.Kafdeck__Governance__RestoreMaintenanceMode = 'true'
        $settings.Kafdeck__Governance__PreviousDeploymentGeneration = $PreviousGeneration
        $settings.Kafdeck__Governance__FencedWorkersEvidenceSha256 = $FenceHash
        $settings.Kafdeck__Governance__RestoreEvidenceSha256 = $RestoreHash
    }
    if ($Assembly.EndsWith('.dll')) {
        $entry = $dotnet
        $arguments = @('"' + $Assembly + '"') + $arguments
    } else { $entry = $Assembly }
    Start-Process -FilePath $entry -ArgumentList $arguments -WorkingDirectory $run -PassThru -WindowStyle Hidden -Environment $settings -RedirectStandardOutput (Join-Path $run "$Name.out.log") -RedirectStandardError (Join-Path $run "$Name.err.log")
}

function Wait-Ready($Process, [string]$Name) {
    for ($i=0; $i -lt 40 -and !$Process.HasExited; $i++) {
        try {
            $reply = Invoke-WebRequest -NoProxy -TimeoutSec 2 -Uri "$base/healthz"
            if ($reply.StatusCode -eq 200) { return }
        } catch { Start-Sleep -Milliseconds 250 }
    }
    throw "Runtime $Name did not become healthy; see $run"
}

function Stop-Runtime($Process) {
    if ($Process -and !$Process.HasExited) {
        Stop-Process -InputObject $Process
        if (!$Process.WaitForExit(10000)) { throw 'Exact runtime process did not stop.' }
    }
}

function Invoke-Probe([string]$Action, [string]$Database, [string]$Backup = '') {
    $result = & $dotnet $probe $Action $Provider $Database $Backup
    if ($LASTEXITCODE -ne 0) { throw "Fixture probe $Action failed." }
    $result
}

function Assert-States($Expected, $Actual) {
    if (($Expected.States | ConvertTo-Json -Compress) -cne ($Actual.States | ConvertTo-Json -Compress)) {
        throw 'The original mutation snapshots or states changed.'
    }
}

function Invoke-Pg([string]$Tool, [string[]]$Arguments) {
    $binary = Join-Path $repo "deploy/dev/.generated/g2/postgres/pgsql/bin/$Tool.exe"
    $previous = $env:PGPASSWORD
    try {
        $env:PGPASSWORD = [IO.File]::ReadAllText((Join-Path $PSScriptRoot 'pg-lab/admin-password.txt')).Trim()
        & $binary @Arguments
        if ($LASTEXITCODE -ne 0) { throw "PostgreSQL $Tool failed." }
    } finally { $env:PGPASSWORD = $previous }
}

function Assert-RestoredSqliteGeneration([string]$Database) {
    $postUpgradeBackup = Join-Path $run 'post-upgrade-backup.db'
    $restored = Join-Path $run 'post-upgrade-restored.db'
    Invoke-Probe 'backup' $Database $postUpgradeBackup
    Invoke-Probe 'backup' $postUpgradeBackup $restored
    $fenced = Join-Path $run 'fenced-workers.json'
    if (!$new.HasExited -or !$old.HasExited) { throw 'Original fixture processes must be stopped before admission.' }
    [ordered]@{ oldProcessExited=$old.HasExited; candidateProcessExited=$new.HasExited; originalPath=$Database; restoredPath=$restored } |
        ConvertTo-Json | Set-Content -LiteralPath $fenced
    $fenceHash = (Get-FileHash -LiteralPath $fenced).Hash.ToLowerInvariant()
    $restoreHash = (Get-FileHash -LiteralPath $postUpgradeBackup).Hash.ToLowerInvariant()
    $next = [Guid]::NewGuid().ToString('D')
    $admission = Start-Runtime $newAssembly 'candidate-admit-restored-generation' $restored $next $generation $fenceHash $restoreHash
    try {
        if (!$admission.WaitForExit(25000) -or $admission.ExitCode -ne 0) { throw 'Offline restored generation admission failed.' }
        if ((Get-Content -LiteralPath (Join-Path $run 'candidate-admit-restored-generation.out.log') -Raw) -notmatch 'HTTP and workers were not started') {
            throw 'Quarantined admission evidence missing.'
        }
    } finally { Stop-Runtime $admission }
    $stale = Start-Runtime $newAssembly 'candidate-stale-generation' $restored $generation
    try {
        if (!$stale.WaitForExit(25000) -or $stale.ExitCode -eq 0) { throw 'Stale generation was not refused.' }
        if ((Get-Content -LiteralPath (Join-Path $run 'candidate-stale-generation.err.log') -Raw) -notmatch 'GovernanceGenerationException') {
            throw 'Stale generation refusal was caused by an unrelated error.'
        }
    } finally { Stop-Runtime $stale }
    $resumed = Start-Runtime $newAssembly 'candidate-current-generation' $restored $next
    try {
        Wait-Ready $resumed 'candidate-current-generation'
        Assert-States $original (Invoke-Probe 'observe' $restored | ConvertFrom-Json)
    } finally { Stop-Runtime $resumed }
}

function Assert-InterruptedPostgresMigration([string]$DatabaseName, [string]$Database) {
    $lockFile = Join-Path $run 'hold-catalog-migration.sql'
    [IO.File]::WriteAllText($lockFile, "SET statement_timeout=0;`nSELECT pg_advisory_lock(64964007301);`nSELECT pg_sleep(65);`n")
    $psql = Join-Path $repo 'deploy/dev/.generated/g2/postgres/pgsql/bin/psql.exe'
    $postgresArgs = $pgConnection + @('-X','-v','ON_ERROR_STOP=1','-d',$DatabaseName,'-f',('"' + $lockFile + '"'))
    $pass = [IO.File]::ReadAllText((Join-Path $PSScriptRoot 'pg-lab/admin-password.txt')).Trim()
    $lockerName = 'w80_lock_' + [Guid]::NewGuid().ToString('N')
    $locker = Start-Process -FilePath $psql -ArgumentList $postgresArgs -WorkingDirectory $run -PassThru -WindowStyle Hidden -Environment @{PGPASSWORD=$pass;PGAPPNAME=$lockerName} -RedirectStandardOutput (Join-Path $run 'lock.out.log') -RedirectStandardError (Join-Path $run 'lock.err.log')
    $candidate = $null
    $psqlArgs = $pgConnection + @('-XAt','-d',$DatabaseName,'-c')
    try {
        # Check the actual advisory key instead of a fixed sleep.
        $exists = "SELECT count(*) FROM pg_locks JOIN pg_stat_activity USING(pid) WHERE application_name='$lockerName' AND locktype='advisory' AND (classid::bigint << 32) + objid::bigint=64964007301 AND granted;"
        for ($i=0; $i -lt 30 -and !$locker.HasExited; $i++) {
            if ((Invoke-Pg 'psql' ($psqlArgs + @($exists))) -eq '1') { break }
            Start-Sleep -Milliseconds 100
        }
        if ($i -ge 30 -or $locker.HasExited) { throw 'Catalog migration lock was not acquired.' }
        $candidate = Start-Runtime $newAssembly 'candidate-interrupted' $Database $generation
        $waiting = "SELECT count(*) FROM pg_stat_activity WHERE datname=current_database() AND wait_event='advisory' AND query='SELECT pg_advisory_xact_lock(64964007301)';"
        for ($i=0; $i -lt 50 -and !$candidate.HasExited; $i++) {
            if ((Invoke-Pg 'psql' ($psqlArgs + @($waiting))) -eq '1') { break }
            Start-Sleep -Milliseconds 100
        }
        if ($i -ge 50 -or $candidate.HasExited) { throw 'Candidate did not block at the selected catalog migration.' }
        Stop-Runtime $candidate
        $afterInterrupt = Invoke-Probe 'observe' $Database | ConvertFrom-Json
        if ($afterInterrupt.Tables -notcontains 'governance_generation_schema' -or $afterInterrupt.Tables -contains 'catalog_owned_schema') {
            throw 'Interruption was not observed between committed generation and pending catalog migrations.'
        }
        Assert-States $original $afterInterrupt
    } finally {
        Stop-Runtime $candidate
        # Killing the psql client alone can leave pg_sleep and its lock alive on the server.
        Invoke-Pg 'psql' ($psqlArgs + @("SELECT pg_terminate_backend(pid, 5000) FROM pg_stat_activity WHERE datname=current_database() AND application_name='$lockerName';")) | Out-Null
        Stop-Runtime $locker
    }
}

$old = $null
$new = $null
function Assert-InterruptedSqliteMigration {
    $ready = Join-Path $run 'sqlite-lock-ready.txt'
    $release = Join-Path $run 'sqlite-lock-release.txt'
    $python = (Get-Command python).Source
    $lockArguments = @('"'+(Join-Path $PSScriptRoot 'hold-signed-sqlite.py')+'"','"'+$location+'"','"'+$ready+'"','"'+$release+'"')
    $locker = Start-Process -FilePath $python -ArgumentList $lockArguments -WorkingDirectory $run -WindowStyle Hidden -PassThru -RedirectStandardOutput (Join-Path $run 'sqlite-lock.out.log') -RedirectStandardError (Join-Path $run 'sqlite-lock.err.log')
    $candidate = $null
    try {
        for ($i=0; $i -lt 50 -and !$locker.HasExited; $i++) {
            if (Test-Path -LiteralPath $ready) { break }
            Start-Sleep -Milliseconds 100
        }
        if (!(Test-Path -LiteralPath $ready) -or $locker.HasExited) { throw 'SQLite exclusive migration lock was not acquired.' }
        $candidate = Start-Runtime $newAssembly 'candidate-sqlite-interrupted' $location $generation
        for ($i=0; $i -lt 80 -and !$candidate.HasExited; $i++) {
            if (@((Get-Process -Id $candidate.Id).Modules | Where-Object ModuleName -eq 'e_sqlite3.dll').Count) { break }
            Start-Sleep -Milliseconds 100
        }
        if ($candidate.HasExited -or $i -ge 80) { throw 'Signed candidate did not enter native SQLite initialization.' }
        Start-Sleep -Milliseconds 500
        $healthy = $false
        try { $healthy = (Invoke-WebRequest -NoProxy -TimeoutSec 1 -Uri "$base/healthz").StatusCode -eq 200 } catch {}
        if ($healthy -or $candidate.HasExited -or $locker.HasExited) { throw 'SQLite interruption did not occur at the held migration boundary.' }
        Stop-Runtime $candidate
    } finally {
        Stop-Runtime $candidate
        [IO.File]::WriteAllText($release, 'release')
        if (!$locker.WaitForExit(5000)) { Stop-Process -InputObject $locker; throw 'SQLite fixture lock did not release.' }
    }
    $observed = Invoke-Probe 'observe' $location | ConvertFrom-Json
    Assert-States $original $observed
    if ($observed.Tables -contains 'governance_generation_schema' -or $observed.Tables -contains 'catalog_owned_schema') { throw 'Blocked SQLite migration unexpectedly committed.' }
    @{exclusiveLockHeld=$true;nativeSqliteLoaded=$true;candidateKilledBeforeHealth=$true;legacySnapshotsUnchanged=$true;phase='startup migration blocked by a real SQLite exclusive write lock'} |
        ConvertTo-Json | Set-Content -LiteralPath (Join-Path $run 'sqlite-interruption.json')
}
try {
    $check = [Net.Sockets.TcpListener]::new([Net.IPAddress]::Loopback,$port)
    try { $check.Start() } finally { $check.Stop() }
    if ($Provider -eq 'postgres') {
        $sourceDatabase = 'w80_' + [Guid]::NewGuid().ToString('N')
        Invoke-Pg 'createdb' ($pgConnection + @($sourceDatabase))
        $sourceConnection = Join-Path $run 'source-connection.txt'
        [IO.File]::WriteAllText($sourceConnection, [IO.File]::ReadAllText($location).Replace('Database=postgres;', "Database=$sourceDatabase;"))
        $location = $sourceConnection
    }
    $old = Start-Runtime $oldAssembly 'v08' $location
    Wait-Ready $old 'v08'
    Stop-Runtime $old
    $original = Invoke-Probe 'seed' $location | ConvertFrom-Json
    if (($original.States.State -join ',') -cne '3,4,9' -or $original.Tables -contains 'governance_deployment_generation') {
        throw 'v0.8 seed failed to establish all three legacy states.'
    }
    if ($Provider -eq 'sqlite') {
        $backup = Join-Path $run 'backup.db'
        Invoke-Probe 'backup' $location $backup
    } else {
        $backup = Join-Path $run 'v08.pgdump'
        Invoke-Pg 'pg_dump' ($pgConnection + @('-Fc','-d',$sourceDatabase,'-f',$backup))
        if ((Invoke-Pg 'pg_restore' @('-l',$backup) | Select-String -SimpleMatch 'kafdeck_mutation_operations').Count -lt 1) {
            throw 'PostgreSQL archive table-of-contents is missing the populated mutation fixture.'
        }
    }
    $backupHash = (Get-FileHash -LiteralPath $backup -Algorithm SHA256).Hash.ToLowerInvariant()
    if ($Provider -eq 'postgres' -and $Interrupt) {
        Assert-InterruptedPostgresMigration $sourceDatabase $location
    }
    if ($Provider -eq 'sqlite' -and $Interrupt) { Assert-InterruptedSqliteMigration }
    $new = Start-Runtime $newAssembly 'candidate' $location $generation
    Wait-Ready $new 'candidate'
    Stop-Runtime $new
    $upgraded = Invoke-Probe 'observe' $location | ConvertFrom-Json
    Assert-States $original $upgraded
    foreach ($table in @('catalog_owned_schema','governance_generation_schema')) {
        if ($upgraded.Tables -notcontains $table) { throw "Candidate did not install $table." }
    }
    $new = Start-Runtime $newAssembly 'candidate-repeat' $location $generation
    Wait-Ready $new 'candidate-repeat'
    Stop-Runtime $new
    $repeated = Invoke-Probe 'observe' $location | ConvertFrom-Json
    Assert-States $original $repeated
    if (($upgraded.Tables -join ',') -cne ($repeated.Tables -join ',')) { throw 'Candidate schema was not stable on re-entry.' }
    if ($Provider -eq 'sqlite' -and $Fence) { Assert-RestoredSqliteGeneration $location }
    if ($Provider -eq 'sqlite') {
        $restored = Join-Path $run 'restored.db'
        Invoke-Probe 'backup' $backup $restored
    } else {
        $restoreDatabase = 'w80_' + [Guid]::NewGuid().ToString('N')
        Invoke-Pg 'createdb' ($pgConnection + @($restoreDatabase))
        Invoke-Pg 'pg_restore' ($pgConnection + @('--exit-on-error','-d',$restoreDatabase,$backup))
        $restored = Join-Path $run 'restore-connection.txt'
        [IO.File]::WriteAllText($restored, [IO.File]::ReadAllText($location).Replace("Database=$sourceDatabase;", "Database=$restoreDatabase;"))
    }
    $old = Start-Runtime $oldAssembly 'v08-restored' $restored
    Wait-Ready $old 'v08-restored'
    Stop-Runtime $old
    $restoredData = Invoke-Probe 'observe' $restored | ConvertFrom-Json
    Assert-States $original $restoredData
    if (($original.Tables -join ',') -cne ($restoredData.Tables -join ',')) { throw 'Restore did not retain the old schema.' }
    [ordered]@{
        provider = $Provider; originalSource = (& git -C "$PSScriptRoot/released-v08" rev-parse HEAD)
        candidateSource = $candidateSource
        originalAssemblySha256 = (Get-FileHash $oldAssembly).Hash.ToLowerInvariant()
        candidateAssemblySha256 = (Get-FileHash $newAssembly).Hash.ToLowerInvariant()
        candidateArchiveSha256 = $candidateArchiveHash
        originalApiHealthy = $true; candidateApiHealthy = $true; replayApiHealthy = $true
        oldSnapshotHashesUnchanged = $true; original = $original; upgraded = $upgraded
        preUpgradeBackupSha256 = $backupHash
        sqliteVerifiedPreUpgradeRestore = ($Provider -eq 'sqlite')
        postgresVerifiedLogicalPreUpgradeRestore = ($Provider -eq 'postgres')
        interruptedPostgresBetweenStoresAndRetried = ($Provider -eq 'postgres' -and $Interrupt)
        sqliteInterruptedMigrationAndRetried = ($Provider -eq 'sqlite' -and $Interrupt)
        sqliteOfflineNewGenerationAdmittedAndStaleRefused = ($Provider -eq 'sqlite' -and $Fence)
        fixtureApprovalSynthetic = $true; noKafkaDispatched = $true
    } | ConvertTo-Json -Depth 8 | Set-Content -LiteralPath (Join-Path $run 'evidence.json')
    Write-Output "Evidence: $run/evidence.json"
}
finally {
    Stop-Runtime $new
    Stop-Runtime $old
}
