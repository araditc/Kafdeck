#requires -Version 7.0
param(
    [ValidateSet('Prepare','Start','Status','Sync','Backup','Failover','Restore','Stop')][string]$Action = 'Prepare',
    [string]$OutputDirectory = "$PSScriptRoot/.generated/g2-recovery/postgres",
    [string]$BinDirectory,
    [ValidateRange(1024,65535)][int]$PrimaryPort = 15433,
    [ValidateRange(1024,65535)][int]$StandbyPort = 15434,
    [ValidateRange(1024,65535)][int]$RestorePort = 15435,
    [ValidatePattern('^[0-9A-Fa-f]+/[0-9A-Fa-f]+$')][string]$TargetLsn,
    [ValidateSet('fast','immediate')][string]$PrimaryStopMode = 'fast'
)
$ErrorActionPreference = 'Stop'
$lab = [IO.Path]::GetFullPath($OutputDirectory)
$manifestFile = Join-Path $lab 'lab.json'
$pathComparison = if ($IsWindows) { [StringComparison]::OrdinalIgnoreCase } else { [StringComparison]::Ordinal }
if (@($PrimaryPort,$StandbyPort,$RestorePort | Sort-Object -Unique).Count -ne 3) { throw 'Each laboratory endpoint requires its own port.' }
if ([string]::IsNullOrWhiteSpace($BinDirectory)) {
    if ($IsWindows) { $BinDirectory = "$PSScriptRoot/.generated/g2/postgres/pgsql/bin" }
    else {
        $config = Get-Command pg_config -ErrorAction SilentlyContinue
        if ($null -eq $config) { throw 'Pass an existing PostgreSQL BinDirectory; this script installs no OS packages.' }
        $BinDirectory = (& $config.Source --bindir).Trim()
    }
}
$bin = [IO.Path]::GetFullPath($BinDirectory)
$suffix = if ($IsWindows) { '.exe' } else { '' }
function Tool([string]$Name) {
    $path = Join-Path $bin ($Name + $suffix)
    if (!(Test-Path -LiteralPath $path)) { throw "Missing PostgreSQL tool: $Name" }
    return $path
}
function NativeOk([string]$Label) { if ($LASTEXITCODE -ne 0) { throw "$Label failed; inspect ignored laboratory logs (exit $LASTEXITCODE)." } }
function Child([string]$Relative) {
    $path = [IO.Path]::GetFullPath((Join-Path $lab $Relative))
    $prefix = $lab.TrimEnd([IO.Path]::DirectorySeparatorChar) + [IO.Path]::DirectorySeparatorChar
    if (!$path.StartsWith($prefix,$pathComparison)) { throw 'Laboratory path escaped its explicit root.' }
    return $path
}
function SaveJson([string]$Path,$Value) {
    $temporary = $Path + '.tmp-' + [Guid]::NewGuid().ToString('N')
    [IO.File]::WriteAllText($temporary, ($Value | ConvertTo-Json -Depth 12), [Text.UTF8Encoding]::new($false))
    [IO.File]::Move($temporary,$Path,$true)
}
function PrivateFile([string]$Path,[string]$Value) {
    [IO.File]::WriteAllText($Path,$Value,[Text.UTF8Encoding]::new($false))
    if (!$IsWindows) { & chmod 600 $Path; NativeOk 'Private credential permissions' }
}
function PgPath([string]$Path) { return $Path.Replace('\','/').Replace("'","''") }
function AssertExactPath([string]$Recorded,[string]$Relative) {
    if ([string]::IsNullOrWhiteSpace($Recorded) -or ![string]::Equals([IO.Path]::GetFullPath($Recorded),(Child $Relative),$pathComparison)) {
        throw 'Recorded laboratory path does not match its exact owned location.'
    }
    # A lexical child must not redirect control/copy operations through a link.
    $cursor = [IO.Path]::GetFullPath($Recorded)
    while ($cursor.Length -ge $lab.Length) {
        $item = Get-Item -LiteralPath $cursor -Force -ErrorAction SilentlyContinue
        if ($null -ne $item -and ($item.Attributes -band [IO.FileAttributes]::ReparsePoint)) { throw 'Linked laboratory paths are not admitted.' }
        if ([string]::Equals($cursor,$lab,$pathComparison)) { break }
        $cursor = [IO.Path]::GetDirectoryName($cursor)
    }
}
function ValidateManifest {
    if ($metadata.formatVersion -ne 1 -or $metadata.bindAddress -ne '127.0.0.1') { throw 'Unsupported recovery laboratory manifest.' }
    foreach ($entry in @(@('adminPasswordFile','admin-password.txt'),@('replicationPasswordFile','replication-password.txt'),
            @('replicationPassfile','replication.pgpass'),@('fenceFile','old-primary-fence.json'),
            @('latestBackupFile','latest-backup.json'),@('restoreQuarantineFile','restore-quarantine.json'))) {
        AssertExactPath $metadata.($entry[0]) $entry[1]
    }
    $ports = @()
    foreach ($name in @('primary','standby','restore')) {
        $endpoint = $metadata.$name
        $parsedPort = 0
        if ($endpoint.name -cne $name -or ![int]::TryParse([string]$endpoint.port,[ref]$parsedPort) -or $parsedPort -lt 1024 -or $parsedPort -gt 65535) {
            throw 'Invalid recorded recovery endpoint identity or port.'
        }
        $ports += $parsedPort
        AssertExactPath $endpoint.dataDirectory $name
        AssertExactPath $endpoint.connectionStringFile ($name + '-connection-string.txt')
    }
    if (@($ports | Sort-Object -Unique).Count -ne 3) { throw 'Recorded recovery endpoint ports collide.' }
    if ($metadata.state -ne 'Preparing' -and [string]$metadata.systemIdentifier -notmatch '^\d{10,22}$') { throw 'Recovery cluster system identifier is missing.' }
    if (Test-Path -LiteralPath $metadata.fenceFile) {
        $fence = Get-Content -LiteralPath $metadata.fenceFile -Raw | ConvertFrom-Json
        AssertExactPath $fence.oldPrimaryDataDirectory 'primary'
        if ($fence.oldPrimaryPort -ne $metadata.primary.port -or $fence.oldPrimaryStopped -ne $true -or $fence.oldPrimaryPortClosed -ne $true) {
            throw 'Recorded primary fencing evidence is invalid.'
        }
        # The separately persisted fence wins even after a crash between fence
        # persistence and manifest replacement. Never revive the old primary.
        $metadata.oldPrimaryFenced = $true
        if ($Action -in @('Start','Backup','Restore') -and ((Running $metadata.primary) -or (PortListening $metadata.primary.port))) {
            throw 'A fenced old primary is reachable; new writer activation is forbidden.'
        }
    } elseif ($metadata.oldPrimaryFenced -eq $true) { throw 'Fenced manifest lacks its required independent fence record.' }
}
function AssertNodeIdentity($Endpoint) {
    if (!(Test-Path -LiteralPath (Join-Path $Endpoint.dataDirectory 'PG_VERSION')) -or !$metadata.systemIdentifier) { return }
    $previousLocale = $env:LC_ALL
    try {
        $env:LC_ALL = 'C'
        $control = & (Tool 'pg_controldata') $Endpoint.dataDirectory 2>> (Child 'control-identity-errors.log')
        NativeOk 'Owned database control identity inspection'
    } finally { $env:LC_ALL = $previousLocale }
    $identifier = @($control | Select-String '^Database system identifier:\s+(\d+)\s*$')
    if ($identifier.Count -ne 1 -or $identifier[0].Matches[0].Groups[1].Value -ne [string]$metadata.systemIdentifier) {
        throw 'Owned data directory belongs to a different PostgreSQL system.'
    }
    if ((Running $Endpoint) -and (Query $Endpoint.port 'SELECT system_identifier::text FROM pg_control_system()') -ne [string]$metadata.systemIdentifier) {
        throw 'Live endpoint belongs to a different PostgreSQL system.'
    }
}
function Query([int]$Port,[string]$Sql) {
    $previous = $env:PGPASSWORD
    $previousTimeout = $env:PGCONNECT_TIMEOUT
    try {
        $env:PGPASSWORD = [IO.File]::ReadAllText($metadata.adminPasswordFile)
        $env:PGCONNECT_TIMEOUT = '5'
        $result = & (Tool 'psql') -X -A -t -v ON_ERROR_STOP=1 -h 127.0.0.1 -p $Port -U g2_recovery_owner -d postgres -c $Sql 2>> (Child 'psql-errors.log')
        NativeOk 'Bounded laboratory SQL'
        return (($result | Out-String).Trim())
    } finally { $env:PGPASSWORD=$previous; $env:PGCONNECT_TIMEOUT=$previousTimeout }
}
function Running($Endpoint) {
    if (!(Test-Path -LiteralPath (Join-Path $Endpoint.dataDirectory 'PG_VERSION'))) { return $false }
    & (Tool 'pg_ctl') -D $Endpoint.dataDirectory status *> $null
    return $LASTEXITCODE -eq 0
}
function StartNode($Endpoint) {
    AssertNodeIdentity $Endpoint
    if (Running $Endpoint) { return }
    # PowerShell native pipelines can wait for inherited descendant handles on
    # Windows. Redirect directly to files and wait only for pg_ctl, never its
    # long-lived postmaster descendants. No credentials are command arguments.
    $arguments = @('-D',$Endpoint.dataDirectory,'-l',(Child ($Endpoint.name + '.log')),'-w','-t','40','start')
    $launch = @{FilePath=(Tool 'pg_ctl');ArgumentList=($arguments | ForEach-Object { '"' + $_.Replace('"','\"') + '"' });
        RedirectStandardOutput=(Child ($Endpoint.name + '-control.log'));RedirectStandardError=(Child ($Endpoint.name + '-control-error.log'));PassThru=$true}
    if ($IsWindows) { $launch.WindowStyle='Hidden' }
    $controlProcess = Start-Process @launch
    try {
        if (!$controlProcess.WaitForExit(45000)) { throw 'Exact laboratory startup exceeded its finite control-process deadline.' }
        if ($controlProcess.ExitCode -ne 0) { throw 'Exact laboratory startup failed; inspect ignored node/control logs.' }
    } finally { $controlProcess.Dispose() }
}
function StopNode($Endpoint,[string]$Mode='fast') {
    AssertNodeIdentity $Endpoint
    if (!(Running $Endpoint)) { return }
    & (Tool 'pg_ctl') -D $Endpoint.dataDirectory -w -t 40 -m $Mode stop *> (Child ($Endpoint.name + '-stop-control.log'))
    NativeOk 'Exact laboratory node shutdown'
    if (Running $Endpoint) { throw 'The exact node still reports running after shutdown.' }
}
function PortListening([int]$Port) {
    $client = [Net.Sockets.TcpClient]::new()
    try { $client.ConnectAsync('127.0.0.1',$Port).WaitAsync([TimeSpan]::FromSeconds(1)).GetAwaiter().GetResult(); return $true }
    catch { return $false } finally { $client.Dispose() }
}
function WaitReplay([string]$Lsn) {
    if ($Lsn -notmatch '^[0-9A-Fa-f]+/[0-9A-Fa-f]+$') { throw 'Invalid WAL replay barrier.' }
    $deadline = [DateTimeOffset]::UtcNow.AddSeconds(35)
    do {
        if ((Query $metadata.standby.port "SELECT pg_is_in_recovery() AND pg_last_wal_replay_lsn() >= '$Lsn'::pg_lsn") -eq 't') { return }
        Start-Sleep -Milliseconds 100
    } while ([DateTimeOffset]::UtcNow -lt $deadline)
    throw 'Standby did not physically replay the required WAL barrier.'
}
function BackupFrom([int]$Port,[string]$Directory,[bool]$StandbySeed) {
    if (Test-Path -LiteralPath $Directory) { throw 'A backup destination already exists; data is never overwritten or deleted.' }
    $previous = $env:PGPASSWORD
    $previousTimeout = $env:PGCONNECT_TIMEOUT
    try {
        $env:PGPASSWORD = [IO.File]::ReadAllText($metadata.replicationPasswordFile)
        $env:PGCONNECT_TIMEOUT = '5'
        $arguments = @('-h','127.0.0.1','-p',"$Port",'-U','g2_replicator','-D',$Directory,'-X','stream','-c','fast','-w')
        if ($StandbySeed) { $arguments += @('-R','-C','-S','g2_standby') }
        & (Tool 'pg_basebackup') @arguments *> (Child 'basebackup.log')
        NativeOk 'Physical streaming base backup'
        & (Tool 'pg_verifybackup') $Directory *> (Child 'verifybackup.log')
        NativeOk 'Physical backup manifest verification'
    } finally { $env:PGPASSWORD=$previous; $env:PGCONNECT_TIMEOUT=$previousTimeout }
}

if ($Action -eq 'Prepare') {
    if (Test-Path -LiteralPath $lab) { throw 'Select a fresh OutputDirectory. Existing laboratories are never reset or deleted.' }
    foreach ($port in @($PrimaryPort,$StandbyPort,$RestorePort)) { if (PortListening $port) { throw "Laboratory port $port is already occupied." } }
    New-Item -ItemType Directory -Path $lab -Force | Out-Null
    if (!$IsWindows) { & chmod 700 $lab; NativeOk 'Private laboratory directory permissions' }
    $version = (& (Tool 'postgres') --version | Out-String).Trim()
    if ($version -notmatch 'PostgreSQL\) (1[6-9])\.') { throw 'This laboratory requires existing PostgreSQL16–19 tools of one identical version.' }
    $metadata = [ordered]@{
        formatVersion=1; postgresVersion=$version; binDirectory=$bin; bindAddress='127.0.0.1'; state='Preparing'
        adminPasswordFile=(Child 'admin-password.txt'); replicationPasswordFile=(Child 'replication-password.txt')
        replicationPassfile=(Child 'replication.pgpass'); fenceFile=(Child 'old-primary-fence.json')
        latestBackupFile=(Child 'latest-backup.json'); restoreQuarantineFile=(Child 'restore-quarantine.json')
        primary=@{name='primary';port=$PrimaryPort;dataDirectory=(Child 'primary');connectionStringFile=(Child 'primary-connection-string.txt')}
        standby=@{name='standby';port=$StandbyPort;dataDirectory=(Child 'standby');connectionStringFile=(Child 'standby-connection-string.txt')}
        restore=@{name='restore';port=$RestorePort;dataDirectory=(Child 'restore');connectionStringFile=(Child 'restore-connection-string.txt')}
    }
    PrivateFile $metadata.adminPasswordFile ([Convert]::ToHexString([Security.Cryptography.RandomNumberGenerator]::GetBytes(32)))
    PrivateFile $metadata.replicationPasswordFile ([Convert]::ToHexString([Security.Cryptography.RandomNumberGenerator]::GetBytes(32)))
    PrivateFile $metadata.replicationPassfile "127.0.0.1:*:replication:g2_replicator:$([IO.File]::ReadAllText($metadata.replicationPasswordFile))`n"
    foreach ($endpoint in @($metadata.primary,$metadata.standby,$metadata.restore)) {
        PrivateFile $endpoint.connectionStringFile "Host=127.0.0.1;Port=$($endpoint.port);Username=g2_recovery_owner;Password=$([IO.File]::ReadAllText($metadata.adminPasswordFile));Database=postgres;Pooling=false;Timeout=5;Command Timeout=15"
    }
    SaveJson $manifestFile $metadata
    & (Tool 'initdb') -D $metadata.primary.dataDirectory -U g2_recovery_owner "--pwfile=$($metadata.adminPasswordFile)" --encoding=UTF8 --locale=C --auth-local=scram-sha-256 --auth-host=scram-sha-256 *> (Child 'initdb.log')
    NativeOk 'Fresh isolated SCRAM primary initialization'
    [IO.File]::AppendAllText((Join-Path $metadata.primary.dataDirectory 'postgresql.conf'), @"

listen_addresses = '127.0.0.1'
port = $PrimaryPort
wal_level = replica
max_wal_senders = 6
max_replication_slots = 4
wal_keep_size = '128MB'
max_slot_wal_keep_size = '256MB'
hot_standby = on
fsync = on
synchronous_commit = remote_apply
synchronous_standby_names = ''
max_connections = 40
shared_buffers = '32MB'
statement_timeout = '20s'
"@)
    [IO.File]::WriteAllText((Join-Path $metadata.primary.dataDirectory 'pg_hba.conf'), "host all all 127.0.0.1/32 scram-sha-256`nhost replication g2_replicator 127.0.0.1/32 scram-sha-256`n")
    StartNode $metadata.primary
    $previous = $env:PGPASSWORD
    try {
        $env:PGPASSWORD = [IO.File]::ReadAllText($metadata.adminPasswordFile)
        "CREATE ROLE g2_replicator LOGIN REPLICATION PASSWORD '$([IO.File]::ReadAllText($metadata.replicationPasswordFile))'; CREATE TABLE g2_recovery_lab_probe(marker text PRIMARY KEY); INSERT INTO g2_recovery_lab_probe VALUES ('prepared');" |
            & (Tool 'psql') -X -v ON_ERROR_STOP=1 -h 127.0.0.1 -p $PrimaryPort -U g2_recovery_owner -d postgres *> (Child 'bootstrap.log')
        NativeOk 'Dedicated replication role and committed bootstrap marker'
    } finally { $env:PGPASSWORD=$previous }
    BackupFrom $PrimaryPort $metadata.standby.dataDirectory $true
    [IO.File]::AppendAllText((Join-Path $metadata.standby.dataDirectory 'postgresql.auto.conf'), @"

port = $StandbyPort
synchronous_standby_names = ''
primary_conninfo = 'host=127.0.0.1 port=$PrimaryPort user=g2_replicator application_name=g2_standby passfile=''$(PgPath $metadata.replicationPassfile)'' connect_timeout=5 sslmode=disable'
primary_slot_name = 'g2_standby'
"@)
    StartNode $metadata.standby
    $deadline = [DateTimeOffset]::UtcNow.AddSeconds(30)
    do {
        $streaming = Query $PrimaryPort "SELECT count(*) FROM pg_stat_replication WHERE application_name='g2_standby' AND state='streaming'"
        if ($streaming -eq '1') { break }
        Start-Sleep -Milliseconds 100
    } while ([DateTimeOffset]::UtcNow -lt $deadline)
    if ($streaming -ne '1') { throw 'The physical standby never entered streaming state.' }
    Query $PrimaryPort "ALTER SYSTEM SET synchronous_standby_names='FIRST 1 (g2_standby)'" | Out-Null
    Query $PrimaryPort 'SELECT pg_reload_conf()' | Out-Null
    Query $PrimaryPort "INSERT INTO g2_recovery_lab_probe VALUES ('synchronous-commit')" | Out-Null
    $barrier = Query $PrimaryPort 'SELECT pg_current_wal_lsn()::text'
    WaitReplay $barrier
    if ((Query $PrimaryPort "SELECT sync_state FROM pg_stat_replication WHERE application_name='g2_standby'") -ne 'sync') { throw 'Standby is not a synchronous member.' }
    $metadata.state='StreamingSynchronous'; $metadata.activeEndpoint='primary'; $metadata.oldPrimaryFenced=$false
    $metadata.systemIdentifier=Query $PrimaryPort 'SELECT system_identifier::text FROM pg_control_system()'
    $metadata.preparedAtUtc=[DateTimeOffset]::UtcNow.ToString('O')
    SaveJson $manifestFile $metadata
    SaveJson (Child 'replication-evidence.json') @{observedAtUtc=[DateTimeOffset]::UtcNow.ToString('O');postgresVersion=$version;physicalBasebackupVerified=$true;streaming=$true;synchronousCommit='remote_apply';syncState='sync';barrierLsn=$barrier;standbyReplayedBarrier=$true;primaryPort=$PrimaryPort;standbyPort=$StandbyPort}
    Write-Output 'Prepared a fresh physical synchronous primary/standby pair. Private connection references are in lab.json.'
    exit
}

if (!(Test-Path -LiteralPath $manifestFile)) { throw 'Prepare a fresh recovery laboratory first.' }
$metadata = Get-Content -LiteralPath $manifestFile -Raw | ConvertFrom-Json
if (![string]::Equals([IO.Path]::GetFullPath($metadata.binDirectory),$bin,$pathComparison)) { throw 'Use the exact binary directory recorded by this laboratory.' }
ValidateManifest
switch ($Action) {
    'Start' {
        if ($metadata.oldPrimaryFenced) { StartNode $metadata.standby }
        else { StartNode $metadata.primary; StartNode $metadata.standby }
        Write-Output 'Started only the admitted recorded node(s); a fenced old primary is never restarted.'
    }
    'Sync' {
        if ($metadata.oldPrimaryFenced) { throw 'The original replication pair is fenced; no old-primary barrier is valid.' }
        $barrier = if ($TargetLsn) { $TargetLsn } else { Query $metadata.primary.port 'SELECT pg_current_wal_lsn()::text' }
        WaitReplay $barrier
        SaveJson (Child 'latest-sync-evidence.json') @{observedAtUtc=[DateTimeOffset]::UtcNow.ToString('O');barrierLsn=$barrier;standbyReplayedBarrier=$true}
        Write-Output "Standby physically replayed WAL barrier $barrier."
    }
    'Backup' {
        $source = if ($metadata.oldPrimaryFenced) { $metadata.standby } else { $metadata.primary }
        if ((Query $source.port 'SELECT pg_is_in_recovery()') -ne 'f') { throw 'Backup source must be the current writable primary.' }
        New-Item -ItemType Directory -Path (Child 'backups') -Force | Out-Null
        $backupPath = Child ('backups/base-' + [Guid]::NewGuid().ToString('N'))
        BackupFrom $source.port $backupPath $false
        $backupManifest = Get-Content -LiteralPath (Join-Path $backupPath 'backup_manifest') -Raw | ConvertFrom-Json
        $record = @{formatVersion=1;createdAtUtc=[DateTimeOffset]::UtcNow.ToString('O');backupDirectory=$backupPath;sourceEndpoint=$source.name;sourcePort=$source.port;sourceConnectionStringFile=$source.connectionStringFile;verified=$true;manifestSha256=(Get-FileHash (Join-Path $backupPath 'backup_manifest') -Algorithm SHA256).Hash.ToLowerInvariant();walRanges=$backupManifest.'WAL-Ranges';systemIdentifier=$metadata.systemIdentifier}
        SaveJson $metadata.latestBackupFile $record
        SaveJson (Child ([IO.Path]::GetFileName($backupPath) + '-evidence.json')) $record
        Write-Output 'Created and verified a fresh physical base backup; latest-backup.json references it without credentials.'
    }
    'Failover' {
        if ($metadata.oldPrimaryFenced) { throw 'Old primary is already fenced. This laboratory does not perform failback or reset.' }
        AssertNodeIdentity $metadata.primary
        AssertNodeIdentity $metadata.standby
        $barrier = Query $metadata.primary.port 'SELECT pg_current_wal_lsn()::text'
        WaitReplay $barrier
        StopNode $metadata.primary $PrimaryStopMode
        if (PortListening $metadata.primary.port) { throw 'Old primary TCP port remains reachable; promotion is forbidden.' }
        SaveJson $metadata.fenceFile @{observedAtUtc=[DateTimeOffset]::UtcNow.ToString('O');oldPrimaryPort=$metadata.primary.port;oldPrimaryDataDirectory=$metadata.primary.dataDirectory;stopMode=$PrimaryStopMode;oldPrimaryStopped=$true;oldPrimaryPortClosed=$true;replayedBarrier=$barrier}
        $metadata.oldPrimaryFenced=$true; $metadata.state='OldPrimaryFenced'; SaveJson $manifestFile $metadata
        & (Tool 'pg_ctl') -D $metadata.standby.dataDirectory -w -t 40 promote *> (Child 'promotion.log')
        NativeOk 'Fenced physical standby promotion'
        if ((Query $metadata.standby.port 'SELECT pg_is_in_recovery()') -ne 'f') { throw 'Promoted node remains in recovery.' }
        Query $metadata.standby.port 'CHECKPOINT' | Out-Null
        $metadata.activeEndpoint='standby'; $metadata.state='Promoted'; SaveJson $manifestFile $metadata
        SaveJson (Child 'promotion-evidence.json') @{observedAtUtc=[DateTimeOffset]::UtcNow.ToString('O');oldPrimaryFenced=$true;oldPrimaryPortClosed=$true;newPrimaryPort=$metadata.standby.port;inRecovery=$false;timeline=[int](Query $metadata.standby.port 'SELECT timeline_id FROM pg_control_checkpoint()');replayedBarrier=$barrier}
        Write-Output 'Old primary is stopped/fenced; standby is now the sole admitted primary. Old primary restart is prohibited.'
    }
    'Restore' {
        if (Test-Path -LiteralPath $metadata.restore.dataDirectory) { throw 'Restore destination exists; no restore data is ever overwritten or deleted.' }
        $backup = Get-Content -LiteralPath $metadata.latestBackupFile -Raw | ConvertFrom-Json
        $allowedBackup = Child ('backups/' + [IO.Path]::GetFileName($backup.backupDirectory))
        AssertExactPath $backup.backupDirectory ('backups/' + [IO.Path]::GetFileName($backup.backupDirectory))
        if ($backup.systemIdentifier -ne $metadata.systemIdentifier) { throw 'Backup belongs to a different PostgreSQL system.' }
        if ((Get-FileHash (Join-Path $allowedBackup 'backup_manifest') -Algorithm SHA256).Hash.ToLowerInvariant() -ne $backup.manifestSha256) { throw 'Backup manifest identity changed.' }
        & (Tool 'pg_verifybackup') $allowedBackup *> (Child 'restore-verify.log'); NativeOk 'Pre-restore physical backup verification'
        Copy-Item -LiteralPath $allowedBackup -Destination $metadata.restore.dataDirectory -Recurse
        if (!$IsWindows) { & chmod 700 $metadata.restore.dataDirectory; NativeOk 'Restore data permissions' }
        [IO.File]::AppendAllText((Join-Path $metadata.restore.dataDirectory 'postgresql.auto.conf'), "`nport = $($metadata.restore.port)`nsynchronous_standby_names = ''`nsynchronous_commit = on`ndefault_transaction_read_only = on`n")
        SaveJson $metadata.restoreQuarantineFile @{formatVersion=1;state='Quarantined';observedAtUtc=[DateTimeOffset]::UtcNow.ToString('O');restoredPort=$metadata.restore.port;backupManifestSha256=$backup.manifestSha256;backupWalRanges=$backup.walRanges;automaticSenderActivationAllowed=$false;databaseDefaultReadOnly=$true}
        StartNode $metadata.restore
        if ((Query $metadata.restore.port 'SHOW default_transaction_read_only') -ne 'on') { throw 'Restored database is not quarantined read-only.' }
        SaveJson (Child 'restore-evidence.json') @{observedAtUtc=[DateTimeOffset]::UtcNow.ToString('O');physicalBackupVerified=$true;restorePort=$metadata.restore.port;standaloneRecoveryComplete=((Query $metadata.restore.port 'SELECT pg_is_in_recovery()') -eq 'f');quarantined=$true;automaticSenderActivationAllowed=$false;backupManifestSha256=$backup.manifestSha256}
        Write-Output 'Restored a verified physical backup into a fresh read-only quarantined node. Automatic sending is forbidden.'
    }
    'Status' {
        foreach ($endpoint in @($metadata.primary,$metadata.standby,$metadata.restore)) { Write-Output "$($endpoint.name) port=$($endpoint.port) running=$(Running $endpoint)" }
        Write-Output "state=$($metadata.state); oldPrimaryFenced=$($metadata.oldPrimaryFenced)"
    }
    'Stop' {
        StopNode $metadata.restore
        StopNode $metadata.standby
        StopNode $metadata.primary
        Write-Output 'Stopped only this laboratory; all data, backups and fencing evidence are retained.'
    }
}
