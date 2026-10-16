param(
    [ValidateSet('Prepare', 'PrepareKeycloak', 'Start', 'Stop', 'Status')][string]$Action = 'Prepare',
    [string]$OutputDirectory = "$PSScriptRoot/.generated/g2/postgres",
    [string]$KeycloakHaDirectory = "$PSScriptRoot/.generated/g2/keycloak-ha",
    [ValidateRange(1024, 65535)][int]$Port = 15432
)
$ErrorActionPreference = 'Stop'
$labPath = [IO.Path]::GetFullPath($OutputDirectory)
$archiveName = 'postgresql-17.11-5-windows-x64-binaries.zip'
$archiveHash = '80379b2c04d51c30225532e0ae04509899141e9957ed096fe749d7fd9df8f82f'
$archivePath = Join-Path $labPath $archiveName
$binPath = Join-Path $labPath 'pgsql/bin'
$dataPath = Join-Path $labPath 'data'
$passwordPath = Join-Path $labPath 'password.txt'
$metadataPath = Join-Path $labPath 'lab.json'

function Assert-NativeSuccess([string]$Operation) {
    if ($LASTEXITCODE -ne 0) { throw "$Operation failed (exit $LASTEXITCODE); inspect the ignored fixture log." }
}

if ($Action -eq 'Prepare') {
    if (-not $IsWindows) { throw 'This fixture pins the Windows x64 EDB binary ZIP.' }
    New-Item -ItemType Directory -Path $labPath -Force | Out-Null
    if (Test-Path -LiteralPath $metadataPath) {
        $previous = Get-Content -LiteralPath $metadataPath -Raw | ConvertFrom-Json
        if ($previous.port -ne $Port -or $previous.dataDirectory -ne $dataPath) {
            throw 'Existing fixture location/port differs. Use a separate output directory for another cluster.'
        }
    }
    if (-not (Test-Path -LiteralPath $archivePath)) {
        & curl.exe --fail --location --retry 2 --connect-timeout 15 --max-time 600 `
            "https://get.enterprisedb.com/postgresql/$archiveName" --output $archivePath
        Assert-NativeSuccess 'Pinned PostgreSQL download'
    }
    if ((Get-FileHash -LiteralPath $archivePath -Algorithm SHA256).Hash.ToLowerInvariant() -ne $archiveHash) {
        throw 'PostgreSQL archive SHA256 does not match the publisher-provided checksum; no extraction or execution allowed.'
    }
    $runtimeMarker = Join-Path $labPath 'verified-runtime.sha256'
    if (-not (Test-Path -LiteralPath $runtimeMarker)) {
        # Extract only the server runtime; the ZIP also carries unrelated GUI tools.
        $rootPrefix = $labPath.TrimEnd([IO.Path]::DirectorySeparatorChar) + [IO.Path]::DirectorySeparatorChar
        $archive = [IO.Compression.ZipFile]::OpenRead($archivePath)
        try {
            foreach ($entry in $archive.Entries) {
                if ($entry.FullName -notmatch '^pgsql/(bin|lib|share)/') { continue }
                $targetPath = [IO.Path]::GetFullPath((Join-Path $labPath $entry.FullName))
                if (-not $targetPath.StartsWith($rootPrefix, [StringComparison]::OrdinalIgnoreCase)) {
                    throw 'Archive entry escaped the laboratory extraction directory.'
                }
                if ($entry.FullName.EndsWith('/')) {
                    [IO.Directory]::CreateDirectory($targetPath) | Out-Null
                } else {
                    [IO.Directory]::CreateDirectory([IO.Path]::GetDirectoryName($targetPath)) | Out-Null
                    [IO.Compression.ZipFileExtensions]::ExtractToFile($entry, $targetPath, $true)
                }
            }
        } finally { $archive.Dispose() }
        [IO.File]::WriteAllText($runtimeMarker, $archiveHash)
    } elseif ([IO.File]::ReadAllText($runtimeMarker).Trim() -ne $archiveHash) {
        throw 'Existing runtime marker differs from the pinned archive; inspect before replacing runtime files.'
    }
    if (-not (Test-Path -LiteralPath (Join-Path $dataPath 'global/pg_control'))) {
        if (Test-Path -LiteralPath $dataPath) {
            throw 'Partial PostgreSQL data directory exists; inspect it before retrying. This script never deletes data.'
        }
        $labPassword = [Convert]::ToHexString([Security.Cryptography.RandomNumberGenerator]::GetBytes(32))
        [IO.File]::WriteAllText($passwordPath, $labPassword, [Text.UTF8Encoding]::new($false))
        & (Join-Path $binPath 'initdb.exe') -D $dataPath -U g2_owner --pwfile=$passwordPath `
            --encoding=UTF8 --locale=C --auth-local=scram-sha-256 --auth-host=scram-sha-256 `
            *> (Join-Path $labPath 'initdb.log')
        Assert-NativeSuccess 'Disposable SCRAM database initialization'
        [IO.File]::AppendAllText((Join-Path $dataPath 'postgresql.conf'), @"

# Kafdeck isolated G2 laboratory: no service or public listener.
listen_addresses = '127.0.0.1'
port = $Port
password_encryption = 'scram-sha-256'
max_connections = 60
shared_buffers = '64MB'
fsync = on
synchronous_commit = on
"@)
    }
    if (-not (Test-Path -LiteralPath $passwordPath)) { throw 'Existing fixture password file is missing.' }
    $connection = "Host=127.0.0.1;Port=$Port;Username=g2_owner;Password=$([IO.File]::ReadAllText($passwordPath));Database=postgres;Pooling=false;Timeout=10;Command Timeout=30"
    [IO.File]::WriteAllText((Join-Path $labPath 'connection-string.txt'), $connection)
    @{
        postgresVersion = '17.11'; distribution = $archiveName; archiveSha256 = $archiveHash
        checksumSource = 'https://github.com/EnterpriseDB/edb-installers/issues/731'
        bindAddress = '127.0.0.1'; port = $Port; user = 'g2_owner'; database = 'postgres'
        dataDirectory = $dataPath; binDirectory = $binPath
        connectionStringFile = Join-Path $labPath 'connection-string.txt'
    } | ConvertTo-Json | Set-Content -LiteralPath $metadataPath -Encoding utf8
    Write-Output "Prepared verified portable PostgreSQL 17.11 under $labPath. Credentials stay in ignored files."
    exit
}

if (-not (Test-Path -LiteralPath $metadataPath)) { throw 'Run Prepare before controlling this laboratory.' }
$metadata = Get-Content -LiteralPath $metadataPath -Raw | ConvertFrom-Json
if ($metadata.port -ne $Port) { throw 'Requested port differs from the prepared fixture metadata.' }
$control = Join-Path $binPath 'pg_ctl.exe'
switch ($Action) {
    'PrepareKeycloak' {
        $haPath = [IO.Path]::GetFullPath($KeycloakHaDirectory)
        New-Item -ItemType Directory -Path $haPath -Force | Out-Null
        $haPasswordPath = Join-Path $haPath 'database-password.txt'
        $psql = Join-Path $binPath 'psql.exe'
        $previousPassword = $env:PGPASSWORD
        try {
            $env:PGPASSWORD = [IO.File]::ReadAllText($passwordPath)
            $exists = & $psql -X -A -t -h 127.0.0.1 -p $Port -U g2_owner -d postgres -c "SELECT 1 FROM pg_roles WHERE rolname='g2_keycloak';"
            Assert-NativeSuccess 'Keycloak fixture role lookup'
            if ($exists -eq '1') {
                if (-not (Test-Path -LiteralPath $haPasswordPath)) { throw 'Existing Keycloak role password file is missing; no password is silently replaced.' }
            } else {
                $haPassword = [Convert]::ToHexString([Security.Cryptography.RandomNumberGenerator]::GetBytes(32))
                [IO.File]::WriteAllText($haPasswordPath, $haPassword)
                # Pass secret material over stdin, never a process argument.
                "CREATE ROLE g2_keycloak LOGIN PASSWORD '$haPassword';" |
                    & $psql -X -v ON_ERROR_STOP=1 -h 127.0.0.1 -p $Port -U g2_owner -d postgres
                Assert-NativeSuccess 'Separate Keycloak fixture role creation'
            }
            $exists = & $psql -X -A -t -h 127.0.0.1 -p $Port -U g2_owner -d postgres -c "SELECT 1 FROM pg_database WHERE datname='g2_keycloak';"
            Assert-NativeSuccess 'Keycloak fixture database lookup'
            if ($exists -ne '1') {
                & $psql -X -v ON_ERROR_STOP=1 -h 127.0.0.1 -p $Port -U g2_owner -d postgres -c 'CREATE DATABASE g2_keycloak OWNER g2_keycloak;'
                Assert-NativeSuccess 'Separate Keycloak fixture database creation'
            }
        } finally {
            if ($null -eq $previousPassword) { Remove-Item Env:PGPASSWORD -ErrorAction SilentlyContinue }
            else { $env:PGPASSWORD = $previousPassword }
        }
        @{
            database = 'g2_keycloak'; user = 'g2_keycloak'; jdbcUrl = "jdbc:postgresql://127.0.0.1:$Port/g2_keycloak"
            databasePasswordFile = $haPasswordPath; postgresVersion = '17.11'; scope = 'isolated-Keycloak-HA-lab-only'
        } | ConvertTo-Json | Set-Content -LiteralPath (Join-Path $haPath 'database.json') -Encoding utf8
        Write-Output 'Separate Keycloak database and non-superuser role are ready; credentials remain in ignored files.'
    }
    'Start' {
        & $control -D $dataPath status *> $null
        if ($LASTEXITCODE -eq 0) { Write-Output 'PostgreSQL fixture is already running.'; exit }
        & $control -D $dataPath -l (Join-Path $labPath 'postgres.log') -w -t 45 start
        Assert-NativeSuccess 'Portable PostgreSQL startup'
    }
    'Stop' {
        & $control -D $dataPath -w -t 45 -m fast stop
        Assert-NativeSuccess 'Portable PostgreSQL shutdown'
    }
    'Status' { & $control -D $dataPath status; Assert-NativeSuccess 'Portable PostgreSQL status' }
}
