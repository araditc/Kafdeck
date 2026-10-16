param(
    [ValidateSet('Prepare', 'Start', 'Stop', 'Status')][string]$Action = 'Start',
    [ValidateSet('all', 'node-a', 'node-b')][string]$Node = 'all',
    [string]$OutputDirectory = "$PSScriptRoot/.generated/g2/keycloak-ha",
    [string]$IdentityDirectory = "$PSScriptRoot/.generated/g2/identity",
    [string]$KeycloakArchive = "$PSScriptRoot/.generated/g1/keycloak-segmented.zip",
    [string]$JavaHome = "$PSScriptRoot/.generated/g1/jdk-21.0.12.1+1"
)
$ErrorActionPreference = 'Stop'
$labPath = [IO.Path]::GetFullPath($OutputDirectory)
$identityPath = [IO.Path]::GetFullPath($IdentityDirectory)
$archivePath = [IO.Path]::GetFullPath($KeycloakArchive)
$javaPath = [IO.Path]::GetFullPath($JavaHome)
$metadataPath = Join-Path $labPath 'ha-lab.json'
$databaseMetadata = Join-Path $labPath 'database.json'
$archiveHash = '4cd312c889af358a819a74a6c6f068f9ea159a6959e49924607cec95cfd4c1a4'
$nodeDefinitions = @(
    @{ name = 'node-a'; httpsPort = 19443; clusterPort = 7800 },
    @{ name = 'node-b'; httpsPort = 19444; clusterPort = 7810 }
)

if ($Action -in @('Prepare', 'Start')) {
    if (-not $IsWindows) { throw 'This local launcher currently supports Windows PowerShell 7.' }
    if (-not (Test-Path -LiteralPath $databaseMetadata)) { throw 'Prepare the separate g2_keycloak PostgreSQL database metadata first.' }
    if ((Get-FileHash -LiteralPath $archivePath -Algorithm SHA256).Hash.ToLowerInvariant() -ne $archiveHash) {
        throw 'Keycloak archive differs from the verified pinned 26.8.1 ZIP.'
    }
    $identity = Get-Content -LiteralPath (Join-Path $identityPath 'lab.json') -Raw | ConvertFrom-Json
    if ([string]::IsNullOrWhiteSpace($identity.realmImportFile)) {
        throw 'Identity manifest must pin its own realmImportFile; a mutable shared runtime import is not accepted.'
    }
    $sourceRealm = [IO.Path]::GetFullPath($identity.realmImportFile)
    if (-not (Test-Path -LiteralPath $sourceRealm)) { throw 'The prepared G2 realm import is missing.' }
    foreach ($definition in $nodeDefinitions) {
        $nodeDirectory = Join-Path $labPath $definition.name
        $nodeHome = Join-Path $nodeDirectory 'keycloak-26.8.1'
        $marker = Join-Path $nodeDirectory 'verified-runtime.sha256'
        if (-not (Test-Path -LiteralPath $marker)) {
            New-Item -ItemType Directory -Path $nodeDirectory -Force | Out-Null
            [IO.Compression.ZipFile]::ExtractToDirectory($archivePath, $nodeDirectory)
            [IO.File]::WriteAllText($marker, $archiveHash)
        } elseif ([IO.File]::ReadAllText($marker).Trim() -ne $archiveHash) {
            throw 'Existing Keycloak node marker differs from the pinned archive.'
        }
        $importDirectory = Join-Path $nodeHome 'data/import'
        New-Item -ItemType Directory -Path $importDirectory -Force | Out-Null
        Copy-Item -LiteralPath $sourceRealm -Destination (Join-Path $importDirectory 'kafdeck-g2-realm.json')
    }
    $launchScript = Join-Path $labPath 'run-node.ps1'
    @'
param([string]$NodeHome, [string]$IdentityDirectory, [string]$DatabaseMetadata,
    [string]$JavaHome, [int]$HttpsPort, [int]$ClusterPort, [string]$NodeName)
$ErrorActionPreference = 'Stop'
$database = Get-Content -LiteralPath $DatabaseMetadata -Raw | ConvertFrom-Json
$env:JAVA_HOME = $JavaHome
$env:KC_DB = 'postgres'
$env:KC_DB_URL = $database.jdbcUrl
$env:KC_DB_USERNAME = $database.user
$env:KC_DB_PASSWORD = [IO.File]::ReadAllText($database.databasePasswordFile)
$env:KC_HTTPS_KEY_STORE_PASSWORD = [IO.File]::ReadAllText((Join-Path $IdentityDirectory 'https-password.txt'))
$env:JAVA_OPTS_APPEND = '-Xms128m -Xmx512m'
& (Join-Path $NodeHome 'bin/kc.bat') start --http-enabled=false --http-host=127.0.0.1 `
    "--https-port=$HttpsPort" --hostname=https://localhost:19443 `
    --db-pool-initial-size=2 --db-pool-min-size=2 --db-pool-max-size=8 `
    "--https-key-store-file=$(Join-Path $IdentityDirectory 'https.p12')" `
    --cache=ispn --cache-stack=jdbc-ping --cache-embedded-mtls-enabled=true `
    --cache-embedded-network-bind-address=127.0.0.1 "--cache-embedded-network-bind-port=$ClusterPort" `
    "--cache-embedded-node-name=$NodeName" --import-realm
exit $LASTEXITCODE
'@ | Set-Content -LiteralPath $launchScript -Encoding utf8
    if (-not (Test-Path -LiteralPath $metadataPath)) {
        @{
            keycloakVersion = '26.8.1'; issuer = 'https://localhost:19443/realms/kafdeck-g2'
            audience = $identity.audience; clientId = $identity.clientId; subject = $identity.subject
            caPemFile = $identity.caPemFile; clientPrivateKeyFile = $identity.clientPrivateKeyFile
            realm = 'kafdeck-g2'; databaseMetadataFile = $databaseMetadata; cache = 'ispn'; stack = 'jdbc-ping'; clusterMtls = $true
            nodes = @($nodeDefinitions | ForEach-Object { @{
                name = $_.name; httpsPort = $_.httpsPort; clusterPort = $_.clusterPort
                home = Join-Path (Join-Path $labPath $_.name) 'keycloak-26.8.1'
                endpoint = "https://localhost:$($_.httpsPort)/realms/kafdeck-g2"
            } })
        } | ConvertTo-Json -Depth 6 | Set-Content -LiteralPath $metadataPath -Encoding utf8
    }
    if ($Action -eq 'Prepare') { Write-Output 'Prepared two independent verified Keycloak node runtimes; no process started.'; exit }
}

if (-not (Test-Path -LiteralPath $metadataPath)) { throw 'Prepare the HA fixture first.' }
$metadata = Get-Content -LiteralPath $metadataPath -Raw | ConvertFrom-Json
foreach ($entry in $metadata.nodes) {
    if ($Node -ne 'all' -and $entry.name -ne $Node) { continue }
    $processFile = Join-Path $labPath ($entry.name + '-process.json')
    $existing = $null
    if (Test-Path -LiteralPath $processFile) {
        $saved = Get-Content -LiteralPath $processFile -Raw | ConvertFrom-Json
        $candidate = Get-Process -Id $saved.processId -ErrorAction SilentlyContinue
        # PowerShell 7.5+ materializes JSON ISO timestamps as DateTime. Compare
        # UTC ticks, not a string implicitly converted through local culture.
        $savedStartedUtc = if ($saved.startedAtUtc -is [DateTime]) { $saved.startedAtUtc.ToUniversalTime() }
            else { [DateTimeOffset]::Parse([string]$saved.startedAtUtc, [Globalization.CultureInfo]::InvariantCulture).UtcDateTime }
        if ($null -ne $candidate -and $candidate.StartTime.ToUniversalTime().Ticks -eq $savedStartedUtc.Ticks -and
            $candidate.Path -eq (Join-Path $PSHOME 'pwsh.exe')) {
            $existing = $candidate
        }
    }
    switch ($Action) {
        'Start' {
            if ($null -ne $existing) { Write-Output "$($entry.name) launcher is already running."; continue }
            $parameters = @('-NoProfile', '-NonInteractive', '-File', (Join-Path $labPath 'run-node.ps1'),
                '-NodeHome', $entry.home, '-IdentityDirectory', $identityPath, '-DatabaseMetadata', $databaseMetadata,
                '-JavaHome', $javaPath, '-HttpsPort', $entry.httpsPort, '-ClusterPort', $entry.clusterPort, '-NodeName', $entry.name)
            $quoted = $parameters | ForEach-Object { '"' + $_.ToString().Replace('"', '\"') + '"' }
            $process = Start-Process -FilePath (Join-Path $PSHOME 'pwsh.exe') -ArgumentList $quoted -WindowStyle Hidden `
                -RedirectStandardOutput (Join-Path $labPath ($entry.name + '.log')) `
                -RedirectStandardError (Join-Path $labPath ($entry.name + '-error.log')) -PassThru
            @{ processId = $process.Id; startedAtUtc = $process.StartTime.ToUniversalTime().ToString('O'); name = $entry.name } |
                ConvertTo-Json | Set-Content -LiteralPath $processFile -Encoding utf8
            Write-Output "Started $($entry.name) hidden launcher; HTTPS $($entry.httpsPort), cluster $($entry.clusterPort). Readiness and two-node membership still require verification."
        }
        'Stop' {
            if ($null -ne $existing) {
                & taskkill.exe /PID $existing.Id /T /F | Out-Null
                if ($LASTEXITCODE -ne 0) { throw 'Could not stop the exact recorded fixture process tree.' }
                Write-Output "Stopped $($entry.name) recorded process tree."
            } else { Write-Output "$($entry.name) is not running." }
        }
        'Status' { Write-Output "$($entry.name) launcher running: $($null -ne $existing)" }
    }
}
