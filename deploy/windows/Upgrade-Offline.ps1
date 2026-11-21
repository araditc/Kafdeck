#requires -Version 5.1
#requires -RunAsAdministrator
[CmdletBinding()]
param([string]$InstallRoot='C:\Kafdeck', [switch]$VerifyOnly)
$ErrorActionPreference = 'Stop'
[Net.ServicePointManager]::SecurityProtocol = [Net.SecurityProtocolType]::Tls12
$mediaRoot = $PSScriptRoot
. (Join-Path $mediaRoot 'payload/scripts/Offline-Tools.ps1')
. (Join-Path $mediaRoot 'payload/scripts/Offline-Upgrade.ps1')
. (Join-Path $mediaRoot 'payload/scripts/Windows-Services.ps1')
$manifest = Test-OfflineMedia $mediaRoot
if ($manifest.version -notmatch '^\d+\.\d+\.\d+$' -or $manifest.upgradeLayout -ne 'windows-services-v1') { throw 'Package lacks the supported upgrade contract.' }
$InstallRoot = [IO.Path]::GetFullPath($InstallRoot).TrimEnd('\')
if ($InstallRoot -eq [IO.Path]::GetPathRoot($InstallRoot).TrimEnd('\') -or $InstallRoot.StartsWith($env:SystemRoot,[StringComparison]::OrdinalIgnoreCase)) { throw 'Invalid installation directory.' }
if ($mediaRoot.StartsWith($InstallRoot+'\',[StringComparison]::OrdinalIgnoreCase) -or $InstallRoot.StartsWith($mediaRoot+'\',[StringComparison]::OrdinalIgnoreCase) -or $InstallRoot -eq $mediaRoot) { throw 'Extract the package outside the installed application directory.' }
Assert-UpgradeTree $InstallRoot
$lock = [IO.File]::Open((Join-Path $InstallRoot '.offline-upgrade.lock'), 'OpenOrCreate', 'ReadWrite', 'None')
try {
    $journalPath = Join-Path $InstallRoot 'offline-upgrade.json'
    $pending = $null
    if (Test-Path -LiteralPath $journalPath) {
        $pending = Get-Content -LiteralPath $journalPath -Raw | ConvertFrom-Json
        if ($pending.root -ne $InstallRoot -or $pending.backup -notmatch '^backups[/\\]upgrade-[a-f0-9]{32}$') { throw 'Invalid recovery journal.' }
    }
    $recovering = $pending -and $pending.phase -notin @('complete','rolled-back')
    $receipt = if ($recovering) { $pending.receipt } else { Get-Content -LiteralPath (Join-Path $InstallRoot 'config/offline-install.json') -Raw | ConvertFrom-Json }
    $identity = [Security.Principal.WindowsIdentity]::GetCurrent()
    if ($receipt.root -ne $InstallRoot -or $receipt.userSid -ne $identity.User.Value -or $receipt.phase -ne 'complete' -or $receipt.deploymentMode -ne 'windows-services') { throw 'Use the original installation account and a completed Windows Services installation.' }
    $services = @('Kafdeck','Kafdeck-Keycloak','Kafdeck-PostgreSQL')
    foreach ($name in $services) {
        $service = Get-CimInstance Win32_Service -Filter "Name='$name'"
        if (!$service) { throw "Missing service $name." }
        if ($name -ne 'Kafdeck-PostgreSQL') {
            Assert-OfflineServiceOwner $service (Join-Path $InstallRoot "services/$name/$name.exe") $identity.User.Value
        } else {
            $pg = Join-Path $InstallRoot 'runtime/postgresql/bin/pg_ctl.exe'
            $data = Join-Path $InstallRoot 'state/postgresql'
            $command = $service.PathName
            $executable = if ($command.StartsWith('"')) { $command.Split('"')[1] } else { $command.Split(' ')[0] }
            $match = [regex]::Match($command, '(?:^|\s)-D\s+(?:"([^"]+)"|(\S+))')
            $databasePath = if ($match.Groups[1].Success) { $match.Groups[1].Value } else { $match.Groups[2].Value }
            $databaseOwner = ([Security.Principal.NTAccount]::new($service.StartName)).Translate([Security.Principal.SecurityIdentifier]).Value
            if ($executable -ne $pg -or $databasePath -ne $data -or $databaseOwner -ne 'S-1-5-20') { throw 'PostgreSQL service paths do not match this installation.' }
        }
    }
    if (!$recovering) {
    foreach ($folder in $OfflineUpgradeFolders) {
        if (!(Test-Path -LiteralPath (Join-Path $InstallRoot $folder) -PathType Container)) { throw "Missing installation folder $folder." }
    }
    Assert-OfflineUpgradeVendorStack $InstallRoot
    $settings = Get-Content -LiteralPath (Join-Path $InstallRoot 'config/appsettings.Production.json') -Raw | ConvertFrom-Json
    Assert-UpgradeConfiguration $settings $InstallRoot
    # Windows Services inherit machine configuration and the installation identity.
    foreach ($scope in @('Machine','User')) {
        $overrides = [Environment]::GetEnvironmentVariables($scope)
        foreach ($name in $overrides.Keys) {
            if ($name -like 'Kafdeck__*' -or $name -like 'ConnectionStrings__*') {
                Assert-UpgradeConfiguration ([pscustomobject]@{([string]$name)=[string]$overrides[$name]}) $InstallRoot
            }
        }
    }
    $localIdentity = Get-Content -LiteralPath (Join-Path $InstallRoot 'config/local-keycloak.json') -Raw | ConvertFrom-Json
    if ($localIdentity.home -ne (Join-Path $InstallRoot 'runtime/keycloak') -or $localIdentity.javaHome -ne (Join-Path $InstallRoot 'runtime/java')) { throw 'Identity runtime directories differ from the bundled installation.' }
    $identityConfig = Get-Content -LiteralPath (Join-Path $InstallRoot 'config/keycloak.conf')
    $databaseUrl = @($identityConfig | Where-Object { $_ -match '^db-url=' })
    if ($databaseUrl.Count -ne 1 -or $databaseUrl[0] -ne "db-url=jdbc:postgresql://127.0.0.1:$($receipt.databasePort)/kafdeck_keycloak") { throw 'Keycloak must use the bundled local PostgreSQL database for this upgrade path.' }
    if ($receipt.version -and [version]$manifest.version -lt [version]$receipt.version) { throw 'Package version is older than the installed version.' }
    }
    $size = (Get-ChildItem -LiteralPath $InstallRoot -Recurse -File | Where-Object { $_.FullName -notlike "$InstallRoot\backups\*" } | Measure-Object Length -Sum).Sum
    if ([IO.DriveInfo]::new([IO.Path]::GetPathRoot($InstallRoot)).AvailableFreeSpace -lt (3*$size + 1GB)) { throw 'Insufficient space for a verified cold backup, staged application and recovery.' }
    if ($VerifyOnly) { Write-Host "Package and installation are eligible for upgrade to $($manifest.version)."; return }
    $expectedSetup = $receipt.setupState
    if (!$recovering) {
        $expectedSetup = (Invoke-RestMethod -Uri "$($receipt.origin)/api/v1/setup" -MaximumRedirection 0 -TimeoutSec 10).state
        if ($expectedSetup -notin @('complete','available')) { throw 'Existing setup state requires recovery before upgrade.' }
        $receipt | Add-Member -NotePropertyName setupState -NotePropertyValue $expectedSetup -Force
        # Persist the baseline before the cold snapshot so interrupted recovery can verify it.
        Write-UpgradeJournal (Join-Path $InstallRoot 'config/offline-install.json') $receipt
    }
    $stop = {
        foreach ($name in $services) { Set-Service -Name $name -StartupType Disabled }
        foreach ($name in $services) {
            $service = Get-Service -Name $name
            if ($service.Status -ne 'Stopped') { Stop-Service -Name $name; $service.WaitForStatus('Stopped',[TimeSpan]::FromSeconds(120)) }
        }
    }
    $start = {
        foreach ($name in @('Kafdeck-PostgreSQL','Kafdeck-Keycloak','Kafdeck')) {
            Set-Service -Name $name -StartupType Automatic
            if ($name -ne 'Kafdeck-PostgreSQL') { Set-ItemProperty -LiteralPath "HKLM:\SYSTEM\CurrentControlSet\Services\$name" -Name DelayedAutoStart -Value 1 }
            Start-Service -Name $name
        }
    }
    $check = {
        param($NewVersion)
        $null = Wait-OfflineUrl "$($receipt.identityOrigin)/realms/kafdeck/.well-known/openid-configuration" 600
        $null = Wait-OfflineUrl "$($receipt.origin)/healthz" 180
        $setup = Invoke-RestMethod -Uri "$($receipt.origin)/api/v1/setup" -MaximumRedirection 0 -TimeoutSec 10
        if ($setup.state -ne $expectedSetup) { throw 'First-owner state requires recovery.' }
        if ($NewVersion) {
            $expected = @($manifest.files | Where-Object { $_.path.StartsWith('payload/app/') })
            foreach ($file in $expected) {
                $path = Assert-OfflineChild $InstallRoot $file.path.Substring(8)
                if ((Get-FileHash -LiteralPath $path).Hash -ne $file.sha256) { throw 'Installed application differs from verified media.' }
            }
        }
        & (Join-Path $InstallRoot 'scripts/Test-InstalledServices.ps1')
    }
    Invoke-OfflineUpgradeTransaction -Root $InstallRoot -Media $mediaRoot -Manifest $manifest -Stop $stop -Start $start -Check $check
} finally { $lock.Dispose() }
