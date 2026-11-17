#requires -Version 5.1
param([switch]$DataOnlyFixture)
$ErrorActionPreference = 'Stop'
$repo = [IO.Path]::GetFullPath((Join-Path $PSScriptRoot '../..'))
. (Join-Path $repo 'deploy/windows/Offline-Tools.ps1')
. (Join-Path $repo 'deploy/windows/Offline-Upgrade.ps1')
if ($DataOnlyFixture) {
    function Copy-UpgradeTree([string]$Source, [string]$Destination) {
        New-Item -ItemType Directory -Path $Destination -Force | Out-Null
        Get-ChildItem -LiteralPath $Source -Force | Copy-Item -Destination $Destination -Recurse -Force
    }
}
$fixture = Join-Path $repo ('artifacts/offline-upgrade-tests-' + [guid]::NewGuid().ToString('N'))
New-Item -ItemType Directory -Path $fixture | Out-Null
function Assert($Condition, $Message) { if (!$Condition) { throw $Message } }
$passed = @()
function Scenario([string]$Name, [scriptblock]$Body) {
    $root = Join-Path $fixture $Name
    $media = Join-Path $fixture ($Name + '-media')
    foreach ($folder in $OfflineUpgradeFolders) {
        New-Item -ItemType Directory -Path (Join-Path $root $folder) -Force | Out-Null
        [IO.File]::WriteAllText((Join-Path $root "$folder/old.txt"), "original-$folder")
    }
    foreach ($folder in @('app','scripts','docs','agent')) {
        New-Item -ItemType Directory -Path (Join-Path $media "payload/$folder") -Force | Out-Null
        [IO.File]::WriteAllText((Join-Path $media "payload/$folder/new.txt"), "new-$folder")
    }
    $receipt = @{root=$root; packageHash='old'; origin='https://127.0.0.1:8080'; phase='complete'}
    Write-UpgradeJournal (Join-Path $root 'config/offline-install.json') $receipt
    [IO.File]::WriteAllText((Join-Path $media 'bundle-manifest.json'), '{"version":"1.0.0"}')
    $manifest = [pscustomobject]@{version='1.0.0'}
    $stop = { }
    $start = { }
    $check = { param($NewVersion) }
    & $Body
    $script:passed += $Name
}
Scenario 'success-and-idempotence' {
    Invoke-OfflineUpgradeTransaction $root $media $manifest $stop $start $check
    Assert (Test-Path (Join-Path $root 'app/new.txt')) 'New application missing'
    Assert (!(Test-Path (Join-Path $root 'app/old.txt'))) 'Stale application file survived'
    Assert ((Get-Content (Join-Path $root 'secrets/old.txt')) -eq 'original-secrets') 'Secrets changed'
    $before = @(Get-ChildItem (Join-Path $root 'backups')).Count
    Invoke-OfflineUpgradeTransaction $root $media $manifest $stop $start $check
    Assert (@(Get-ChildItem (Join-Path $root 'backups')).Count -eq $before) 'Retry created another backup'
}
Scenario 'failed-health-restores-data-and-binaries' {
    $baseline = @(Get-UpgradeInventory $root)
    $check = { param($NewVersion)
        if ($NewVersion) {
            [IO.File]::WriteAllText((Join-Path $root 'state/old.txt'), 'migration-changed-data')
            throw 'Injected unhealthy new application'
        }
    }
    try { Invoke-OfflineUpgradeTransaction $root $media $manifest $stop $start $check; throw 'Expected failure' }
    catch { Assert ($_.Exception.Message -like 'Upgrade failed and the original*') $_.Exception.Message }
    Assert-UpgradeInventory $root $baseline
    Assert ((Get-Content (Join-Path $root 'offline-upgrade.json') -Raw | ConvertFrom-Json).phase -eq 'rolled-back') 'Recovery not committed'
}
Scenario 'interrupted-replacement-recovery' {
    $backup = 'backups/upgrade-' + [guid]::NewGuid().ToString('N')
    $snapshot = Join-Path $root "$backup/snapshot"
    foreach ($folder in $OfflineUpgradeFolders) { Copy-UpgradeTree (Join-Path $root $folder) (Join-Path $snapshot $folder) }
    $baseline = @(Get-UpgradeInventory $root)
    Write-UpgradeJournal (Join-Path $root "$backup/inventory.json") $baseline
    Write-UpgradeJournal (Join-Path $root 'offline-upgrade.json') @{root=$root;backup=$backup;phase='replacing';backupReady=$true;receipt=$receipt}
    Move-Item -LiteralPath (Join-Path $root 'config') -Destination (Join-Path $root "$backup/interrupted-config")
    Invoke-OfflineUpgradeTransaction $root $media $manifest $stop $start $check
    Assert-UpgradeInventory $root $baseline
}
Scenario 'incomplete-backup-restarts-original' {
    $backup = 'backups/upgrade-' + [guid]::NewGuid().ToString('N')
    Write-UpgradeJournal (Join-Path $root 'offline-upgrade.json') @{root=$root;backup=$backup;phase='preparing';backupReady=$false;receipt=$receipt}
    $baseline = @(Get-UpgradeInventory $root)
    Invoke-OfflineUpgradeTransaction $root $media $manifest $stop $start $check
    Assert-UpgradeInventory $root $baseline
}
Scenario 'escaped-journal-rejected' {
    Write-UpgradeJournal (Join-Path $root 'offline-upgrade.json') @{root=$root;backup='../outside';phase='checking';backupReady=$true}
    try { Invoke-OfflineUpgradeTransaction $root $media $manifest $stop $start $check; throw 'Expected rejection' }
    catch { Assert ($_.Exception.Message -eq 'Invalid upgrade journal.') $_.Exception.Message }
}
Scenario 'exclusive-lock' {
    $path = Join-Path $root '.offline-upgrade.lock'
    $handle = [IO.File]::Open($path,'OpenOrCreate','ReadWrite','None')
    try {
        try { $other=[IO.File]::Open($path,'OpenOrCreate','ReadWrite','None'); $other.Dispose(); throw 'Lock failed' }
        catch { Assert ($_.Exception.Message -ne 'Lock failed') 'Concurrent upgrade was admitted' }
    } finally { $handle.Dispose() }
}
Scenario 'configured-path-snapshot-coverage' {
    Assert-UpgradeConfiguration ([pscustomobject]@{DatabasePath=(Join-Path $root 'state/history.sqlite')}) $root
    Assert-UpgradeConfiguration ([pscustomobject]@{StateFile='../state/identity.json'}) $root
    foreach ($path in @((Join-Path $root 'data/history.sqlite'), (Join-Path $root 'state/../../external.db'), '../../external.db')) {
        try { Assert-UpgradeConfiguration ([pscustomobject]@{DatabasePath=$path}) $root; throw "Expected rejection for $path" }
        catch { Assert ($_.Exception.Message -like 'Configured path is outside the cold snapshot:*') $_.Exception.Message }
    }
    try { Assert-UpgradeConfiguration ([pscustomobject]@{Profiles=@([pscustomobject]@{Path=(Join-Path $root 'custom/private.key')})}) $root; throw 'Expected rejection' }
    catch { Assert ($_.Exception.Message -like 'Configured path is outside the cold snapshot:*') $_.Exception.Message }
}
$errors = @()
foreach ($file in Get-ChildItem (Join-Path $repo 'deploy/windows') -Filter '*.ps1') {
    $tokens = $null; $parseErrors = $null
    $null = [Management.Automation.Language.Parser]::ParseFile($file.FullName,[ref]$tokens,[ref]$parseErrors)
    $errors += $parseErrors
}
Assert (!$errors.Count) ($errors | Out-String)
@{passed=$passed;count=$passed.Count;fixture=$fixture;productTestsRepeated=$false;aclCopyExercised=(!$DataOnlyFixture)} | ConvertTo-Json -Depth 4 | Set-Content (Join-Path $fixture 'evidence.json')
Write-Host "PASS $($passed.Count) upgrade scenarios. Evidence: $fixture/evidence.json"
