# Windows PowerShell 5.1 transaction primitives. Service adapters are supplied by Upgrade-Offline.ps1.
$OfflineUpgradeFolders = @('app','scripts','docs','agent','config','secrets','state','runtime','services','public')

function Assert-OfflineUpgradeVendorStack([string]$Root) {
    # Keycloak 26.8.1 supports OpenJDK 17, 21 and 25, including vendor patch revisions.
    $release = Get-Content -LiteralPath (Join-Path $Root 'runtime/java/release') -Raw
    $versions = [regex]::Matches($release, '(?m)^JAVA_VERSION="([^"\r\n]+)"\r?$')
    if ($versions.Count -ne 1) { throw 'Cannot read the installed Java version from runtime/java/release.' }
    $javaVersion = $versions[0].Groups[1].Value
    if ($javaVersion -notmatch '^(17|21|25)(?:\.\d+)*(?:\+\d+(?:\.\d+)*)?(?:-LTS)?$') {
        throw "Unsupported installed Java version '$javaVersion'. Keycloak 26.8.1 requires a supported Java 17, 21 or 25 release."
    }
    $postgresVersion = (Get-Content -LiteralPath (Join-Path $Root 'state/postgresql/PG_VERSION') -Raw).Trim()
    if ($postgresVersion -ne '17') { throw "Unsupported installed PostgreSQL data version '$postgresVersion'. This upgrade requires PostgreSQL 17." }
    $keycloakLibraries = Join-Path $Root 'runtime/keycloak/lib/lib/main'
    if (!(Get-ChildItem -LiteralPath $keycloakLibraries -File -Filter '*keycloak-core-26.8.1.jar')) {
        throw 'Installed Keycloak core 26.8.1 was not found in runtime/keycloak/lib/lib/main. This upgrade requires Keycloak 26.8.1.'
    }
}

function Assert-UpgradeConfiguration([object]$Settings, [string]$Root) {
    foreach ($property in $Settings.PSObject.Properties) {
        if ($null -eq $property.Value) { continue }
        if ($property.Value.GetType().FullName -eq 'System.Management.Automation.PSCustomObject') {
            Assert-UpgradeConfiguration $property.Value $Root
        } elseif ($property.Value -is [array]) {
            foreach ($item in $property.Value) {
                if ($null -ne $item -and $item.GetType().FullName -eq 'System.Management.Automation.PSCustomObject') { Assert-UpgradeConfiguration $item $Root }
            }
        } elseif ($property.Value -is [string]) {
            $value = $property.Value
            if ($property.Name -match 'ConnectionString' -and $value) { throw 'External database configuration requires its own consistent backup plan.' }
            $isFileReference = $value.StartsWith('file:')
            if ($isFileReference) { $value = $value.Substring(5) }
            if (!$value -or $value.StartsWith('env:')) { continue }
            if ($isFileReference -or [IO.Path]::IsPathRooted($value) -or $property.Name -match '(Path|Directory|File)$') {
                if (![IO.Path]::IsPathRooted($value)) { $value = Join-Path (Join-Path $Root 'config') $value }
                $path = [IO.Path]::GetFullPath($value).TrimEnd('\')
                $covered = $false
                foreach ($folder in $OfflineUpgradeFolders) {
                    $directory = [IO.Path]::GetFullPath((Join-Path $Root $folder))
                    if ($path -eq $directory -or $path.StartsWith($directory+'\',[StringComparison]::OrdinalIgnoreCase)) { $covered = $true; break }
                }
                if (!$covered) { throw "Configured path is outside the cold snapshot: $($property.Name)." }
            }
        }
    }
}

function Write-UpgradeJournal([string]$Path, [object]$Value) {
    $bytes = [Text.Encoding]::UTF8.GetBytes(($Value | ConvertTo-Json -Depth 12))
    $temporary = $Path + '.new'
    $stream = [IO.File]::Open($temporary, 'Create', 'Write', 'None')
    try { $stream.Write($bytes, 0, $bytes.Length); $stream.Flush($true) } finally { $stream.Dispose() }
    if (Test-Path -LiteralPath $Path) { [IO.File]::Replace($temporary, $Path, [NullString]::Value) }
    else { [IO.File]::Move($temporary, $Path) }
}

function Assert-UpgradeTree([string]$Path) {
    $item = Get-Item -LiteralPath $Path -Force
    if ($item.Attributes -band [IO.FileAttributes]::ReparsePoint) { throw "Reparse points are unsupported: $Path" }
    foreach ($child in Get-ChildItem -LiteralPath $Path -Force) {
        if ($child.Attributes -band [IO.FileAttributes]::ReparsePoint) { throw "Reparse points are unsupported: $($child.FullName)" }
        if ($child.PSIsContainer) { Assert-UpgradeTree $child.FullName }
    }
}

function Copy-UpgradeTree([string]$Source, [string]$Destination) {
    # Preserve PostgreSQL NetworkService ACLs and private DPAPI/configuration ACLs.
    & robocopy.exe $Source $Destination /E /COPY:DATS /DCOPY:DAT /XJ /R:1 /W:1 /NFL /NDL /NJH /NJS /NP | Out-Null
    if ($LASTEXITCODE -ge 8) { throw "Backup/copy failed ($LASTEXITCODE): $Source" }
    $global:LASTEXITCODE = 0
}

function Get-UpgradeInventory([string]$Root) {
    $rows = foreach ($folder in $OfflineUpgradeFolders) {
        foreach ($file in Get-ChildItem -LiteralPath (Join-Path $Root $folder) -File -Recurse -Force) {
            [pscustomobject]@{
                path = $file.FullName.Substring($Root.TrimEnd('\').Length + 1)
                bytes = $file.Length
                sha256 = (Get-FileHash -LiteralPath $file.FullName -Algorithm SHA256).Hash
            }
        }
    }
    return @($rows)
}

function Assert-UpgradeInventory([string]$Root, [object[]]$Rows) {
    $actual = @(Get-UpgradeInventory $Root)
    if ($actual.Count -ne $Rows.Count) { throw 'Backup inventory count mismatch.' }
    $expected = @{}
    foreach ($row in $Rows) { $expected[$row.path] = $row }
    foreach ($row in $actual) {
        $match = $expected[$row.path]
        if (!$match -or $match.bytes -ne $row.bytes -or $match.sha256 -ne $row.sha256) { throw "Backup verification failed: $($row.path)" }
    }
}

function Restore-UpgradeSnapshot([string]$Root, [object]$Journal) {
    $backup = Assert-OfflineChild $Root $Journal.backup
    $snapshot = Join-Path $backup 'snapshot'
    $rows = Get-Content -LiteralPath (Join-Path $backup 'inventory.json') -Raw | ConvertFrom-Json
    Assert-UpgradeTree $snapshot
    Assert-UpgradeInventory $snapshot $rows
    # Copy first, then rename. Retain the verified snapshot for interrupted recovery.
    foreach ($folder in $OfflineUpgradeFolders) {
        $temporary = Join-Path $Root ('restore-' + $folder + '-' + [guid]::NewGuid().ToString('N'))
        Copy-UpgradeTree (Join-Path $snapshot $folder) $temporary
        $target = Assert-OfflineChild $Root $folder
        if (Test-Path -LiteralPath $target) {
            $failed = Assert-OfflineChild $backup ('failed-' + $folder + '-' + [guid]::NewGuid().ToString('N'))
            Move-Item -LiteralPath $target -Destination $failed
        }
        Move-Item -LiteralPath $temporary -Destination $target
    }
    Assert-UpgradeInventory $Root $rows
}

function Invoke-OfflineUpgradeTransaction {
    param([string]$Root, [string]$Media, [object]$Manifest, [scriptblock]$Stop, [scriptblock]$Start, [scriptblock]$Check)
    $journalPath = Join-Path $Root 'offline-upgrade.json'
    $journal = $null
    if (Test-Path -LiteralPath $journalPath) {
        $journal = Get-Content -LiteralPath $journalPath -Raw | ConvertFrom-Json
        if ($journal.root -ne $Root -or $journal.backup -notmatch '^backups[/\\]upgrade-[a-f0-9]{32}$') { throw 'Invalid upgrade journal.' }
        if ($journal.phase -notin @('complete','rolled-back')) {
            & $Stop
            if ($journal.backupReady) { Restore-UpgradeSnapshot $Root $journal }
            & $Start
            & $Check $false
            $journal.phase = 'rolled-back'
            Write-UpgradeJournal $journalPath $journal
            Write-Host 'Interrupted upgrade recovered. Run the same upgrade command again to apply the new package.'
            return
        }
    }
    $receiptPath = Join-Path $Root 'config/offline-install.json'
    $receipt = Get-Content -LiteralPath $receiptPath -Raw | ConvertFrom-Json
    $packageHash = (Get-FileHash -LiteralPath (Join-Path $Media 'bundle-manifest.json')).Hash
    if ($receipt.packageHash -eq $packageHash) {
        & $Check $true
        Write-Host "Version $($Manifest.version) is already installed."
        return
    }
    $relativeBackup = 'backups/upgrade-' + [guid]::NewGuid().ToString('N')
    $backup = Assert-OfflineChild $Root $relativeBackup
    $journal = [pscustomobject]@{root=$Root; backup=$relativeBackup; phase='preparing'; backupReady=$false; version=$Manifest.version; packageHash=$packageHash; receipt=$receipt}
    Write-UpgradeJournal $journalPath $journal
    try {
        & $Stop
        $snapshot = Join-Path $backup 'snapshot'
        foreach ($folder in $OfflineUpgradeFolders) { Copy-UpgradeTree (Join-Path $Root $folder) (Join-Path $snapshot $folder) }
        $rows = @(Get-UpgradeInventory $Root)
        Assert-UpgradeInventory $snapshot $rows
        Write-UpgradeJournal (Join-Path $backup 'inventory.json') $rows
        $journal.backupReady = $true
        $journal.phase = 'replacing'
        Write-UpgradeJournal $journalPath $journal
        foreach ($folder in @('app','scripts','docs','agent')) {
            $temporary = Join-Path $Root ('upgrade-' + $folder + '-' + [guid]::NewGuid().ToString('N'))
            Copy-UpgradeTree (Join-Path $Media ('payload/' + $folder)) $temporary
            $target = Assert-OfflineChild $Root $folder
            Move-Item -LiteralPath $target -Destination (Join-Path $backup ('previous-' + $folder))
            Move-Item -LiteralPath $temporary -Destination $target
        }
        $journal.phase = 'checking'
        Write-UpgradeJournal $journalPath $journal
        & $Start
        & $Check $true
        $receipt.packageHash = $packageHash
        $receipt | Add-Member -NotePropertyName version -NotePropertyValue $Manifest.version -Force
        $receipt | Add-Member -NotePropertyName upgradedAt -NotePropertyValue ([DateTime]::UtcNow.ToString('o')) -Force
        Write-UpgradeJournal $receiptPath $receipt
        $journal.phase = 'complete'
        Write-UpgradeJournal $journalPath $journal
        Write-Host "Upgrade complete: $($receipt.origin)/"
        Write-Host "Verified recovery backup: $backup"
    } catch {
        $failure = $_
        try {
            & $Stop
            if ($journal.backupReady) { Restore-UpgradeSnapshot $Root $journal }
            & $Start
            & $Check $false
            $journal.phase = 'rolled-back'
            Write-UpgradeJournal $journalPath $journal
        } catch { throw "Recovery is incomplete. Keep the installation and backup, then rerun this script. $($_.Exception.Message)" }
        throw "Upgrade failed and the original installation was recovered. $($failure.Exception.Message)"
    }
}
