#requires -Version 7.4
param([ValidatePattern('^pg-physical(?:-[a-z0-9]+)?$')][string]$LabName = 'pg-physical')
$ErrorActionPreference = 'Stop'
$repo = (Resolve-Path "$PSScriptRoot/../..").Path
$scriptFile = Join-Path $PSScriptRoot 'physical-signed-lab-control.ps1'
$template = Get-Content -LiteralPath "$repo/deploy/dev/prepare-postgres-g2-recovery.ps1" -Raw
$template = $template.Replace('[IO.File]::Move($temporary,$Path,$true)', @'
for ($attempt=0; ; $attempt++) {
        try { [IO.File]::Move($temporary,$Path,$true); break }
        catch [IO.IOException] {
            if ($attempt -ge 12) { throw }
            Start-Sleep -Milliseconds 250
        }
        catch [UnauthorizedAccessException] {
            if ($attempt -ge 12) { throw }
            Start-Sleep -Milliseconds 250
        }
    }
'@)
$template = $template.Replace("statement_timeout = '20s'", @'
statement_timeout = '20s'
archive_mode = on
archive_command = '$env:KAFDECK_W80_ARCHIVE_COMMAND'
'@)
Set-Content -LiteralPath $scriptFile -Value $template -Encoding utf8NoBOM
$lab = Join-Path $PSScriptRoot $LabName
if (Test-Path -LiteralPath $lab) { throw 'Physical lab already exists. Preserve the prior run.' }
$archive = Join-Path $PSScriptRoot "$LabName-archive"
if (Test-Path -LiteralPath $archive) { throw 'Physical WAL archive already exists. Preserve the prior run.' }
$null = New-Item -ItemType Directory -Path $archive
$python = (Get-Command python).Source.Replace('\','/')
$helper = (Resolve-Path "$PSScriptRoot/wal-transfer.py").Path.Replace('\','/')
$archiveUnix = $archive.Replace('\','/')
$env:KAFDECK_W80_ARCHIVE_COMMAND = ('"{0}" "{1}" archive "{2}" "%f" "%p"' -f $python,$helper,$archiveUnix)
$env:KAFDECK_W80_RESTORE_COMMAND = ('"{0}" "{1}" restore "{2}" "%f" "%p"' -f $python,$helper,$archiveUnix)
$env:KAFDECK_W80_RESTORE_COMMAND | Set-Content -LiteralPath (Join-Path $PSScriptRoot "$LabName-restore-command.txt") -Encoding utf8NoBOM
$pgBin = (Resolve-Path "$repo/deploy/dev/.generated/g2/postgres/pgsql/bin").Path
& pwsh -NoProfile -File $scriptFile -Action Prepare -OutputDirectory $lab -BinDirectory $pgBin -PrimaryPort 25533 -StandbyPort 25534 -RestorePort 25535
exit $LASTEXITCODE

