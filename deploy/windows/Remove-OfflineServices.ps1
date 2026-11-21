#requires -Version 5.1
#requires -RunAsAdministrator
[CmdletBinding()]
param([Parameter(Mandatory=$true)][string]$InstallRoot)
$ErrorActionPreference='Stop'
. (Join-Path $PSScriptRoot 'Offline-Tools.ps1')
. (Join-Path $PSScriptRoot 'Offline-Upgrade.ps1')
. (Join-Path $PSScriptRoot 'Windows-Services.ps1')
$InstallRoot=[IO.Path]::GetFullPath($InstallRoot).TrimEnd('\')
Assert-UpgradeTree $InstallRoot
$lock=[IO.File]::Open((Join-Path $InstallRoot '.offline-upgrade.lock'),'OpenOrCreate','ReadWrite','None')
try {
    $receiptPath=Join-Path $InstallRoot 'config/offline-install.json'
    $receipt=Get-Content -LiteralPath $receiptPath -Raw|ConvertFrom-Json
    $sid=[Security.Principal.WindowsIdentity]::GetCurrent().User.Value
    if($receipt.root -ne $InstallRoot -or $receipt.userSid -ne $sid -or $receipt.phase -notin @('complete','services-removed') -or $receipt.deploymentMode -ne 'windows-services'){throw 'Installation ownership or state does not match.'}
    $journal=Join-Path $InstallRoot 'offline-upgrade.json'
    if(Test-Path -LiteralPath $journal){$pending=Get-Content -LiteralPath $journal -Raw|ConvertFrom-Json;if($pending.phase -notin @('complete','rolled-back')){throw 'Recover the interrupted upgrade before removing services.'}}
    $names=@('Kafdeck','Kafdeck-Keycloak','Kafdeck-PostgreSQL')
    # Admit every service before stopping any. No broad process kill, filesystem deletion or data removal.
    foreach($name in $names){
        $service=Get-CimInstance Win32_Service -Filter "Name='$name'"
        if(!$service){continue}
        if($name -ne 'Kafdeck-PostgreSQL'){Assert-OfflineServiceOwner $service (Join-Path $InstallRoot "services/$name/$name.exe") $sid}
        else {
            $match=[regex]::Match($service.PathName,'^\s*(?:"([^"]+)"|(\S+))')
            $exe=if($match.Groups[1].Success){$match.Groups[1].Value}else{$match.Groups[2].Value}
            $data=[regex]::Match($service.PathName,'(?:^|\s)-D\s+(?:"([^"]+)"|(\S+))')
            $directory=if($data.Groups[1].Success){$data.Groups[1].Value}else{$data.Groups[2].Value}
            $owner=([Security.Principal.NTAccount]::new($service.StartName)).Translate([Security.Principal.SecurityIdentifier]).Value
            if(!$directory -or [IO.Path]::GetFullPath($exe) -ne (Join-Path $InstallRoot 'runtime/postgresql/bin/pg_ctl.exe') -or [IO.Path]::GetFullPath($directory).TrimEnd('\') -ne (Join-Path $InstallRoot 'state/postgresql') -or $owner -ne 'S-1-5-20'){throw 'The PostgreSQL service belongs to another installation.'}
        }
    }
    # Journal before the first stop; rerunning removal can safely finish an interrupted removal.
    $receipt.phase='services-removed';Write-UpgradeJournal $receiptPath $receipt
    foreach($name in $names){
        $service=Get-Service -Name $name -ErrorAction SilentlyContinue
        if(!$service){continue}
        if($service.Status -ne 'Stopped'){Stop-Service -Name $name;$service.WaitForStatus('Stopped',[TimeSpan]::FromSeconds(120))}
        $service.Dispose()
        & "$env:SystemRoot/System32/sc.exe" delete $name|Out-Null
        if($LASTEXITCODE){throw 'Windows did not remove an owned service.'}
        $deadline=[DateTime]::UtcNow.AddSeconds(20)
        do {$remaining=Get-CimInstance Win32_Service -Filter "Name='$name'";if($remaining){Start-Sleep -Milliseconds 250}} while($remaining -and [DateTime]::UtcNow -lt $deadline)
        if($remaining){throw 'Service deletion is pending. Close service management consoles and resume removal.'}
    }
    # Configuration, identity/database data, DPAPI keys, runtimes, binaries, backups and recovery scripts are retained.
    Write-Host 'Services removed. All installation files and data were retained for recovery.'
} finally {$lock.Dispose()}
