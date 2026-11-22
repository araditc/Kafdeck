#requires -Version 5.1
#requires -RunAsAdministrator
# Isolated Windows CI only. Never runs against an existing Kafdeck installation.
$ErrorActionPreference='Stop'
$repo=[IO.Path]::GetFullPath((Join-Path $PSScriptRoot '../..'))
$names=@('Kafdeck','Kafdeck-Keycloak','Kafdeck-PostgreSQL')
foreach($name in $names){if(Get-Service -Name $name -ErrorAction SilentlyContinue){throw 'This acceptance requires a clean isolated host.'}}
$id=[guid]::NewGuid().ToString('N')
$root=Join-Path $env:SystemDrive ('KafdeckSetupAcceptance-'+$id)
$task='KafdeckSetupAcceptance-'+$id
New-Item -ItemType Directory -Path $root,(Join-Path $root 'config'),(Join-Path $root 'secrets'),(Join-Path $root 'state/postgresql'),(Join-Path $root 'runtime/postgresql/bin'),(Join-Path $root 'scripts') -Force|Out-Null
Copy-Item -Path (Join-Path $repo 'deploy/windows/*.ps1') -Destination (Join-Path $root 'scripts')
$source=@'
using System;
using System.IO;
using System.ServiceProcess;
public class Fixture : ServiceBase {
  public Fixture() { string file=Path.GetFileNameWithoutExtension(System.Diagnostics.Process.GetCurrentProcess().MainModule.FileName); ServiceName=file=="pg_ctl"?"Kafdeck-PostgreSQL":file; }
  public static void Main() { Run(new Fixture()); }
}
'@
$cs=Join-Path $root 'Fixture.cs';[IO.File]::WriteAllText($cs,$source)
$fixture=Join-Path $root 'Fixture.exe'
& "$env:WINDIR/Microsoft.NET/Framework64/v4.0.30319/csc.exe" /nologo /target:exe "/out:$fixture" /reference:System.ServiceProcess.dll $cs
if($LASTEXITCODE){throw 'Fixture compilation failed.'}
$receiptFile=Join-Path $root 'config/offline-install.json'
@{root=$root;userSid='S-1-5-18';phase='complete';deploymentMode='windows-services'}|ConvertTo-Json|Set-Content -LiteralPath $receiptFile -Encoding UTF8
$retained=Join-Path $root 'secrets/retained.txt';[IO.File]::WriteAllText($retained,'private-acceptance-state')
$hash=(Get-FileHash -LiteralPath $retained).Hash
function Invoke-SystemRemoval {
    $wrapper=Join-Path $root 'Remove-Test.ps1'
    [IO.File]::WriteAllText($wrapper,"try { & '$root\scripts\Remove-OfflineServices.ps1' -InstallRoot '$root'; exit 0 } catch { [IO.File]::WriteAllText('$root\fixture-error.txt',`$_.Exception.Message); exit 1 }")
    $action=New-ScheduledTaskAction -Execute "$env:WINDIR/System32/WindowsPowerShell/v1.0/powershell.exe" -Argument ('-NoProfile -NonInteractive -ExecutionPolicy Bypass -File "'+$wrapper+'"')
    $principal=New-ScheduledTaskPrincipal -UserId SYSTEM -LogonType ServiceAccount -RunLevel Highest
    $null=Register-ScheduledTask -TaskName $task -Action $action -Principal $principal -Force
    Start-ScheduledTask -TaskName $task
    $deadline=[DateTime]::UtcNow.AddSeconds(120)
    do {Start-Sleep -Milliseconds 500;$info=Get-ScheduledTaskInfo -TaskName $task;$state=(Get-ScheduledTask -TaskName $task).State} while(($info.LastTaskResult -eq 267009 -or $state -eq 'Running' -or $info.LastRunTime.Year -lt 2020) -and [DateTime]::UtcNow -lt $deadline)
    if($state -eq 'Running'){throw 'Removal fixture timed out.'}
    return $info.LastTaskResult
}
try {
    foreach($name in $names){
        $exe=if($name -eq 'Kafdeck-PostgreSQL'){Join-Path $root 'runtime/postgresql/bin/pg_ctl.exe'}else{Join-Path $root "services/$name/$name.exe"}
        $null=New-Item -ItemType Directory -Path (Split-Path $exe) -Force;Copy-Item -LiteralPath $fixture -Destination $exe
        $binary='"'+$exe+'"'
        $parameters=@{Name=$name;BinaryPathName=$binary;StartupType='Manual'}
        if($name -eq 'Kafdeck-PostgreSQL'){
            $parameters.BinaryPathName=$binary+' runservice -N Kafdeck-PostgreSQL -D "'+(Join-Path $root 'state/postgresql')+'"'
            $account=([Security.Principal.SecurityIdentifier]::new('S-1-5-20')).Translate([Security.Principal.NTAccount]).Value
            $parameters.Credential=[PSCredential]::new($account,[securestring]::new())
        }
        $null=New-Service @parameters;Start-Service -Name $name
    }
    # An unadmitted database owner blocks removal of ALL services, preserving running state.
    $pg=Get-CimInstance Win32_Service -Filter "Name='Kafdeck-PostgreSQL'"
    Stop-Service 'Kafdeck-PostgreSQL'
    $null=Invoke-CimMethod -InputObject $pg -MethodName Change -Arguments @{StartName='LocalSystem';StartPassword=''}
    Start-Service 'Kafdeck-PostgreSQL'
    if((Invoke-SystemRemoval) -eq 0){throw 'Wrong service owner was accepted.'}
    foreach($name in $names){$controller=Get-Service $name;try {if($controller.Status -ne 'Running'){throw 'Admission failure stopped a service.'}}finally{$controller.Dispose()}}
    if((Get-Content -LiteralPath $receiptFile -Raw|ConvertFrom-Json).phase -ne 'complete'){throw 'Admission failure modified the receipt.'}
    Stop-Service 'Kafdeck-PostgreSQL'
    $null=Invoke-CimMethod -InputObject (Get-CimInstance Win32_Service -Filter "Name='Kafdeck-PostgreSQL'") -MethodName Change -Arguments @{StartName=$account;StartPassword=''}
    Start-Service 'Kafdeck-PostgreSQL'
    [GC]::Collect();[GC]::WaitForPendingFinalizers()
    if((Invoke-SystemRemoval) -ne 0){throw ('Owned service removal failed: '+[IO.File]::ReadAllText((Join-Path $root 'fixture-error.txt')))}
    foreach($name in $names){if(Get-Service -Name $name -ErrorAction SilentlyContinue){throw 'An owned service remains.'}}
    if((Get-FileHash -LiteralPath $retained).Hash -ne $hash -or (Get-Content -LiteralPath $receiptFile -Raw|ConvertFrom-Json).phase -ne 'services-removed'){throw 'Retained data/removal journal changed unexpectedly.'}
    if((Invoke-SystemRemoval) -ne 0){throw 'Idempotent removal resume failed.'}
    Write-Host 'PASS: all-service admission, real SCM removal, retained secrets and idempotent recovery.'
} finally {
    Unregister-ScheduledTask -TaskName $task -Confirm:$false -ErrorAction SilentlyContinue
    foreach($name in $names){$s=Get-CimInstance Win32_Service -Filter "Name='$name'";if($s -and $s.PathName.Contains($root)){Stop-Service $name -ErrorAction SilentlyContinue;& "$env:WINDIR/System32/sc.exe" delete $name|Out-Null}}
    # Preserve this small fixture for diagnosis; never recursively delete a computed system-drive path.
}
