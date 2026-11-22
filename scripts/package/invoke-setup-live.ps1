#requires -Version 5.1
#requires -RunAsAdministrator
# Runs only on a disposable GitHub-hosted Windows VM, never a developer/operator host.
$ErrorActionPreference='Stop'
if($env:GITHUB_ACTIONS -ne 'true' -or $env:RUNNER_ENVIRONMENT -ne 'github-hosted'){throw 'Isolated GitHub-hosted runner required.'}
foreach($name in @('Kafdeck','Kafdeck-Keycloak','Kafdeck-PostgreSQL')){if(Get-Service -Name $name -ErrorAction SilentlyContinue){throw 'A preexisting installation was preserved; acceptance requires a clean host.'}}
$repo=[IO.Path]::GetFullPath((Join-Path $PSScriptRoot '../..'))
$id=[guid]::NewGuid().ToString('N')
$lab=Join-Path $env:SystemDrive ('KafdeckInstallerLab-'+$id)
$user='KfSetup'+$id.Substring(0,10)
$pipeName='KafdeckSetup-'+$id
$task='KafdeckSetup-'+$id
$bytes=New-Object byte[] 24;$rng=[Security.Cryptography.RandomNumberGenerator]::Create()
try {$rng.GetBytes($bytes)} finally {$rng.Dispose()}
$password='Kf!'+[BitConverter]::ToString($bytes).Replace('-','')+'a9'
$secure=ConvertTo-SecureString $password -AsPlainText -Force
$created=$false;$server=$null
try {
    $account=New-LocalUser -Name $user -Password $secure -PasswordNeverExpires -AccountNeverExpires
    $created=$true
    $administrators=([Security.Principal.SecurityIdentifier]::new('S-1-5-32-544')).Translate([Security.Principal.NTAccount]).Value.Split('\')[-1]
    Add-LocalGroupMember -Group $administrators -Member $user
    $null=New-Item -ItemType Directory -Path $lab
    & icacls.exe $lab /inheritance:r /grant:r "*$($account.SID.Value):(OI)(CI)F" '*S-1-5-18:(OI)(CI)F' '*S-1-5-32-544:(OI)(CI)F' | Out-Null
    if($LASTEXITCODE){throw 'Cannot protect acceptance directory.'}
    $report=Join-Path $repo 'artifacts/setup-live-evidence.json'
    @{sid=$account.SID.Value;root=(Join-Path $lab 'installed');repo=$repo;report=$report;candidate=(Join-Path $repo 'artifacts/setup-live-candidate')}|ConvertTo-Json|Set-Content -LiteralPath (Join-Path $lab 'fixture.json') -Encoding UTF8
    $security=New-Object IO.Pipes.PipeSecurity
    foreach($sid in @($account.SID,[Security.Principal.WindowsIdentity]::GetCurrent().User,[Security.Principal.SecurityIdentifier]::new('S-1-5-18'))){$security.AddAccessRule([IO.Pipes.PipeAccessRule]::new($sid,[IO.Pipes.PipeAccessRights]::FullControl,[Security.AccessControl.AccessControlType]::Allow))}
    $server=[IO.Pipes.NamedPipeServerStream]::new($pipeName,[IO.Pipes.PipeDirection]::Out,1,[IO.Pipes.PipeTransmissionMode]::Byte,[IO.Pipes.PipeOptions]::Asynchronous,4096,4096,$security)
    $pwsh=(Get-Command pwsh.exe).Source
    $script=Join-Path $repo 'scripts/package/test-setup-live.ps1'
    $action=New-ScheduledTaskAction -Execute $pwsh -Argument ('-NoLogo -NoProfile -NonInteractive -ExecutionPolicy Bypass -File "'+$script+'" -Lab "'+$lab+'" -PipeName '+$pipeName)
    $principal=New-ScheduledTaskPrincipal -UserId ($env:COMPUTERNAME+'\'+$user) -LogonType Password -RunLevel Highest
    $definition=New-ScheduledTask -Action $action -Principal $principal -Settings (New-ScheduledTaskSettingsSet -ExecutionTimeLimit (New-TimeSpan -Minutes 30))
    $null=Register-ScheduledTask -TaskName $task -InputObject $definition -User ($env:COMPUTERNAME+'\'+$user) -Password $password
    Start-ScheduledTask -TaskName $task
    $connection=$server.BeginWaitForConnection($null,$null)
    if(!$connection.AsyncWaitHandle.WaitOne(180000)){throw 'Acceptance account did not connect.'}
    $server.EndWaitForConnection($connection)
    $writer=[IO.StreamWriter]::new($server,[Text.UTF8Encoding]::new($false),4096,$true)
    try {$writer.WriteLine($password);$writer.Flush()} finally {$writer.Dispose()}
    $password=$null;$secure.Dispose()
    $deadline=[DateTime]::UtcNow.AddMinutes(30)
    do {Start-Sleep -Seconds 5;$info=Get-ScheduledTaskInfo -TaskName $task;$state=(Get-ScheduledTask -TaskName $task).State}while(($state -eq 'Running' -or $info.LastTaskResult -eq 267009) -and [DateTime]::UtcNow -lt $deadline)
    if($state -eq 'Running'){throw 'Acceptance timed out.'}
    if(!(Test-Path -LiteralPath $report)){throw 'Acceptance produced no safe report.'}
    $proof=Get-Content -LiteralPath $report -Raw|ConvertFrom-Json
    $proof|ConvertTo-Json -Depth 8
    if($info.LastTaskResult -ne 0 -or !$proof.success){throw 'Full installer acceptance failed; see the safe stage report.'}
} finally {
    if($server){$server.Dispose()}
    Stop-ScheduledTask -TaskName $task -ErrorAction SilentlyContinue
    Unregister-ScheduledTask -TaskName $task -Confirm:$false -ErrorAction SilentlyContinue
    foreach($name in @('Kafdeck','Kafdeck-Keycloak','Kafdeck-PostgreSQL')){
        $service=Get-CimInstance Win32_Service -Filter "Name='$name'"
        if($service -and $service.PathName.Replace('/','\').TrimStart('"').StartsWith($lab+'\',[StringComparison]::OrdinalIgnoreCase)){
            Stop-Service -Name $name -ErrorAction SilentlyContinue
            & "$env:SystemRoot/System32/sc.exe" delete $name|Out-Null
        }
    }
    if($created){Remove-LocalUser -Name $user}
    $password=$null
    # The VM is discarded by GitHub. Never upload its private state, profile, database or credentials.
}
