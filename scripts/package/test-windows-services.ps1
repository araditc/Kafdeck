#requires -Version 5.1
# No SCM writes: actual WinSW configuration parser plus bounded process probes.
$ErrorActionPreference='Stop'
$repo=Split-Path -Parent (Split-Path -Parent $PSScriptRoot)
$out=Join-Path $repo 'artifacts/windows-offline'
$fixture=Join-Path $out 'service-tests'
New-Item -ItemType Directory -Force (Join-Path $fixture 'runtime'),(Join-Path $fixture 'logs')|Out-Null
Copy-Item -LiteralPath (Join-Path $out 'vendor/winsw.exe') -Destination (Join-Path $fixture 'runtime/winsw.exe') -Force
. (Join-Path $repo 'deploy/windows/Offline-Tools.ps1')
. (Join-Path $repo 'deploy/windows/Windows-Services.ps1')
function Assert($Condition,[string]$Message){if(!$Condition){throw $Message}}
$checks=0
$policy=[regex]::Match([IO.File]::ReadAllText((Join-Path $repo 'deploy/windows/Windows-Services.ps1')),"(?s)Add-Type -TypeDefinition @'\r?\n(.*?)\r?\n'@").Groups[1].Value
Assert ($policy.Length -gt 0) 'Policy helper missing';Add-Type -TypeDefinition $policy;$checks++
Get-ChildItem -LiteralPath (Join-Path $repo 'deploy/windows') -Filter '*.ps1'|ForEach-Object {
    $tokens=$null;$errorsFound=$null
    $null=[Management.Automation.Language.Parser]::ParseFile($_.FullName,[ref]$tokens,[ref]$errorsFound)
    Assert (!$errorsFound.Count) ("Syntax error: "+$_.Name);$checks++
}
$assembly=[Reflection.Assembly]::LoadFrom((Join-Path $fixture 'runtime/winsw.exe'))
$configType=$assembly.GetType('WinSW.XmlServiceConfig',$true)
foreach($component in @('Keycloak','Api')){
    $exe=Write-OfflineServiceConfig $fixture $component
    $text=[IO.File]::ReadAllText([IO.Path]::ChangeExtension($exe,'.xml'))
    # Parse with the shipped WinSW, not only the generic XML parser.
    $cfg=$configType.GetMethod('FromXML').Invoke($null,@($text))
    $name=if($component -eq 'Api'){'Kafdeck'}else{'Kafdeck-Keycloak'}
    $dependency=if($component -eq 'Api'){'Kafdeck-Keycloak'}else{'Kafdeck-PostgreSQL'}
    Assert ($cfg.Name -eq $name) 'Wrong service ID';$checks++
    Assert ($cfg.Executable -eq "$env:SystemRoot\System32\WindowsPowerShell\v1.0\powershell.exe") 'Wrong absolute PowerShell path';$checks++
    Assert ($cfg.Arguments -like "*-Component $component" -and $cfg.Arguments -like '*-NonInteractive*') 'Wrong runner arguments';$checks++
    Assert ($cfg.StartMode.ToString() -eq 'Automatic' -and $cfg.DelayedAutoStart) 'Automatic delayed start not parsed';$checks++
    Assert (@($cfg.ServiceDependencies).Count -eq 1 -and $cfg.ServiceDependencies[0] -eq $dependency) 'Dependency not parsed';$checks++
    Assert ($cfg.StopTimeout.TotalSeconds -eq 60 -and !$cfg.StopParentProcessFirst) 'Child-first bounded stop not parsed';$checks++
    Assert ($text -notmatch 'password|serviceaccount|interactive>') 'Unexpected credential/interactive setting';$checks++
    Assert ((Get-FileHash -LiteralPath $exe).Hash -eq (Get-FileHash -LiteralPath (Join-Path $fixture 'runtime/winsw.exe')).Hash) 'Wrapper binary changed';$checks++
}
$sid=[Security.Principal.WindowsIdentity]::GetCurrent().User.Value
$user=[Security.Principal.WindowsIdentity]::GetCurrent().Name
Assert-OfflineServiceOwner ([pscustomobject]@{Name='Kafdeck';PathName='"C:\Kafdeck\services\Kafdeck\Kafdeck.exe"';StartName=$user}) 'C:\Kafdeck\services\Kafdeck\Kafdeck.exe' $sid
$checks++
foreach($foreign in @([pscustomobject]@{Name='Kafdeck';PathName='C:\Other\foreign.exe';StartName=$user},[pscustomobject]@{Name='Kafdeck';PathName='C:\Kafdeck\services\Kafdeck\Kafdeck.exe';StartName='NT AUTHORITY\SYSTEM'})){
    $rejected=$false
    try {Assert-OfflineServiceOwner $foreign 'C:\Kafdeck\services\Kafdeck\Kafdeck.exe' $sid}catch{$rejected=$true}
    Assert $rejected 'Foreign service was accepted';$checks++
}
# Registration boundary: verify exact SCM parameters without requiring UAC.
$script:created=@();$script:commands=@()
function Get-CimInstance {param($ClassName,$Filter) return $null}
function Get-Service {param($Name,$ErrorAction) return $null}
function Grant-OfflineServiceLogon {param($Sid) Assert ($Sid.Value -eq $sid) 'Wrong logon SID'}
function New-Service {param($Name,$DisplayName,$Description,$BinaryPathName,$StartupType,$DependsOn,$Credential) $script:created+=@{name=$Name;binary=$BinaryPathName;dependency=$DependsOn;credential=$Credential;startup=$StartupType}}
function Invoke-OfflineNative {param($File,$Arguments) $script:commands+=,@($Arguments)}
$credential=[PSCredential]::new($user,(ConvertTo-SecureString 'TEST-ONLY-NOT-A-REAL-PASSWORD' -AsPlainText -Force))
Install-OfflineWindowsServices $fixture $credential
Assert ($created.Count -eq 2 -and $created[0].name -eq 'Kafdeck-Keycloak' -and $created[1].name -eq 'Kafdeck') 'Registration order incorrect';$checks++
Assert ($created[0].dependency -eq 'Kafdeck-PostgreSQL' -and $created[1].dependency -eq 'Kafdeck-Keycloak') 'SCM dependencies incorrect';$checks++
Assert ($created[0].credential -eq $credential -and $created[1].credential -eq $credential) 'SCM credential was not passed directly';$checks++
Assert (($commands|ConvertTo-Json -Depth 5) -notmatch 'TEST-ONLY') 'Password leaked to process arguments';$checks++
Assert (@($commands|Where-Object {$_[0] -eq 'failure'}).Count -eq 3) 'All three recovery policies required';$checks++
Assert (@($commands|Where-Object {$_[0] -eq 'config' -and $_[3] -eq 'delayed-auto'}).Count -eq 2) 'Delayed service startup missing';$checks++
Assert (@($commands|Where-Object {$_[0] -eq 'failureflag' -and $_[2] -eq '1'}).Count -eq 3) 'Non-crash recovery missing';$checks++
$credential.Password.Dispose()
$result=[ordered]@{checks=$checks;windowsPowerShell=$PSVersionTable.PSVersion.ToString();actualWinSWParser=$true;scmRegistrationBoundary='mocked: no administrator token';serviceStartStopAndReboot='not executed on this host';previousRuntimeEvidence='acceptance-evidence.json; reused, not rerun';existingInstallationChanged=$false}
$result|ConvertTo-Json|Set-Content -LiteralPath (Join-Path $out 'windows-services-evidence.json') -Encoding utf8
$result|ConvertTo-Json
