#requires -Version 5.1
# Read-only identity regression. No services are created, stopped or changed.
$ErrorActionPreference='Stop'
$repo=[IO.Path]::GetFullPath((Join-Path $PSScriptRoot '../..'))
. (Join-Path $repo 'deploy/windows/Windows-Services.ps1')
function Assert($Condition,[string]$Message){if(!$Condition){throw $Message}}
$checks=0
$identity=[Security.Principal.WindowsIdentity]::GetCurrent()
$sid=$identity.User.Value
$user=$identity.Name
# Upgrade preflight must accept the path separators used by Join-Path and SCM.
foreach($registered in @('C:\Kafdeck\services\Kafdeck\Kafdeck.exe','"C:/Kafdeck/services/Kafdeck/Kafdeck.exe"','  "c:\kafdeck\services\Kafdeck\Kafdeck.exe"  ')){
    Assert-OfflineServiceOwner ([pscustomobject]@{Name='Kafdeck';PathName=$registered;StartName=$user}) (Join-Path 'C:\Kafdeck' 'services/Kafdeck/Kafdeck.exe') $sid
    $checks++
}
# A real local account exercises .\user resolution even when CI runs as a domain user.
$localAccount=Get-CimInstance Win32_UserAccount -Filter 'LocalAccount=True' | Select-Object -First 1
Assert ($null -ne $localAccount) 'A local account is required for the SCM alias regression'
Assert-OfflineServiceOwner ([pscustomobject]@{Name='Kafdeck';PathName='"C:\Kafdeck\services\Kafdeck\Kafdeck.exe"';StartName=('.\'+$localAccount.Name)}) 'C:\Kafdeck\services/Kafdeck/Kafdeck.exe' $localAccount.SID
$checks++
foreach($registered in @('"C:\Kafdeck\services\Kafdeck\Kafdeck.exe" --other-config', 'C:\Other\services\Kafdeck\Kafdeck.exe', 'C:\Kafdeck\services\Kafdeck\Kafdeck.exe.bak')){
    $rejected=$false
    try {Assert-OfflineServiceOwner ([pscustomobject]@{Name='Kafdeck';PathName=$registered;StartName=$user}) 'C:\Kafdeck\services/Kafdeck/Kafdeck.exe' $sid}catch{$rejected=$true}
    Assert $rejected 'A different executable or command line was accepted'
    $checks++
}

foreach($account in @('NT AUTHORITY\SYSTEM', 'KafdeckNonexistentDomain\NoSuchUser')){
    $rejected=$false
    try {Assert-OfflineServiceOwner ([pscustomobject]@{Name='Kafdeck';PathName='"C:\Kafdeck\services\Kafdeck\Kafdeck.exe"';StartName=$account}) 'C:\Kafdeck\services/Kafdeck/Kafdeck.exe' $sid}catch{$rejected=$true}
    Assert $rejected 'A different or unresolvable account was accepted'
    $checks++
}
Assert-OfflineServiceOwner $null 'C:\Kafdeck\services/Kafdeck/Kafdeck.exe' $sid
$checks++
Write-Host "PASS $checks service ownership checks on Windows PowerShell $($PSVersionTable.PSVersion). No service changes."
