#requires -Version 5.1
$ErrorActionPreference='Stop'
$repo=Split-Path -Parent (Split-Path -Parent $PSScriptRoot)
. (Join-Path $repo 'deploy/windows/Offline-Tools.ps1')
$account=[Security.Principal.WindowsIdentity]::GetCurrent().Name
$checks=0
& {
    function Read-Host {param($Prompt,[switch]$AsSecureString) if(!$AsSecureString){throw 'Password prompt must be masked'};ConvertTo-SecureString 'TEST-ONLY-Password' -AsPlainText -Force}
    $credential=Get-OfflineWindowsCredential
    if($credential.UserName -ne $account -or $credential.Password.Length -ne 18){throw 'Console prompt did not bind to current identity'}
    $credential.Password.Dispose()
};$checks++
& {
    function Read-Host {param($Prompt,[switch]$AsSecureString) New-Object Security.SecureString}
    $rejected=$false
    try {$null=Get-OfflineWindowsCredential}catch {if($_.Exception.Message -like '*No Windows password*'){$rejected=$true}else{throw}}
    if(!$rejected){throw 'Empty console input was accepted'}
};$checks++
$credential=[PSCredential]::new($account,(ConvertTo-SecureString 'TEST-ONLY-Password' -AsPlainText -Force))
if((Get-OfflineWindowsCredential $credential) -ne $credential){throw 'Explicit same-account credential changed'};$credential.Password.Dispose();$checks++
$foreign=[PSCredential]::new('NT AUTHORITY\SYSTEM',(ConvertTo-SecureString 'TEST-ONLY-Password' -AsPlainText -Force))
$rejected=$false
try {$null=Get-OfflineWindowsCredential $foreign}catch {if($_.Exception.Message -like '*Windows account mismatch*'){$rejected=$true}else{throw}}
if(!$rejected){throw 'Different account was accepted'};$foreign.Password.Dispose();$checks++
$tokens=$null;$syntax=$null
$null=[Management.Automation.Language.Parser]::ParseFile((Join-Path $repo 'deploy/windows/Install-Offline.ps1'),[ref]$tokens,[ref]$syntax)
if($syntax.Count){throw 'Installer syntax invalid'};$checks++
$evidence=@{checks=$checks;windowsPowerShell=$PSVersionTable.PSVersion.ToString();maskedConsolePrompt=$true;currentAccountAutomatic=$true;blankPasswordRejected=$true;foreignAccountRejected=$true;actualServiceLogon='not executed; unrelated successful checks reused'}
$evidence|ConvertTo-Json|Set-Content -LiteralPath (Join-Path $repo 'artifacts/windows-offline/credential-prompt-evidence.json') -Encoding utf8
$evidence|ConvertTo-Json
