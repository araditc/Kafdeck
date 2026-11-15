#requires -Version 5.1
[CmdletBinding()]
param([string]$TaskName,[switch]$Keycloak)
$ErrorActionPreference='Stop'
if(!$TaskName){$TaskName=if($Keycloak){'Kafdeck-Keycloak'}else{'Kafdeck'}}
$identity=[Security.Principal.WindowsIdentity]::GetCurrent()
if(Get-ScheduledTask -TaskName $TaskName -ErrorAction SilentlyContinue){throw 'Task already exists; inspect it instead of overwriting.'}
$credential=Get-Credential -UserName $identity.Name -Message 'Use the SAME Windows account that initialized Kafdeck (DPAPI and key ring).'
if(!$credential){throw 'No credential supplied.'}
$account=New-Object Security.Principal.NTAccount($credential.UserName)
if($account.Translate([Security.Principal.SecurityIdentifier]).Value -ne $identity.User.Value){throw 'The account must match the installation identity.'}
$script=Join-Path $PSScriptRoot $(if($Keycloak){'Start-Keycloak.ps1'}else{'Start-Kafdeck.ps1'})
$action=New-ScheduledTaskAction -Execute "$env:SystemRoot/System32/WindowsPowerShell/v1.0/powershell.exe" -Argument "-NoProfile -NonInteractive -File `"$script`"" -WorkingDirectory (Split-Path -Parent $PSScriptRoot)
$settings=New-ScheduledTaskSettingsSet -StartWhenAvailable -RestartCount 5 -RestartInterval (New-TimeSpan -Minutes 1) -ExecutionTimeLimit ([TimeSpan]::Zero) -MultipleInstances IgnoreNew
Register-ScheduledTask -TaskName $TaskName -Action $action -Trigger (New-ScheduledTaskTrigger -AtStartup) -Settings $settings -User $credential.UserName -Password $credential.GetNetworkCredential().Password | Out-Null
Write-Host 'Startup task registered. Stop any foreground Kafdeck instance before Start-ScheduledTask.'
