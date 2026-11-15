#requires -Version 5.1
$ErrorActionPreference='Stop'
$root=Split-Path -Parent $PSScriptRoot
. (Join-Path $PSScriptRoot 'Offline-Tools.ps1')
$receipt=Get-Content -LiteralPath (Join-Path $root 'config/offline-install.json') -Raw|ConvertFrom-Json
if([Security.Principal.WindowsIdentity]::GetCurrent().User.Value -ne $receipt.userSid){throw 'Use the installation Windows account.'}
Write-Host "Identity administration: $($receipt.identityOrigin)/admin"
Write-Host 'Username: kafdeck-identity-admin'
Write-Host ('Password: '+(Read-OfflineSecret (Join-Path $root 'secrets/identity-admin-password.dpapi')))
Write-Host 'Keep this recovery password private. It is separate from the first Kafdeck administrator.'
