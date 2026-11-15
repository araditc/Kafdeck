#requires -Version 5.1
$ErrorActionPreference='Stop'
$root=Split-Path -Parent $PSScriptRoot
. (Join-Path $PSScriptRoot 'Offline-Tools.ps1')
$receipt=Get-Content -LiteralPath (Join-Path $root 'config/offline-install.json') -Raw|ConvertFrom-Json
if([Security.Principal.WindowsIdentity]::GetCurrent().User.Value -ne $receipt.userSid){throw 'Use the Windows account that installed Kafdeck.'}
$password=Read-OfflineSecret (Join-Path $root 'secrets/https-password.dpapi')
$text=Invoke-OfflineNative -File (Join-Path $root 'app/Kafdeck.Api.exe') -Arguments @('--contentRoot',(Join-Path $root 'config'),'--issue-setup-code') -Environment @{ASPNETCORE_ENVIRONMENT='Production';ASPNETCORE_Kestrel__Certificates__Default__Password=$password}
$code=($text -split '\r?\n'|Where-Object {$_ -match '^[A-Za-z0-9_-]{32}$'}|Select-Object -Last 1)
if(!$code){throw 'Setup code was not issued. If the first owner already exists, sign in instead.'}
$message="First administrator: $($receipt.origin)/`r`nOne-time code (15 minutes): $code`r`n"
[IO.File]::WriteAllText((Join-Path $root 'secrets/FIRST-ADMIN.txt'),$message,(New-Object Text.UTF8Encoding($false)))
Write-Host $message
