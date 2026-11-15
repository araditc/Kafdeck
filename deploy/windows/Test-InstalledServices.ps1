#requires -Version 5.1
param([switch]$RequireReboot)
$ErrorActionPreference='Stop'
[Net.ServicePointManager]::SecurityProtocol=[Net.SecurityProtocolType]::Tls12
$root=Split-Path -Parent $PSScriptRoot
$receipt=Get-Content -LiteralPath (Join-Path $root 'config/offline-install.json') -Raw|ConvertFrom-Json
if($RequireReboot){
    $boot=(Get-CimInstance Win32_OperatingSystem).LastBootUpTime.ToUniversalTime()
    if(!$receipt.servicesInstalledAt -or $boot -le [DateTime]::Parse($receipt.servicesInstalledAt).ToUniversalTime()){throw 'Restart Windows after installing the services, then run this check again.'}
}
. (Join-Path $PSScriptRoot 'Offline-Tools.ps1')
foreach($name in @('Kafdeck-PostgreSQL','Kafdeck-Keycloak','Kafdeck')){
    $service=Get-CimInstance Win32_Service -Filter "Name='$name'"
    if(!$service -or $service.State -ne 'Running' -or $service.StartMode -ne 'Auto'){throw "$name is not running with automatic startup. Inspect services.msc and logs."}
    if($name -ne 'Kafdeck-PostgreSQL'){
        $dependency=if($name -eq 'Kafdeck'){'Kafdeck-Keycloak'}else{'Kafdeck-PostgreSQL'}
        if((Get-Service -Name $name).ServicesDependedOn.Name -notcontains $dependency){throw "Missing service dependency: $name -> $dependency"}
        if(!(Get-ItemPropertyValue -LiteralPath "HKLM:\SYSTEM\CurrentControlSet\Services\$name" -Name DelayedAutostart)){throw "Missing delayed automatic start: $name"}
    }
    Write-Host "$name : Running / Automatic"
}
$null=Wait-OfflineUrl "$($receipt.identityOrigin)/realms/kafdeck/.well-known/openid-configuration" 600
$null=Wait-OfflineUrl "$($receipt.origin)/healthz" 180
Write-Host "HTTPS and Keycloak ready: $($receipt.origin)/"
if($RequireReboot){Write-Host 'All three services and both HTTPS endpoints passed after Windows reboot.'}
