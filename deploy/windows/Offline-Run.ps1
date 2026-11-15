#requires -Version 5.1
param([Parameter(Mandatory)][ValidateSet('Api','Keycloak')][string]$Component)
$ErrorActionPreference='Stop'
$root=Split-Path -Parent $PSScriptRoot
. (Join-Path $PSScriptRoot 'Offline-Tools.ps1')
[Net.ServicePointManager]::SecurityProtocol=[Net.SecurityProtocolType]::Tls12
try {
    $receipt=Get-Content -LiteralPath (Join-Path $root 'config/offline-install.json') -Raw|ConvertFrom-Json
    if([Security.Principal.WindowsIdentity]::GetCurrent().User.Value -ne $receipt.userSid){throw 'Service account does not match the installation (DPAPI).'}
    if($Component -eq 'Keycloak'){
        $password=Read-OfflineSecret (Join-Path $root 'secrets/keycloak-database-password.dpapi')
        $ready=$false;$deadline=(Get-Date).AddSeconds(120)
        do {
            try {
                $null=Invoke-OfflineNative -File (Join-Path $root 'runtime/postgresql/bin/psql.exe') -Arguments @('-X','--no-password','-h','127.0.0.1','-p',[string]$receipt.databasePort,'-U','kafdeck_keycloak','-d','kafdeck_keycloak','-v','ON_ERROR_STOP=1','-c','SELECT 1') -Environment @{PGPASSWORD=$password;PGCONNECT_TIMEOUT='3'} -TimeoutSeconds 10
                $ready=$true
            }catch {Start-Sleep -Seconds 2}
        }while(!$ready -and (Get-Date) -lt $deadline)
        $password=$null
        if(!$ready){throw 'PostgreSQL was not ready for Keycloak within 120 seconds.'}
    }else{
        $null=Wait-OfflineUrl "$($receipt.identityOrigin)/realms/kafdeck/.well-known/openid-configuration" 600
    }
    $script=Join-Path $PSScriptRoot $(if($Component -eq 'Api'){'Start-Kafdeck.ps1'}else{'Start-Keycloak.ps1'})
    & $script
    # Any unsolicited exit must reach SCM recovery, even a clean child exit.
    throw "$Component exited unexpectedly. Windows Service recovery will restart it."
}catch {
    [Console]::Error.WriteLine($_.Exception.Message)
    exit 1
}
