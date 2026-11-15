#requires -Version 5.1
param([Parameter(Mandatory)][ValidateSet('Api','Keycloak')][string]$Component)
$ErrorActionPreference='Stop'
$root=Split-Path -Parent $PSScriptRoot
$log=Join-Path $root ("logs/"+$Component+'-'+(Get-Date -Format yyyyMMdd)+'.log')
try {
    $script=Join-Path $PSScriptRoot $(if($Component -eq 'Api'){'Start-Kafdeck.ps1'}else{'Start-Keycloak.ps1'})
    & $script *>> $log
}catch {
    $_.Exception.Message|Out-File -LiteralPath $log -Append
    exit 1
}
