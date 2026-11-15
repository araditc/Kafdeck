#requires -Version 5.1
[CmdletBinding()]
param([switch]$IssueSetupCode)
$ErrorActionPreference='Stop'
$root=Split-Path -Parent $PSScriptRoot
$config=Join-Path $root 'config'
$binary=Join-Path $root 'app/Kafdeck.Api.exe'
if(!(Test-Path -LiteralPath (Join-Path $config 'appsettings.Production.json'))) {throw 'Run Initialize-Kafdeck.ps1 first.'}
$passwordFile=Join-Path $root 'secrets/https-password.dpapi'
if(!(Test-Path -LiteralPath $passwordFile)) {throw 'HTTPS password file is missing; use the installation account.'}
$secure=Get-Content -LiteralPath $passwordFile -Raw | ConvertTo-SecureString
$previousEnvironment=$env:ASPNETCORE_ENVIRONMENT
$previousPassword=$env:ASPNETCORE_Kestrel__Certificates__Default__Password
try {
    $env:ASPNETCORE_ENVIRONMENT='Production'
    $env:ASPNETCORE_Kestrel__Certificates__Default__Password=[Net.NetworkCredential]::new('', $secure).Password
    $arguments=@('--contentRoot',$config,'--webroot',(Join-Path $root 'app/wwwroot'))
    if($IssueSetupCode){$arguments+='--issue-setup-code'}
    & $binary @arguments
    if($LASTEXITCODE){throw "Kafdeck exited with code $LASTEXITCODE"}
} finally {
    $env:ASPNETCORE_ENVIRONMENT=$previousEnvironment
    $env:ASPNETCORE_Kestrel__Certificates__Default__Password=$previousPassword
    $secure.Dispose()
}
