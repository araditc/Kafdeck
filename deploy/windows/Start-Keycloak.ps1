#requires -Version 5.1
[CmdletBinding()]
param([switch]$BootstrapAdmin)
$ErrorActionPreference='Stop'
$root=Split-Path -Parent $PSScriptRoot
$config=Join-Path $root 'config'
$secrets=Join-Path $root 'secrets'
$settings=Get-Content -Raw -LiteralPath (Join-Path $config 'local-keycloak.json') | ConvertFrom-Json
$saved=@{}
foreach($name in @('JAVA_HOME','KC_DB_PASSWORD','KC_HTTPS_KEY_STORE_PASSWORD','KC_BOOTSTRAP_ADMIN_USERNAME','KC_BOOTSTRAP_ADMIN_PASSWORD')){$saved[$name]=[Environment]::GetEnvironmentVariable($name,'Process')}
try {
    $env:JAVA_HOME=$settings.javaHome
    foreach($pair in @(@('KC_DB_PASSWORD','keycloak-database-password.dpapi'),@('KC_HTTPS_KEY_STORE_PASSWORD','https-password.dpapi'))){
        $secure=Get-Content -Raw -LiteralPath (Join-Path $secrets $pair[1]) | ConvertTo-SecureString
        try {[Environment]::SetEnvironmentVariable($pair[0],[Net.NetworkCredential]::new('',$secure).Password,'Process')} finally {$secure.Dispose()}
    }
    $env:KC_BOOTSTRAP_ADMIN_USERNAME=$null
    $env:KC_BOOTSTRAP_ADMIN_PASSWORD=$null
    if($settings.PSObject.Properties.Name -contains 'bootstrapPasswordFile' -and (Test-Path -LiteralPath $settings.bootstrapPasswordFile)) {
        $secure=Get-Content -LiteralPath $settings.bootstrapPasswordFile -Raw | ConvertTo-SecureString
        try {
            $env:KC_BOOTSTRAP_ADMIN_USERNAME='kafdeck-bootstrap'
            $env:KC_BOOTSTRAP_ADMIN_PASSWORD=[Net.NetworkCredential]::new('',$secure).Password
        } finally {$secure.Dispose()}
    } elseif($BootstrapAdmin){
        $secure=Read-Host 'Choose a temporary Keycloak administrator password (12+ characters)' -AsSecureString
        try {
            if($secure.Length -lt 12){throw 'Use at least 12 characters.'}
            $env:KC_BOOTSTRAP_ADMIN_USERNAME='bootstrap-admin'
            $env:KC_BOOTSTRAP_ADMIN_PASSWORD=[Net.NetworkCredential]::new('',$secure).Password
        } finally {$secure.Dispose()}
    }
    & (Join-Path $settings.home 'bin/kc.bat') --config-file (Join-Path $config 'keycloak.conf') start --import-realm
    if($LASTEXITCODE){throw "Keycloak exited with code $LASTEXITCODE"}
} finally {
    foreach($name in $saved.Keys){[Environment]::SetEnvironmentVariable($name,$saved[$name],'Process')}
}
