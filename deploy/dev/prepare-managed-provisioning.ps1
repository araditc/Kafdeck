param(
    [Parameter(Mandatory=$true)][string]$KeycloakHome,
    [Parameter(Mandatory=$true)][string]$OutputDirectory,
    [Parameter(Mandatory=$true)][string]$CaFile,
    [string]$Issuer = 'https://localhost:18443/realms/kafdeck-managed-g01'
)
$ErrorActionPreference = 'Stop'
$lab = [IO.Path]::GetFullPath($OutputDirectory)
if (Test-Path -LiteralPath $lab) { throw 'Use a fresh managed provisioning laboratory directory.' }
if (!(Test-Path -LiteralPath $CaFile)) { throw 'Prepare the owned HTTPS Keycloak fixture first.' }
New-Item -ItemType Directory -Path $lab | Out-Null
$secret = [Convert]::ToHexString([Security.Cryptography.RandomNumberGenerator]::GetBytes(32))
$controlSecret = [Convert]::ToHexString([Security.Cryptography.RandomNumberGenerator]::GetBytes(32))
$realm = @{
    realm='kafdeck-managed-g01'; enabled=$true; sslRequired='all'
    registrationAllowed=$false; verifyEmail=$false; resetPasswordAllowed=$false
    clients=@(
        @{clientId='g01-provisioner'; enabled=$true; protocol='openid-connect'; clientAuthenticatorType='client-secret'; secret=$secret; serviceAccountsEnabled=$true; standardFlowEnabled=$false; directAccessGrantsEnabled=$false},
        @{clientId='g01-control'; enabled=$true; protocol='openid-connect'; clientAuthenticatorType='client-secret'; secret=$controlSecret; serviceAccountsEnabled=$true; standardFlowEnabled=$false; directAccessGrantsEnabled=$false}
    )
    users=@(
        @{username='service-account-g01-provisioner'; enabled=$true; serviceAccountClientId='g01-provisioner'; clientRoles=@{'realm-management'=@('manage-users')}},
        @{username='service-account-g01-control'; enabled=$true; serviceAccountClientId='g01-control'; clientRoles=@{'realm-management'=@('manage-users','manage-realm')}}
    )
}
$import = Join-Path ([IO.Path]::GetFullPath($KeycloakHome)) 'data/import/kafdeck-managed-g01-realm.json'
$realm | ConvertTo-Json -Depth 20 | Set-Content -LiteralPath $import -Encoding utf8
@{issuer=$Issuer; caFile=[IO.Path]::GetFullPath($CaFile); clientId='g01-provisioner'; secret=$secret; controlClientId='g01-control'; controlSecret=$controlSecret} |
    ConvertTo-Json | Set-Content -LiteralPath (Join-Path $lab 'lab.json') -Encoding utf8
# This file and realm import contain test-only secrets. Only sanitized TRX results
# are retained; neither laboratory configuration is a public evidence artifact.
Write-Output 'Prepared an isolated managed-provisioning realm for new G01 acceptance.'
