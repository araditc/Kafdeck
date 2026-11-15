#requires -Version 5.1
[CmdletBinding()]
param(
    [Parameter(Mandatory)][string]$ServerAddress,
    [Parameter(Mandatory)][string[]]$KafkaNetworks,
    [Parameter(Mandatory)][string]$KeycloakHome,
    [string]$JavaHome=$env:JAVA_HOME,
    [ValidateRange(1024,65535)][int]$AppPort=8080,
    [ValidateRange(1024,65535)][int]$IdentityPort=8443,
    [ValidateRange(1024,65535)][int]$DatabasePort=5432
)
$ErrorActionPreference='Stop'
if($ServerAddress -notmatch '^[a-zA-Z0-9][a-zA-Z0-9.-]*$'){throw 'Use an IPv4 address or DNS name, without https:// or a port.'}
if(!$JavaHome -or !(Test-Path -LiteralPath (Join-Path $JavaHome 'bin/java.exe'))){throw 'JAVA_HOME is missing. Install Java 21, enable JAVA_HOME, and reopen PowerShell.'}
$probe=[Diagnostics.ProcessStartInfo]::new((Join-Path $JavaHome 'bin/java.exe'),'-version')
$probe.UseShellExecute=$false;$probe.CreateNoWindow=$true;$probe.RedirectStandardError=$true;$probe.RedirectStandardOutput=$true
$javaProcess=[Diagnostics.Process]::Start($probe)
$javaVersion=$javaProcess.StandardError.ReadToEnd()+$javaProcess.StandardOutput.ReadToEnd()
$javaProcess.WaitForExit()
if($javaVersion -notmatch 'version "21\.'){throw 'This installer requires Java 21.'}
$KeycloakHome=(Resolve-Path -LiteralPath $KeycloakHome).Path
if(!(Test-Path -LiteralPath (Join-Path $KeycloakHome 'bin/kc.bat'))){throw 'KeycloakHome must contain bin/kc.bat.'}
$root=Split-Path -Parent $PSScriptRoot
$secrets=Join-Path $root 'secrets'
$config=Join-Path $root 'config'
$realmFile=Join-Path $KeycloakHome 'data/import/kafdeck-realm.json'
if((Test-Path -LiteralPath (Join-Path $config 'appsettings.Production.json')) -or (Test-Path -LiteralPath $realmFile)){throw 'This helper is for a fresh installation only; existing settings will not be overwritten.'}
function PrivateDirectory([string]$path){
    New-Item -ItemType Directory -Path $path -Force | Out-Null
    $sid=[Security.Principal.WindowsIdentity]::GetCurrent().User.Value
    & icacls.exe $path /inheritance:r /grant:r "*$($sid):(OI)(CI)F" '*S-1-5-18:(OI)(CI)F' '*S-1-5-32-544:(OI)(CI)F' | Out-Null
    if($LASTEXITCODE){throw 'Could not secure an installation directory.'}
}
function RandomSecret {
    $bytes=New-Object byte[] 32
    $rng=[Security.Cryptography.RandomNumberGenerator]::Create()
    try {$rng.GetBytes($bytes)} finally {$rng.Dispose()}
    return [BitConverter]::ToString($bytes).Replace('-','').ToLowerInvariant()
}
$utf8=New-Object Text.UTF8Encoding($false)
foreach($path in @($secrets,$config,(Split-Path -Parent $realmFile))){PrivateDirectory $path}
$password=ConvertTo-SecureString (RandomSecret) -AsPlainText -Force
$browserSecret=RandomSecret
$managementSecret=RandomSecret
$databaseSecret=RandomSecret
. (Join-Path $PSScriptRoot 'New-InstallCertificate.ps1')
$tls=New-KafdeckInstallCertificate -ServerAddress $ServerAddress -Secrets $secrets -Password $password
$certificate=$tls.Certificate;$pfx=$tls.Pfx
$origin="https://${ServerAddress}:$AppPort"
$idpOrigin="https://${ServerAddress}:$IdentityPort"
$profile=@{attributes=@(
    @{name='username';permissions=@{view=@('admin','user');edit=@('admin')};validations=@{length=@{min=3;max=255}}},
    @{name='email';permissions=@{view=@('admin','user');edit=@('admin','user')};validations=@{email=@{}};required=@{roles=@('user')}},
    @{name='firstName';permissions=@{view=@('admin','user');edit=@('admin','user')}},
    @{name='lastName';permissions=@{view=@('admin','user');edit=@('admin','user')}},
    @{name='kafdeckProvisioningId';multivalued=$false;permissions=@{view=@('admin');edit=@('admin')}}
)}
$realm=@{
    realm='kafdeck';enabled=$true;sslRequired='all';registrationAllowed=$false;verifyEmail=$false
    loginWithEmailAllowed=$true;duplicateEmailsAllowed=$false;resetPasswordAllowed=$false;bruteForceProtected=$true;passwordPolicy='length(12)'
    clients=@(
        @{clientId='kafdeck-browser';enabled=$true;protocol='openid-connect';publicClient=$false;clientAuthenticatorType='client-secret';secret=$browserSecret;standardFlowEnabled=$true;directAccessGrantsEnabled=$false;serviceAccountsEnabled=$false;redirectUris=@("$origin/signin-oidc");webOrigins=@($origin);defaultClientScopes=@('profile','email');attributes=@{'pkce.code.challenge.method'='S256';'post.logout.redirect.uris'="$origin/signout-callback-oidc##$origin/"}},
        @{clientId='kafdeck-user-management';enabled=$true;protocol='openid-connect';publicClient=$false;clientAuthenticatorType='client-secret';secret=$managementSecret;standardFlowEnabled=$false;directAccessGrantsEnabled=$false;serviceAccountsEnabled=$true;defaultClientScopes=@('roles');fullScopeAllowed=$true}
    )
    users=@(@{username='service-account-kafdeck-user-management';enabled=$true;serviceAccountClientId='kafdeck-user-management';clientRoles=@{'realm-management'=@('manage-users')}})
    components=@{'org.keycloak.userprofile.UserProfileProvider'=@(@{providerId='declarative-user-profile';config=@{'kc.user.profile.config'=@(($profile | ConvertTo-Json -Depth 15 -Compress))}})}
}
[IO.File]::WriteAllText($realmFile,($realm | ConvertTo-Json -Depth 20),$utf8)
[IO.File]::WriteAllText((Join-Path $secrets 'keycloak-database-password.dpapi'),((ConvertTo-SecureString $databaseSecret -AsPlainText -Force) | ConvertFrom-SecureString),$utf8)
$sql="\set ON_ERROR_STOP on`nCREATE ROLE kafdeck_keycloak LOGIN PASSWORD '$databaseSecret';`nCREATE DATABASE kafdeck_keycloak OWNER kafdeck_keycloak;`n"
[IO.File]::WriteAllText((Join-Path $secrets 'create-keycloak-db.sql'),$sql,$utf8)
$kcConf=@"
db=postgres
db-url=jdbc:postgresql://127.0.0.1:$DatabasePort/kafdeck_keycloak
db-username=kafdeck_keycloak
hostname=$idpOrigin
https-port=$IdentityPort
https-key-store-file=$((Join-Path $secrets 'https.pfx').Replace('\','/'))
http-enabled=false
cache=local
"@
[IO.File]::WriteAllText((Join-Path $config 'keycloak.conf'),$kcConf+[Environment]::NewLine,$utf8)
[IO.File]::WriteAllText((Join-Path $config 'local-keycloak.json'),(@{home=$KeycloakHome;javaHome=$JavaHome;origin=$idpOrigin;certificateThumbprint=$certificate.Thumbprint;caThumbprint=$tls.Authority.Thumbprint}|ConvertTo-Json),$utf8)
& (Join-Path $PSScriptRoot 'Initialize-Kafdeck.ps1') -PublicOrigin $origin -KeycloakIssuer "$idpOrigin/realms/kafdeck" -CertificateFile $pfx -RegistrationNetworks $KafkaNetworks -IssuerCaFile (Join-Path $secrets 'server-public.pem') -BrowserSecret (ConvertTo-SecureString $browserSecret -AsPlainText -Force) -ManagementSecret (ConvertTo-SecureString $managementSecret -AsPlainText -Force) -CertificatePassword $password
$password.Dispose()
Write-Host "Prepared $origin and $idpOrigin. Public CA thumbprint: $($tls.Authority.Thumbprint)"
Write-Host 'Next: create the database using the supplied SQL file, trust server-public.cer, and run Start-Keycloak.ps1 -BootstrapAdmin.'
