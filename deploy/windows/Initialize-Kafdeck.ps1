#requires -Version 5.1
[CmdletBinding()]
param(
    [Parameter(Mandatory)][uri]$PublicOrigin,
    [Parameter(Mandatory)][uri]$KeycloakIssuer,
    [Parameter(Mandatory)][string]$CertificateFile,
    [Parameter(Mandatory)][string[]]$RegistrationNetworks,
    [string]$BrowserClientId='kafdeck-browser',
    [string]$ManagementClientId='kafdeck-user-management',
    [string]$IssuerCaFile,
    [Security.SecureString]$BrowserSecret,
    [Security.SecureString]$ManagementSecret,
    [Security.SecureString]$CertificatePassword
)
$ErrorActionPreference='Stop'
if($PublicOrigin.Scheme -ne 'https' -or $PublicOrigin.AbsolutePath -ne '/' -or $PublicOrigin.Query -or $PublicOrigin.Fragment -or $PublicOrigin.UserInfo -or $PublicOrigin.Host -in @('0.0.0.0','::')) {throw 'Use the concrete public HTTPS origin, without a path or credentials.'}
if($KeycloakIssuer.Scheme -ne 'https' -or $KeycloakIssuer.AbsolutePath -notmatch '^/realms/[^/]+$' -or $KeycloakIssuer.AbsolutePath -eq '/realms/master' -or $KeycloakIssuer.Query -or $KeycloakIssuer.Fragment -or $KeycloakIssuer.UserInfo) {throw 'Use a dedicated HTTPS Keycloak realm issuer.'}
if(!$RegistrationNetworks.Count -or ($RegistrationNetworks | Where-Object {$_ -notmatch '/\d+$'})){throw 'Specify approved Kafka CIDRs.'}
$root=Split-Path -Parent $PSScriptRoot
$config=Join-Path $root 'config'
$secrets=Join-Path $root 'secrets'
$state=Join-Path $root 'state'
if(Test-Path -LiteralPath (Join-Path $config 'appsettings.Production.json')) {throw 'Installation already configured. Edit its existing configuration instead of reinitializing.'}
$certificate=(Resolve-Path -LiteralPath $CertificateFile).Path
foreach($directory in @($config,$secrets,$state)) {
    New-Item -ItemType Directory -Force -Path $directory | Out-Null
    $sid=[Security.Principal.WindowsIdentity]::GetCurrent().User.Value
    & icacls.exe $directory /inheritance:r /grant:r "*$($sid):(OI)(CI)F" '*S-1-5-18:(OI)(CI)F' '*S-1-5-32-544:(OI)(CI)F' | Out-Null
    if($LASTEXITCODE){throw 'Could not secure the installation directories.'}
}
if(!$BrowserSecret){$BrowserSecret=Read-Host 'Keycloak browser client secret' -AsSecureString}
if(!$ManagementSecret){$ManagementSecret=Read-Host 'Keycloak management client secret' -AsSecureString}
if(!$CertificatePassword){$CertificatePassword=Read-Host 'PFX certificate password' -AsSecureString}
if(!$BrowserSecret.Length -or !$ManagementSecret.Length){throw 'Both separate client secrets are required.'}
$utf8=New-Object Text.UTF8Encoding($false)
[IO.File]::WriteAllText((Join-Path $secrets 'browser-secret'),[Net.NetworkCredential]::new('',$BrowserSecret).Password,$utf8)
[IO.File]::WriteAllText((Join-Path $secrets 'management-secret'),[Net.NetworkCredential]::new('',$ManagementSecret).Password,$utf8)
[IO.File]::WriteAllText((Join-Path $secrets 'https-password.dpapi'),($CertificatePassword | ConvertFrom-SecureString),$utf8)
Copy-Item -LiteralPath $certificate -Destination (Join-Path $secrets 'https.pfx')
$oidc=[ordered]@{Issuer=$KeycloakIssuer.AbsoluteUri;ClientId=$BrowserClientId;ClientSecret='file:'+(Join-Path $secrets 'browser-secret');GroupClaim='groups';Scopes=@('openid','profile','email')}
if($IssuerCaFile) {
    Copy-Item -LiteralPath (Resolve-Path -LiteralPath $IssuerCaFile).Path -Destination (Join-Path $secrets 'issuer-ca.pem')
    $oidc.TrustedCaPemFile=Join-Path $secrets 'issuer-ca.pem'
}
$settings=[ordered]@{
    Kestrel=@{Certificates=@{Default=@{Path=(Join-Path $secrets 'https.pfx')}}}
    Kafdeck=@{
        Deployment=@{ListenUrls=@($PublicOrigin.GetLeftPart([UriPartial]::Authority));AccessMode='Oidc';Oidc=$oidc}
        ManagedIdentity=@{Enabled=$true;StateFile=(Join-Path $state 'identity-state.json');ServiceClientId=$ManagementClientId;ServiceSecretFile=(Join-Path $secrets 'management-secret')}
        ConnectionWorkspace=@{Enabled=$true;RegistrationNetworks=$RegistrationNetworks}
        Monitoring=@{DatabasePath=(Join-Path $state 'kafdeck-monitoring.sqlite');MetadataClusters=@()}
        Clusters=@()
    }
}
[IO.File]::WriteAllText((Join-Path $config 'appsettings.Production.json'),($settings | ConvertTo-Json -Depth 12),$utf8)
Write-Host 'Configured. Run Start-Kafdeck.ps1 -IssueSetupCode, then Start-Kafdeck.ps1 using this same Windows account.'
