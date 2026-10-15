param(
    [Parameter(Mandatory=$true)][string]$KeycloakHome,
    [string]$OutputDirectory = "$PSScriptRoot/.generated/g1/identity",
    [string]$JavaVersion = 'Temurin 21.0.12.1+1'
)
$ErrorActionPreference = 'Stop'
$labDir = [IO.Path]::GetFullPath($OutputDirectory)
New-Item -ItemType Directory -Path $labDir -Force | Out-Null
$importDir = Join-Path $KeycloakHome 'data/import'
New-Item -ItemType Directory -Path $importDir -Force | Out-Null
$tlsKey = [Security.Cryptography.RSA]::Create(2048)
$tlsRequest = [Security.Cryptography.X509Certificates.CertificateRequest]::new('CN=localhost', $tlsKey,
    [Security.Cryptography.HashAlgorithmName]::SHA256, [Security.Cryptography.RSASignaturePadding]::Pkcs1)
$san = [Security.Cryptography.X509Certificates.SubjectAlternativeNameBuilder]::new()
$san.AddDnsName('localhost')
$san.AddIpAddress([Net.IPAddress]::Loopback)
$tlsRequest.CertificateExtensions.Add($san.Build())
$tlsRequest.CertificateExtensions.Add([Security.Cryptography.X509Certificates.X509BasicConstraintsExtension]::new($true,$false,0,$true))
$tlsCert = $tlsRequest.CreateSelfSigned([DateTimeOffset]::UtcNow.AddMinutes(-5),[DateTimeOffset]::UtcNow.AddDays(2))
$p12Password = [Convert]::ToHexString([Security.Cryptography.RandomNumberGenerator]::GetBytes(24))
[IO.File]::WriteAllBytes((Join-Path $labDir 'https.p12'), $tlsCert.Export([Security.Cryptography.X509Certificates.X509ContentType]::Pkcs12,$p12Password))
[IO.File]::WriteAllText((Join-Path $labDir 'ca.pem'), $tlsCert.ExportCertificatePem())
[IO.File]::WriteAllText((Join-Path $labDir 'https-password.txt'), $p12Password)
$clientKey = [Security.Cryptography.RSA]::Create(2048)
$clientRequest = [Security.Cryptography.X509Certificates.CertificateRequest]::new('CN=kafdeck-g1-service', $clientKey,
    [Security.Cryptography.HashAlgorithmName]::SHA256,[Security.Cryptography.RSASignaturePadding]::Pkcs1)
$clientCert = $clientRequest.CreateSelfSigned([DateTimeOffset]::UtcNow.AddMinutes(-5),[DateTimeOffset]::UtcNow.AddDays(2))
[IO.File]::WriteAllText((Join-Path $labDir 'client-private.key'),$clientKey.ExportPkcs8PrivateKeyPem())
$realm = @{
    realm='kafdeck-g1'; enabled=$true; sslRequired='all'; accessTokenLifespan=120
    defaultSignatureAlgorithm='RS256'; defaultDefaultClientScopes=@(); defaultOptionalClientScopes=@()
    clients=@(
        @{
            id='f51b26b6-1152-4e2b-b18a-7c46b94c1c01'; clientId='kafdeck-g1-service'; protocol='openid-connect'
            enabled=$true; publicClient=$false; serviceAccountsEnabled=$true
            standardFlowEnabled=$false; directAccessGrantsEnabled=$false; implicitFlowEnabled=$false
            clientAuthenticatorType='client-jwt'; fullScopeAllowed=$false
            defaultClientScopes=@(); optionalClientScopes=@()
            attributes=@{
                'jwt.credential.certificate'=[Convert]::ToBase64String($clientCert.RawData)
                'token.endpoint.auth.signing.alg'='RS256'
                'client.session.max.lifespan'='120'
            }
            protocolMappers=@(@{
                name='api-audience'; protocol='openid-connect'; protocolMapper='oidc-audience-mapper'
                consentRequired=$false; config=@{
                    'included.custom.audience'='kafdeck-api-g1'; 'access.token.claim'='true'; 'id.token.claim'='false'
                }
            })
        },
        @{
            clientId='kafdeck-g1-cli'; protocol='openid-connect'; enabled=$true; publicClient=$true
            standardFlowEnabled=$true; directAccessGrantsEnabled=$false; implicitFlowEnabled=$false
            redirectUris=@('http://127.0.0.1:18087/callback'); attributes=@{'pkce.code.challenge.method'='S256'}
            defaultClientScopes=@(); optionalClientScopes=@()
        }
    )
    users=@(@{
        id='f51b26b6-1152-4e2b-b18a-7c46b94c1c02'; username='service-account-kafdeck-g1-service'
        enabled=$true; serviceAccountClientId='kafdeck-g1-service'
    })
}
$realm | ConvertTo-Json -Depth 15 | Set-Content -LiteralPath (Join-Path $importDir 'kafdeck-g1-realm.json') -Encoding utf8
$public = @{
    keycloakVersion='26.8.1'; javaVersion=$JavaVersion
    issuer='https://localhost:18443/realms/kafdeck-g1'; audience='kafdeck-api-g1'
    clientId='kafdeck-g1-service'; subject='f51b26b6-1152-4e2b-b18a-7c46b94c1c02'
    caPemFile=(Join-Path $labDir 'ca.pem'); clientPrivateKeyFile=(Join-Path $labDir 'client-private.key')
    keycloakHome=[IO.Path]::GetFullPath($KeycloakHome)
}
$public | ConvertTo-Json | Set-Content -LiteralPath (Join-Path $labDir 'lab.json') -Encoding utf8
$tlsCert.Dispose(); $tlsKey.Dispose(); $clientCert.Dispose(); $clientKey.Dispose()
Write-Output "Prepared isolated Keycloak 26.8.1 realm and ephemeral TLS/client keys under $labDir"
Write-Output 'Keys, TLS password and tokens must stay under ignored .generated; never commit them.'
