param(
    [Parameter(Mandatory=$true)][string]$KeycloakHome,
    [string]$OutputDirectory = "$PSScriptRoot/.generated/g2/identity",
    [string]$JavaVersion = 'Temurin 21.0.12.1+1',
    [ValidateRange(1024,65535)][int]$HttpsPort = 18444,
    [switch]$IncludeLifecycleControl,
    [switch]$IncludeBrowserSignIn
)
$ErrorActionPreference = 'Stop'
$labDir = [IO.Path]::GetFullPath($OutputDirectory)
$serverHome = [IO.Path]::GetFullPath($KeycloakHome)
if (!(Test-Path -LiteralPath (Join-Path $serverHome 'bin/kc.bat')) -and
    !(Test-Path -LiteralPath (Join-Path $serverHome 'bin/kc.sh'))) {
    throw 'KeycloakHome must contain the previously verified Keycloak 26.8.1 distribution.'
}
# Reimport with IGNORE_EXISTING would silently preserve old client keys/passwords.
# A fresh database and identity directory are required for every preparation.
if (Test-Path -LiteralPath $labDir) {
    throw 'OutputDirectory already exists. Select a fresh ignored directory for a new isolated laboratory.'
}
New-Item -ItemType Directory -Path $labDir -Force | Out-Null
$importDir = Join-Path $serverHome 'data/import'
New-Item -ItemType Directory -Path $importDir -Force | Out-Null
$tlsKey = [Security.Cryptography.RSA]::Create(2048)
$clientKey = [Security.Cryptography.RSA]::Create(2048)
$controlKey = [Security.Cryptography.RSA]::Create(2048)
$tlsCert = $null
$clientCert = $null
$controlCert = $null
try {
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
    $clientRequest = [Security.Cryptography.X509Certificates.CertificateRequest]::new('CN=kafdeck-g2-service', $clientKey,
        [Security.Cryptography.HashAlgorithmName]::SHA256,[Security.Cryptography.RSASignaturePadding]::Pkcs1)
    $clientCert = $clientRequest.CreateSelfSigned([DateTimeOffset]::UtcNow.AddMinutes(-5),[DateTimeOffset]::UtcNow.AddDays(2))
    [IO.File]::WriteAllText((Join-Path $labDir 'client-private.key'),$clientKey.ExportPkcs8PrivateKeyPem())
    $humanPassword = [Convert]::ToHexString([Security.Cryptography.RandomNumberGenerator]::GetBytes(24))
    [IO.File]::WriteAllText((Join-Path $labDir 'human-password.txt'),$humanPassword)
    $approverPassword = [Convert]::ToHexString([Security.Cryptography.RandomNumberGenerator]::GetBytes(24))
    [IO.File]::WriteAllText((Join-Path $labDir 'approver-password.txt'),$approverPassword)
    $audienceMapper = @{
        name='api-audience'; protocol='openid-connect'; protocolMapper='oidc-audience-mapper'
        consentRequired=$false; config=@{
            'included.custom.audience'='kafdeck-api-g2'; 'access.token.claim'='true'; 'id.token.claim'='false'
        }
    }
    $subjectMapper = @{
        name='exact-user-subject'; protocol='openid-connect'; protocolMapper='oidc-sub-mapper'
        consentRequired=$false; config=@{
            'access.token.claim'='true'; 'id.token.claim'='true'; 'introspection.token.claim'='true'
        }
    }
    $realm = @{
        realm='kafdeck-g2'; enabled=$true; sslRequired='all'; accessTokenLifespan=120
        registrationAllowed=$false; resetPasswordAllowed=$false; rememberMe=$false
        defaultSignatureAlgorithm='RS256'; defaultDefaultClientScopes=@(); defaultOptionalClientScopes=@()
        clients=@(
            @{
                id='f51b26b6-1152-4e2b-b18a-7c46b94c2c01'; clientId='kafdeck-g2-service'; protocol='openid-connect'
                enabled=$true; publicClient=$false; serviceAccountsEnabled=$true
                standardFlowEnabled=$false; directAccessGrantsEnabled=$false; implicitFlowEnabled=$false
                clientAuthenticatorType='client-jwt'; fullScopeAllowed=$false
                defaultClientScopes=@(); optionalClientScopes=@()
                attributes=@{
                    'jwt.credential.certificate'=[Convert]::ToBase64String($clientCert.RawData)
                    'token.endpoint.auth.signing.alg'='RS256'; 'client.session.max.lifespan'='120'
                }
                protocolMappers=@($audienceMapper, $subjectMapper)
            },
            @{
                clientId='kafdeck-g2-cli'; protocol='openid-connect'; enabled=$true; publicClient=$true
                standardFlowEnabled=$true; directAccessGrantsEnabled=$false; implicitFlowEnabled=$false
                serviceAccountsEnabled=$false; fullScopeAllowed=$false
                # Keycloak's native loopback matching ignores only the port.
                # The client independently binds/validates the exact numeric address/path/port.
                redirectUris=@('http://127.0.0.1/oauth/callback')
                attributes=@{'pkce.code.challenge.method'='S256'; 'access.token.lifespan'='120'}
                defaultClientScopes=@(); optionalClientScopes=@(); protocolMappers=@($audienceMapper, $subjectMapper)
            }
        )
        users=@(
            @{
                id='f51b26b6-1152-4e2b-b18a-7c46b94c2c02'; username='service-account-kafdeck-g2-service'
                enabled=$true; serviceAccountClientId='kafdeck-g2-service'
            },
            @{
                id='f51b26b6-1152-4e2b-b18a-7c46b94c2c03'; username='kafdeck-g2-operator'
                enabled=$true; emailVerified=$true; firstName='Disposable'; lastName='Operator'
                email='operator@example.invalid'; requiredActions=@()
                credentials=@(@{type='password'; value=$humanPassword; temporary=$false})
            },
            @{
                id='f51b26b6-1152-4e2b-b18a-7c46b94c2c04'; username='kafdeck-g2-approver'
                enabled=$true; emailVerified=$true; firstName='Disposable'; lastName='Approver'
                email='approver@example.invalid'; requiredActions=@()
                credentials=@(@{type='password'; value=$approverPassword; temporary=$false})
            }
        )
    }
    $realmImport = Join-Path $labDir 'realm-import.json'
    if ($IncludeBrowserSignIn) {
        $realm.clients += @{
            clientId='kafdeck-g2-browser';protocol='openid-connect';enabled=$true;publicClient=$true
            standardFlowEnabled=$true;directAccessGrantsEnabled=$false;implicitFlowEnabled=$false;serviceAccountsEnabled=$false
            fullScopeAllowed=$false;redirectUris=@('http://127.0.0.1/signin-oidc');webOrigins=@()
            attributes=@{'pkce.code.challenge.method'='S256';'access.token.lifespan'='120'}
            defaultClientScopes=@();optionalClientScopes=@();protocolMappers=@($subjectMapper)
        }
    }
    if ($IncludeLifecycleControl) {
        # Disposable realm administration exists only in this explicit lab.
        # It is never registered as a Kafdeck application principal. Tests use
        # genuine signed assertions; no admin password grant or token is saved.
        $controlRequest = [Security.Cryptography.X509Certificates.CertificateRequest]::new('CN=kafdeck-g2-lab-control', $controlKey,
            [Security.Cryptography.HashAlgorithmName]::SHA256,[Security.Cryptography.RSASignaturePadding]::Pkcs1)
        $controlCert = $controlRequest.CreateSelfSigned([DateTimeOffset]::UtcNow.AddMinutes(-5),[DateTimeOffset]::UtcNow.AddDays(2))
        [IO.File]::WriteAllText((Join-Path $labDir 'control-private.key'), $controlKey.ExportPkcs8PrivateKeyPem())
        $realm.clients += @{
            id='f51b26b6-1152-4e2b-b18a-7c46b94c2c05';clientId='kafdeck-g2-lab-control';protocol='openid-connect'
            enabled=$true;publicClient=$false;serviceAccountsEnabled=$true;standardFlowEnabled=$false
            directAccessGrantsEnabled=$false;implicitFlowEnabled=$false;clientAuthenticatorType='client-jwt';fullScopeAllowed=$true
            defaultClientScopes=@('roles');optionalClientScopes=@()
            attributes=@{'jwt.credential.certificate'=[Convert]::ToBase64String($controlCert.RawData);'token.endpoint.auth.signing.alg'='RS256'}
            protocolMappers=@($subjectMapper)
        }
        $realm.users += @{
            id='f51b26b6-1152-4e2b-b18a-7c46b94c2c06';username='service-account-kafdeck-g2-lab-control'
            enabled=$true;serviceAccountClientId='kafdeck-g2-lab-control'
            clientRoles=@{'realm-management'=@('manage-clients','manage-realm','manage-users','view-realm','view-clients','view-users')}
        }
    }
    $realm | ConvertTo-Json -Depth 15 | Set-Content -LiteralPath $realmImport -Encoding utf8
    Copy-Item -LiteralPath $realmImport -Destination (Join-Path $importDir 'kafdeck-g2-realm.json')
    $public = @{
        keycloakVersion='26.8.1'; javaVersion=$JavaVersion
        issuer="https://localhost:$HttpsPort/realms/kafdeck-g2"; audience='kafdeck-api-g2'
        clientId='kafdeck-g2-service'; subject='f51b26b6-1152-4e2b-b18a-7c46b94c2c02'
        cliClientId='kafdeck-g2-cli'; humanSubject='f51b26b6-1152-4e2b-b18a-7c46b94c2c03'
        humanUsername='kafdeck-g2-operator'; humanPasswordFile=(Join-Path $labDir 'human-password.txt')
        approverSubject='f51b26b6-1152-4e2b-b18a-7c46b94c2c04'
        approverUsername='kafdeck-g2-approver'; approverPasswordFile=(Join-Path $labDir 'approver-password.txt')
        caPemFile=(Join-Path $labDir 'ca.pem'); clientPrivateKeyFile=(Join-Path $labDir 'client-private.key')
        keycloakHome=$serverHome
        realmImportFile=$realmImport
        databaseUrl=('jdbc:h2:file:' + (Join-Path $labDir 'keycloakdb').Replace('\','/') + ';NON_KEYWORDS=VALUE')
    }
    if ($IncludeLifecycleControl) {
        $public.controlClientId='kafdeck-g2-lab-control'
        $public.controlPrivateKeyFile=(Join-Path $labDir 'control-private.key')
    }
    if ($IncludeBrowserSignIn) { $public.browserClientId='kafdeck-g2-browser' }
    $public | ConvertTo-Json | Set-Content -LiteralPath (Join-Path $labDir 'lab.json') -Encoding utf8
} finally {
    if ($null -ne $tlsCert) { $tlsCert.Dispose() }
    if ($null -ne $clientCert) { $clientCert.Dispose() }
    if ($null -ne $controlCert) { $controlCert.Dispose() }
    $tlsKey.Dispose(); $clientKey.Dispose(); $controlKey.Dispose()
}
Write-Output "Prepared isolated G2 realm, human authorization-code client and asymmetric service client under $labDir"
Write-Output 'Keys, password, database, authorization codes and tokens must remain under ignored .generated; never commit them.'
