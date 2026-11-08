param(
    [string]$Helm = 'helm',
    [string]$KubeVersion = '1.34.0'
)

$ErrorActionPreference = 'Stop'
$chart = Join-Path $PSScriptRoot 'kafdeck'
$fixture = @{
    image = @{digest = 'sha256:' + ('a' * 64); pullSecret = 'fixture-pull'}
    publicHost = 'kafdeck.example.test'
    operatorConfigMap = 'fixture-config'
    credentialsSecret = 'fixture-credentials'
    tlsSecret = 'fixture-tls'
    oidc = @{issuer = 'https://idp.example.test/realms/kafdeck'; clientId = 'fixture'}
    kafka = @{
        clusterId = 'fixture'
        bootstrapServers = @('broker-a.example.test:9093', 'broker-b.example.test:9093')
        protocol = 'Ssl'
    }
    persistence = @{provider = 'Sqlite'; existingClaim = 'fixture-kept-state'}
    network = @{
        ingressNamespaces = @('ingress-fixture')
        egress = @(@{cidr = '192.0.2.10/32'; port = 443}, @{cidr = '192.0.2.11/32'; port = 9093})
    }
}
$fixtureFile = [IO.Path]::GetTempFileName()
try {
    $fixture | ConvertTo-Json -Depth 8 | Set-Content -LiteralPath $fixtureFile -Encoding utf8NoBOM
    $helmArgs = @('--kube-version', $KubeVersion, '--values', $fixtureFile)
    & $Helm lint $chart --strict @helmArgs
    if ($LASTEXITCODE -ne 0) { throw 'Helm lint failed.' }

    $sqlite = (& $Helm template w81-fixture $chart --namespace w81-fixture @helmArgs) -join "`n"
    if ($LASTEXITCODE -ne 0) { throw 'SQLite rendering failed.' }
    foreach ($required in @(
        'kind: Deployment', 'kind: NetworkPolicy', 'type: Recreate',
        'allowPrivilegeEscalation: false', 'readOnlyRootFilesystem: true',
        'automountServiceAccountToken: false', 'runAsNonRoot: true',
        'seccompProfile:', 'scheme: HTTPS', 'key: tls-password',
        'claimName: "fixture-kept-state"', '/var/lib/kafdeck/mutations.sqlite3',
        'ghcr.io/araditc/kafdeck@sha256:' + ('a' * 64)
    )) {
        if (!$sqlite.Contains($required)) { throw "Required manifest entry missing: $required" }
    }
    if ($sqlite -match '(?m)^kind: (PersistentVolumeClaim|Secret)$') {
        throw 'An operator-owned PVC or Secret was rendered as a chart resource.'
    }

    $postgres = (& $Helm template w81-fixture $chart --namespace w81-fixture @helmArgs `
        --set 'persistence.provider=PostgreSql') -join "`n"
    if ($LASTEXITCODE -ne 0 -or !$postgres.Contains('file:/run/kafdeck-credentials/postgres-connection') -or
        $postgres.Contains('mutations.sqlite3')) {
        throw 'PostgreSQL rendering did not use the external connection reference.'
    }

    $rejected = @(
        @{ setting = 'replicas=2'; diagnostic = 'replicas' },
        @{ setting = 'image.digest=latest'; diagnostic = '/image/digest' },
        @{ setting = 'image.repository=ghcr.io/araditc/kafdeck:latest'; diagnostic = '/image/repository' },
        @{ setting = 'tlsSecret='; diagnostic = 'tlsSecret' },
        @{ setting = 'credentialsSecret='; diagnostic = 'credentialsSecret' },
        @{ setting = 'operatorConfigMap='; diagnostic = 'operatorConfigMap' },
        @{ setting = 'oidc.issuer=http://idp.example.test'; diagnostic = '/oidc/issuer' },
        @{ setting = 'oidc.clientSecret=fixture-inline'; diagnostic = 'clientSecret' },
        @{ setting = 'kafka.protocol=Plaintext'; diagnostic = '/kafka/protocol' },
        @{ setting = 'kafka.bootstrapServers[0]=broker.example.test:99999'; diagnostic = 'port exceeds 65535' },
        @{ setting = 'securityContext.privileged=true'; diagnostic = 'securityContext' },
        @{ setting = 'network.egress[0].cidr=0.0.0.0/0,network.egress[0].port=443'; diagnostic = 'cidr' }
    )
    foreach ($case in $rejected) {
        $errors = (& $Helm template w81-fixture $chart @helmArgs --set $case.setting 2>&1) -join "`n"
        if ($LASTEXITCODE -eq 0 -or !$errors.Contains($case.diagnostic)) {
            throw "Expected Helm to reject $($case.setting). Received: $errors"
        }
    }
    "Helm lint passed, SQLite and PostgreSQL rendered, and $($rejected.Count) invalid cases were rejected."
} finally {
    Remove-Item -LiteralPath $fixtureFile
}
