#requires -Version 7.4
param(
    [Parameter(Mandatory)][ValidateSet('win-x64','linux-x64','linux-arm64','osx-x64','osx-arm64')][string]$Rid
)

$ErrorActionPreference = 'Stop'
$root = (Resolve-Path "$PSScriptRoot/../..").Path
$source = (& git -C $root rev-parse HEAD).Trim()
$platform = if ($IsWindows) { 'win' } elseif ($IsMacOS) { 'osx' } else { 'linux' }
$architecture = [Runtime.InteropServices.RuntimeInformation]::ProcessArchitecture.ToString().ToLowerInvariant()
if ($Rid -ne "$platform-$architecture") { throw 'Runner does not match the native package RID.' }
$private = Join-Path $env:RUNNER_TEMP "kafdeck-native-$([Guid]::NewGuid().ToString('N'))"
$evidence = Join-Path $root "artifacts/native/$Rid"
New-Item -ItemType Directory -Force -Path $private,$evidence | Out-Null
$owned = [Collections.Generic.List[Diagnostics.Process]]::new()
$suffix = if ($IsWindows) { '.exe' } else { '' }

function Native-Ok([string]$Phase) {
    if ($LASTEXITCODE) { throw "Native acceptance failed at $Phase." }
}
function Port {
    $listener = [Net.Sockets.TcpListener]::new([Net.IPAddress]::Loopback,0)
    $listener.Start()
    try { return $listener.LocalEndpoint.Port } finally { $listener.Stop() }
}
function Launch([string]$File,[string[]]$Arguments,[string]$Directory,[hashtable]$Environment=@{}) {
    $start = [Diagnostics.ProcessStartInfo]::new($File)
    $start.WorkingDirectory = $Directory
    $start.UseShellExecute = $false
    $start.CreateNoWindow = $true
    $start.RedirectStandardInput = $true
    $start.RedirectStandardOutput = $true
    $start.RedirectStandardError = $true
    foreach ($argument in $Arguments) { $start.ArgumentList.Add($argument) }
    foreach ($key in $Environment.Keys) { $start.Environment[$key] = $Environment[$key] }
    $process = [Diagnostics.Process]::Start($start)
    $owned.Add($process)
    return $process
}
function Request-Rpc([Diagnostics.Process]$Process,[hashtable]$Request) {
    $Process.StandardInput.WriteLine(($Request | ConvertTo-Json -Depth 12 -Compress))
    $Process.StandardInput.Flush()
    $line = $Process.StandardOutput.ReadLineAsync().WaitAsync([TimeSpan]::FromSeconds(45)).GetAwaiter().GetResult()
    if (!$line -or $line.Length -gt 262144) { throw 'Native MCP returned no bounded protocol response.' }
    $reply = $line | ConvertFrom-Json
    if ($reply.id -ne $Request.id -or $reply.error) { throw 'Native MCP protocol response failed.' }
    return $reply
}

try {
    $javaHomes = @(Get-ChildItem Env:JAVA_HOME_21_* | Where-Object { Test-Path (Join-Path $_.Value "bin/java$suffix") })
    if (!$javaHomes.Count) { throw 'Java 21 is required for the isolated identity fixture.' }
    $env:JAVA_HOME = $javaHomes[0].Value
    $java = Join-Path $env:JAVA_HOME "bin/java$suffix"
    $javaVersion = (& $java -version 2>&1 | Select-Object -First 1).ToString()
    if ($javaVersion -notmatch '"21\.') { throw 'Expected Java 21.' }

    $components = [ordered]@{}
    $installed = @{}
    foreach ($component in @(
        @{id='api-ui';project='src/backend/Kafdeck.Api/Kafdeck.Api.csproj';exe="Kafdeck.Api$suffix"},
        @{id='cli';project='src/cli/Kafdeck.Cli/Kafdeck.Cli.csproj';exe="kafdeck$suffix"},
        @{id='mcp';project='src/mcp/Kafdeck.Mcp/Kafdeck.Mcp.csproj';exe="kafdeck-mcp$suffix"}
    )) {
        $publish = Join-Path $private "publish/$($component.id)"
        & dotnet publish (Join-Path $root $component.project) -c Release -r $Rid --self-contained true "-p:KafdeckNativeRid=$Rid" -p:PackAsTool=false -p:UseAppHost=true -p:RestoreLockedMode=true -o $publish
        Native-Ok "publish-$($component.id)"
        if ($component.id -eq 'api-ui') {
            Copy-Item -LiteralPath (Join-Path $root 'src/frontend/dist') -Destination (Join-Path $publish 'wwwroot') -Recurse
        }
        $archiveName = "kafdeck-$($component.id)-$Rid.tar.gz"
        $archive = Join-Path $evidence $archiveName
        & tar -czf $archive -C $publish .
        Native-Ok "archive-$($component.id)"
        $destination = Join-Path $private "installed/$($component.id)"
        New-Item -ItemType Directory -Force $destination | Out-Null
        & tar -xzf $archive -C $destination
        Native-Ok "install-$($component.id)"
        $executable = Join-Path $destination $component.exe
        if (!(Test-Path -LiteralPath $executable -PathType Leaf)) { throw 'Native apphost is missing.' }
        $installed[$component.id] = $executable
        $components[$component.id] = @{archive=$archiveName;sha256=(Get-FileHash $archive).Hash.ToLowerInvariant();started=$false;authenticatedOperation=$false}
    }

    $zip = Join-Path $private 'keycloak.zip'
    Invoke-WebRequest -Uri 'https://github.com/keycloak/keycloak/releases/download/26.8.1/keycloak-26.8.1.zip' -OutFile $zip -TimeoutSec 180
    if ((Get-FileHash $zip).Hash.ToLowerInvariant() -ne '4cd312c889af358a819a74a6c6f068f9ea159a6959e49924607cec95cfd4c1a4') { throw 'Keycloak archive identity mismatch.' }
    Expand-Archive -LiteralPath $zip -DestinationPath $private
    $keycloak = Join-Path $private 'keycloak-26.8.1'
    $identity = Join-Path $private 'identity'
    $identityPort = Port
    & (Join-Path $root 'deploy/dev/prepare-keycloak-g2.ps1') -KeycloakHome $keycloak -OutputDirectory $identity -HttpsPort $identityPort -JavaVersion $javaVersion
    $lab = Get-Content (Join-Path $identity 'lab.json') -Raw | ConvertFrom-Json
    $kc = Join-Path $keycloak ('bin/kc' + $(if ($IsWindows) { '.bat' } else { '.sh' }))
    if (!$IsWindows) { & chmod +x $kc; Native-Ok 'keycloak-permission' }
    $arguments = @('start','--http-host=127.0.0.1','--http-enabled=false','--db=dev-file',
        "--https-port=$identityPort","--hostname=https://localhost:$identityPort",
        "--https-key-store-file=$(Join-Path $identity 'https.p12')",'--cache=local','--import-realm')
    $file = $kc
    if ($IsWindows) { $file=$env:ComSpec; $arguments=@('/d','/c','call',$kc)+$arguments }
    $idp = Launch $file $arguments $private @{
        KC_HTTPS_KEY_STORE_PASSWORD=[IO.File]::ReadAllText((Join-Path $identity 'https-password.txt')).Trim()
        KC_DB_URL=$lab.databaseUrl
        JAVA_HOME=$env:JAVA_HOME
    }
    $idpOut = $idp.StandardOutput.ReadToEndAsync()
    $idpError = $idp.StandardError.ReadToEndAsync()
    $curl = if ($IsWindows) { (Get-Command curl.exe).Source } else { (Get-Command curl).Source }
    $identityReady=$false
    for ($attempt=0; $attempt -lt 120 -and !$idp.HasExited; $attempt++) {
        $discovery=& $curl --silent --fail --cacert $lab.caPemFile --connect-timeout 1 --max-time 2 "$($lab.issuer)/.well-known/openid-configuration"
        if ($LASTEXITCODE -eq 0 -and ($discovery | ConvertFrom-Json).issuer -eq $lab.issuer) { $identityReady=$true; break }
        Start-Sleep -Seconds 1
    }
    if (!$identityReady) { throw 'Pinned Keycloak HTTPS discovery failed.' }

    $apiPort = Port
    $origin = "http://127.0.0.1:$apiPort"
    $settings = @{
        'Kafdeck:Deployment:ListenUrls:0'=$origin
        'Kafdeck:Deployment:AccessMode'='Oidc'
        'Kafdeck:Deployment:Oidc:Issuer'=$lab.issuer
        'Kafdeck:Deployment:Oidc:ClientId'=$lab.cliClientId
        'Kafdeck:Deployment:Oidc:TrustedCaPemFile'=$lab.caPemFile
        'Kafdeck:NonBrowserRead:Enabled'='true'
        'Kafdeck:NonBrowserRead:Issuer'=$lab.issuer
        'Kafdeck:NonBrowserRead:ApiAudience'=$lab.audience
        'Kafdeck:NonBrowserRead:TrustedCaPemFile'=$lab.caPemFile
        'Kafdeck:NonBrowserRead:Registrations:0:Id'='native-service'
        'Kafdeck:NonBrowserRead:Registrations:0:OwnerId'='native-fixture'
        'Kafdeck:NonBrowserRead:Registrations:0:ClientId'=$lab.clientId
        'Kafdeck:NonBrowserRead:Registrations:0:Subject'=$lab.subject
        'Kafdeck:NonBrowserRead:Registrations:0:Enabled'='true'
        'Kafdeck:NonBrowserRead:Registrations:0:ExpiresAtUtc'=[DateTimeOffset]::UtcNow.AddMinutes(20).ToString('O')
        'Kafdeck:NonBrowserRead:Registrations:0:ReadActions:0'='SystemRead'
        'Kafdeck:Authorization:Roles:0:Id'='native-reader'
        'Kafdeck:Authorization:Roles:0:Permissions:0:Action'='SystemRead'
        'Kafdeck:Authorization:SubjectBindings:0:Issuer'=$lab.issuer
        'Kafdeck:Authorization:SubjectBindings:0:Subject'=$lab.subject
        'Kafdeck:Authorization:SubjectBindings:0:RoleIds:0'='native-reader'
        'Logging:LogLevel:Default'='Warning'
    }
    $settings | ConvertTo-Json | Set-Content (Join-Path $private 'appsettings.json') -Encoding utf8
    $profile = Join-Path $private 'service.json'
    @{issuer=$lab.issuer;audience=$lab.audience;clientId=$lab.clientId;expectedSubject=$lab.subject;
        privateKeyFile=$lab.clientPrivateKeyFile;trustedCaPemFile=$lab.caPemFile} | ConvertTo-Json | Set-Content $profile -Encoding utf8
    $webroot=Join-Path (Split-Path $installed['api-ui']) 'wwwroot'
    $api = Launch $installed['api-ui'] @('--contentRoot',$private,'--webroot',$webroot) $private
    $apiOut=$api.StandardOutput.ReadToEndAsync(); $apiError=$api.StandardError.ReadToEndAsync()
    $ready=$false
    for ($attempt=0; $attempt -lt 120 -and !$api.HasExited -and !$idp.HasExited; $attempt++) {
        try {
            if ((Invoke-RestMethod "$origin/healthz" -TimeoutSec 2).status -eq 'ok') { $ready=$true; break }
        } catch {}
        Start-Sleep -Seconds 1
    }
    if (!$ready) { throw 'Extracted native API failed startup.' }
    $index=Invoke-WebRequest "$origin/" -TimeoutSec 5
    if ($index.StatusCode -ne 200 -or $index.Content -notmatch '<html') { throw 'Packaged frontend is missing.' }
    $components['api-ui'].started=$true

    $client=Launch $installed.cli @('--url',$origin,'--service-profile',$profile,'views','v08-bearer-read-capabilities') $private
    $clientOut=$client.StandardOutput.ReadToEndAsync(); $clientError=$client.StandardError.ReadToEndAsync()
    if (!$client.WaitForExit(90000)) { throw 'Native CLI authentication timed out.' }
    if ($client.ExitCode -ne 0 -or !(($clientOut.GetAwaiter().GetResult() | ConvertFrom-Json).contracts.Count -gt 0)) { throw 'Native CLI authenticated read failed.' }
    $components.cli.started=$true; $components.cli.authenticatedOperation=$true; $components['api-ui'].authenticatedOperation=$true

    $mcp=Launch $installed.mcp @('--base-url',$origin,'--service-profile',$profile) $private
    $mcpError=$mcp.StandardError.ReadToEndAsync()
    $initialized=Request-Rpc $mcp @{jsonrpc='2.0';id=1;method='initialize';params=@{protocolVersion='2025-06-18';capabilities=@{};clientInfo=@{name='native-acceptance';version='0.9.0'}}}
    if ($initialized.result.serverInfo.version -ne '0.9.0') { throw 'Native MCP version mismatch.' }
    $mcp.StandardInput.WriteLine('{"jsonrpc":"2.0","method":"notifications/initialized"}')
    $mcp.StandardInput.Flush()
    $read=Request-Rpc $mcp @{jsonrpc='2.0';id=2;method='tools/call';params=@{name='read_v08_bearer_read_capabilities';arguments=@{}}}
    if ($read.result.isError -or $read.result.structuredContent.httpStatus -ne 200) { throw 'Native MCP authenticated read failed.' }
    $components.mcp.started=$true; $components.mcp.authenticatedOperation=$true
    @{schemaVersion=1;sourceSha=$source;version='0.9.0';rid=$Rid;components=$components;installedFromArchives=$true;
        identity='Keycloak 26.8.1 HTTPS private_key_jwt';runtime=[Runtime.InteropServices.RuntimeInformation]::OSDescription;
        scope='native archive install, API/frontend startup and service-authenticated reads; effect and restore acceptance is separate'} |
        ConvertTo-Json -Depth 10 | Set-Content (Join-Path $evidence 'native-evidence.json') -Encoding utf8
} finally {
    foreach ($process in $owned) {
        if (!$process.HasExited) { $process.Kill($true); $process.WaitForExit() }
        $process.Dispose()
    }
    $resolved=[IO.Path]::GetFullPath($private)
    $parent=[IO.Path]::GetFullPath($env:RUNNER_TEMP).TrimEnd([IO.Path]::DirectorySeparatorChar)+[IO.Path]::DirectorySeparatorChar
    if (!$resolved.StartsWith($parent,[StringComparison]::OrdinalIgnoreCase)) { throw 'Unexpected fixture cleanup target.' }
    Remove-Item -LiteralPath $resolved -Recurse -Force
}
