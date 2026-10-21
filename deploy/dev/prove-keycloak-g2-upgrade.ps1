#requires -Version 7.0
param(
    [ValidateSet('Prepare','Start','Upgrade','Stop','Status','Prove')][string]$Action='Prepare',
    [ValidateSet('all','node-a','node-b')][string]$Node='all',
    [string]$OutputDirectory="$PSScriptRoot/.generated/g2-complete/keycloak-upgrade",
    [string]$IdentityDirectory="$PSScriptRoot/.generated/g2/identity",
    [string]$PostgresManifest="$PSScriptRoot/.generated/g2/postgres/lab.json",
    [string]$OldArchive="$PSScriptRoot/.generated/g2-complete/runtime/keycloak-26.8.0.zip",
    [string]$NewArchive="$PSScriptRoot/.generated/g1/keycloak-segmented.zip",
    [string]$JavaHome="$PSScriptRoot/.generated/g1/jdk-21.0.12.1+1"
)
$ErrorActionPreference='Stop'
$scope=[IO.Path]::GetFullPath($OutputDirectory)
$manifest=Join-Path $scope 'upgrade-lab.json'
$pwsh=Join-Path $PSHOME $(if($IsWindows){'pwsh.exe'}else{'pwsh'})
$oldSha='7ed1de3fda2598369262613bf682aab7e233d80a38c405e91588f7a7454370a1'
$newSha='4cd312c889af358a819a74a6c6f068f9ea159a6959e49924607cec95cfd4c1a4'
$comparison=if($IsWindows){[StringComparison]::OrdinalIgnoreCase}else{[StringComparison]::Ordinal}
function Child([string]$name){
    $p=[IO.Path]::GetFullPath((Join-Path $scope $name))
    if(!$p.StartsWith($scope.TrimEnd([IO.Path]::DirectorySeparatorChar)+[IO.Path]::DirectorySeparatorChar,$comparison)){throw 'Owned upgrade path escaped its root.'}
    return $p
}
function Save([string]$path,$value){
    $temp=$path+'.tmp-'+[Guid]::NewGuid().ToString('N')
    [IO.File]::WriteAllText($temp,($value|ConvertTo-Json -Depth 12),[Text.UTF8Encoding]::new($false));[IO.File]::Move($temp,$path,$true)
}
function Private([string]$path,[string]$value){[IO.File]::WriteAllText($path,$value,[Text.UTF8Encoding]::new($false));if(!$IsWindows){& chmod 600 $path;if($LASTEXITCODE){throw 'Private upgrade file permission failed.'}}}
function PrivateLog([string]$path){
    $stream=[IO.FileStream]::new($path,[IO.FileMode]::Open,[IO.FileAccess]::Read,[IO.FileShare]::ReadWrite -bor [IO.FileShare]::Delete)
    if($stream.Length -gt 2MB){$stream.Dispose();throw 'Private upgrade log exceeded finite inspection budget.'}
    $reader=[IO.StreamReader]::new($stream)
    try{return $reader.ReadToEnd()}finally{$reader.Dispose();$stream.Dispose()}
}
$script:proofFailure='Unclassified';$script:controlExit=$null
function Diagnostic([string]$stage,[string]$state){
    $stages=@('OldNodes','OldMembership','SameVersionBounce','UpgradeA','MixedReplay','UpgradeB','NewReplay','Complete')
    if($stage -cnotin $stages -or $state -cnotin @('Running','Succeeded','Failed')){throw 'Closed upgrade diagnostic state changed.'}
    $observations=@()
    foreach($entry in $m.nodes){
        $text='';$log=Child ($entry.name+'-Start-'+$entry.version+'.log')
        try{if(Test-Path -LiteralPath $log){$text=PrivateLog $log}}catch{}
        $pidExists=$false;$pathMatches=$false;$startMatches=$false
        try{
            $saved=Get-Content -LiteralPath (Child ($entry.name+'-process.json')) -Raw|ConvertFrom-Json
            $recorded=Get-Process -Id $saved.processId -ErrorAction SilentlyContinue
            if($recorded){
                $pidExists=$true;$pathMatches=[string]::Equals($recorded.Path,$pwsh,$comparison)
                $time=if($saved.startedAtUtc -is [DateTime]){$saved.startedAtUtc.ToUniversalTime()}else{[DateTimeOffset]::Parse([string]$saved.startedAtUtc,[Globalization.CultureInfo]::InvariantCulture).UtcDateTime}
                $startMatches=($recorded.StartTime.ToUniversalTime().Ticks -eq $time.Ticks)
            }
        }catch{}
        # The review-only evidence publisher accepts *typed Boolean*
        # fields, not PowerShell's nullable/dynamic expression output. A
        # missing process/log remains false; the overall proof still fails.
        $observations+=@{node=$entry.name;version=$entry.version;
            launcherPresent=[bool]($null -ne (ExactProcess $entry));httpsPortOpen=[bool](PortOpen $entry.httpsPort);
            recordedPidExists=[bool]$pidExists;recordedPathMatches=[bool]$pathMatches;recordedStartMatches=[bool]$startMatches;
            startupBanner=[bool]($text -match 'Keycloak 26\.8\.[01] .*started');errorMarker=[bool]($text -match '\bERROR\b');
            twoMemberView=[bool]($text -match '\(2\)');nodeALabel=[bool]($text -match '\bnode-a\b');nodeBLabel=[bool]($text -match '\bnode-b\b');
            unknownOption=[bool]($text -match '(?i)unknown option|unrecognized option');
            realmFilenameMismatch=[bool]($text -match '(?is)realm name.*(match|JSON file)');
            bindConflict=[bool]($text -match '(?i)address already in use|failed to bind');
            databaseAuthenticationFailure=[bool]($text -match '(?i)password authentication failed');
            databaseConnectionFailure=[bool]($text -match '(?i)could not acquire connection|connection refused|failed to obtain JDBC');
            tlsMaterialFailure=[bool]($text -match '(?i)failed to load.*(key|certificate)|keystore.*not found')}
    }
    Save (Child 'keycloak-upgrade-control-diagnostic.json') @{formatVersion=1;action='Prove';stage=$stage;state=$state;
        failure=$script:proofFailure;controlExitCode=$script:controlExit;nodes=$observations}
}
function Refuse([string]$code,[string]$message){$script:proofFailure=$code;throw $message}
function TwoMembers([string]$text){
    foreach($view in [regex]::Matches($text,'\(2\)\s*\[([^\]\r\n]{1,180})\]')){
        # Attribute annotations can themselves contain commas. Parse the full
        # pair instead of splitting its nested annotations into extra members.
        $pair=[regex]::Match($view.Groups[1].Value,'^\s*(?<first>node-[ab])(?:\([^()\r\n]{1,64}\))?\s*,\s*(?<second>node-[ab])(?:\([^()\r\n]{1,64}\))?\s*$')
        if($pair.Success -and $pair.Groups['first'].Value -cne $pair.Groups['second'].Value){return $true}
    }
    return $false
}
function PortOpen([int]$port){
    $c=[Net.Sockets.TcpClient]::new();try{$c.ConnectAsync('127.0.0.1',$port).WaitAsync([TimeSpan]::FromSeconds(1)).GetAwaiter().GetResult();return $true}catch{return $false}finally{$c.Dispose()}
}
function VerifyPath([string]$recorded,[string]$relative){
    if(![string]::Equals([IO.Path]::GetFullPath($recorded),(Child $relative),$comparison)){throw 'Upgrade manifest contains an unowned path.'}
    $p=[IO.Path]::GetFullPath($recorded)
    while($p.Length -ge $scope.Length){if(Test-Path -LiteralPath $p){if((Get-Item -LiteralPath $p -Force).LinkType){throw 'Upgrade paths cannot traverse links.'}};$p=[IO.Path]::GetDirectoryName($p);if(!$p){break}}
}
function ExactProcess($entry){
    $file=Child ($entry.name+'-process.json');if(!(Test-Path -LiteralPath $file)){return $null}
    $saved=Get-Content -LiteralPath $file -Raw|ConvertFrom-Json
    $p=Get-Process -Id $saved.processId -ErrorAction SilentlyContinue
    $time=if($saved.startedAtUtc -is [DateTime]){$saved.startedAtUtc.ToUniversalTime()}else{[DateTimeOffset]::Parse([string]$saved.startedAtUtc,[Globalization.CultureInfo]::InvariantCulture).UtcDateTime}
    if($p -and $p.StartTime.ToUniversalTime().Ticks -eq $time.Ticks -and [string]::Equals($p.Path,$pwsh,$comparison)){return $p}
    return $null
}
function StopNode($entry){
    $p=ExactProcess $entry
    if($p){$p.Kill($true);if(!$p.WaitForExit(10000)){throw 'Owned upgrade launcher did not stop.'};$p.Dispose()}
    $until=[DateTimeOffset]::UtcNow.AddSeconds(10)
    while(PortOpen $entry.httpsPort){if([DateTimeOffset]::UtcNow -gt $until){throw 'Upgrade HTTPS port remains occupied; no unowned process is stopped.'};Start-Sleep -Milliseconds 100}
}
function Launch($entry,[string]$command='Start',[string]$version=$entry.version){
    $nodeRuntimeHome=Child ($entry.name+'/'+$version+'/keycloak-'+$version)
    VerifyPath $nodeRuntimeHome ($entry.name+'/'+$version+'/keycloak-'+$version)
    $launchArguments=@('-NoProfile','-NonInteractive','-File',(Child 'run-node.ps1'),'-Manifest',$manifest,'-Name',$entry.name,'-RuntimeHome',$nodeRuntimeHome,'-Command',$command)
    $launchArguments=$launchArguments|ForEach-Object{ '"'+$_.ToString().Replace('"','\"')+'"' }
    $launch=@{FilePath=$pwsh;ArgumentList=$launchArguments;RedirectStandardOutput=(Child ($entry.name+'-'+$command+'-'+$version+'.log'));RedirectStandardError=(Child ($entry.name+'-'+$command+'-'+$version+'-error.log'));PassThru=$true}
    if($IsWindows){$launch.WindowStyle='Hidden'}
    $p=Start-Process @launch
    if($command -eq 'Start'){
        Save (Child ($entry.name+'-process.json')) @{processId=$p.Id;startedAtUtc=$p.StartTime.ToUniversalTime().ToString('O');version=$version}
        $p.Dispose();return
    }
    # Check includes a cold build of the distinct target distribution. Bound the
    # complete control process separately from verified HTTPS readiness.
    try{if(!$p.WaitForExit(180000)){$p.Kill($true);Refuse 'CompatibilityDeadline' 'Upgrade compatibility command exceeded its deadline.'};$script:controlExit=$p.ExitCode;if($p.ExitCode -ne 0){Refuse 'CompatibilityRejected' ('Keycloak upgrade compatibility command refused; exit '+$p.ExitCode)}}finally{$p.Dispose()}
}
if($Action -eq 'Prepare'){
    if(Test-Path -LiteralPath $scope){throw 'A fresh upgrade output directory is required; no old data is replaced or deleted.'}
    foreach($p in @(20443,20444,7900,7910)){if(PortOpen $p){throw 'Owned upgrade ports must be free before preparation.'}}
    if((Get-FileHash -LiteralPath $OldArchive -Algorithm SHA256).Hash.ToLowerInvariant() -cne $oldSha -or
       (Get-FileHash -LiteralPath $NewArchive -Algorithm SHA256).Hash.ToLowerInvariant() -cne $newSha){throw 'Pinned Keycloak archive digest changed.'}
    $identity=Get-Content -LiteralPath (Join-Path $IdentityDirectory 'lab.json') -Raw|ConvertFrom-Json
    $pg=Get-Content -LiteralPath $PostgresManifest -Raw|ConvertFrom-Json
    if($pg.user -cnotmatch '^[A-Za-z_][A-Za-z0-9_]{0,62}$'){throw 'PostgreSQL owner identity is invalid.'}
    New-Item -ItemType Directory -Path $scope|Out-Null
    $database='kc_upgrade_'+[Guid]::NewGuid().ToString('N')
    $password=[Convert]::ToHexString([Security.Cryptography.RandomNumberGenerator]::GetBytes(32)).ToLowerInvariant()
    Private (Child 'database-password.txt') $password
    Private (Child 'provision.sql') "CREATE ROLE $database LOGIN PASSWORD '$password';`nCREATE DATABASE $database OWNER $database;`n"
    $connection=[Data.Common.DbConnectionStringBuilder]::new();$connection.set_ConnectionString([IO.File]::ReadAllText($pg.connectionStringFile).Trim())
    $previous=@{};foreach($name in @('PGPASSWORD','PGCONNECT_TIMEOUT','PGOPTIONS')){$previous[$name]=[Environment]::GetEnvironmentVariable($name)}
    try{
        $env:PGPASSWORD=[string]$connection['Password'];$env:PGCONNECT_TIMEOUT='5';$env:PGOPTIONS='-c statement_timeout=15000 -c lock_timeout=5000'
        & (Join-Path $pg.binDirectory $(if($IsWindows){'psql.exe'}else{'psql'})) -X -w -v ON_ERROR_STOP=1 -h $pg.bindAddress -p $pg.port -U $pg.user -d $pg.database -f (Child 'provision.sql') *> (Child 'provision.log')
        if($LASTEXITCODE){throw 'Fresh isolated Keycloak upgrade database provisioning failed; private diagnostics retained.'}
    }finally{foreach($name in $previous.Keys){[Environment]::SetEnvironmentVariable($name,$previous[$name])}}
    $nodes=@(@{name='node-a';httpsPort=20443;clusterPort=7900;version='26.8.0'},@{name='node-b';httpsPort=20444;clusterPort=7910;version='26.8.0'})
    foreach($entry in $nodes){foreach($version in @('26.8.0','26.8.1')){
        $destination=Child ($entry.name+'/'+$version);New-Item -ItemType Directory -Path $destination -Force|Out-Null
        [IO.Compression.ZipFile]::ExtractToDirectory($(if($version -eq '26.8.0'){$OldArchive}else{$NewArchive}),$destination)
        $import=Join-Path $destination ('keycloak-'+$version+'/data/import');New-Item -ItemType Directory -Path $import -Force|Out-Null
        Copy-Item -LiteralPath $identity.realmImportFile -Destination (Join-Path $import 'kafdeck-g2-realm.json')
    }}
    Save $manifest @{formatVersion=1;scope=$scope;oldVersion='26.8.0';newVersion='26.8.1';oldArchiveSha256=$oldSha;newArchiveSha256=$newSha;
        javaHome=[IO.Path]::GetFullPath($JavaHome);identityDirectory=[IO.Path]::GetFullPath($IdentityDirectory);
        jdbcUrl="jdbc:postgresql://$($pg.bindAddress):$($pg.port)/$database";database=$database;databaseUser=$database;databasePasswordFile=(Child 'database-password.txt');
        issuer='https://localhost:20443/realms/kafdeck-g2';clientId=$identity.clientId;subject=$identity.subject;audience=$identity.audience;
        caPemFile=$identity.caPemFile;clientPrivateKeyFile=$identity.clientPrivateKeyFile;cache='ispn';stack='jdbc-ping';clusterMtls=$true;nodes=$nodes}
    @'
param([string]$Manifest,[string]$Name,[string]$RuntimeHome,[ValidateSet('Start','Metadata','Check')][string]$Command)
$ErrorActionPreference='Stop'
$m=Get-Content -LiteralPath $Manifest -Raw|ConvertFrom-Json;$n=@($m.nodes|Where-Object name -eq $Name)[0]
$env:JAVA_HOME=$m.javaHome;$env:JAVA_OPTS_APPEND='-Xms128m -Xmx384m -XX:ActiveProcessorCount=2'
$env:KC_DB='postgres';$env:KC_DB_URL=$m.jdbcUrl;$env:KC_DB_USERNAME=$m.databaseUser;$env:KC_DB_PASSWORD=[IO.File]::ReadAllText($m.databasePasswordFile)
$env:KC_HTTP_ENABLED='false';$env:KC_HTTP_HOST='127.0.0.1';$env:KC_HTTPS_PORT=[string]$n.httpsPort;$env:KC_HOSTNAME='https://localhost:20443'
$env:KC_HTTPS_KEY_STORE_FILE=Join-Path $m.identityDirectory 'https.p12';$env:KC_HTTPS_KEY_STORE_PASSWORD=[IO.File]::ReadAllText((Join-Path $m.identityDirectory 'https-password.txt'))
$env:KC_CACHE='ispn';$env:KC_CACHE_STACK='jdbc-ping';$env:KC_CACHE_EMBEDDED_MTLS_ENABLED='true'
$env:KC_CACHE_EMBEDDED_NETWORK_BIND_ADDRESS='127.0.0.1';$env:KC_CACHE_EMBEDDED_NETWORK_BIND_PORT=[string]$n.clusterPort;$env:KC_CACHE_EMBEDDED_NODE_NAME=$Name
$env:KC_DB_POOL_INITIAL_SIZE='2';$env:KC_DB_POOL_MIN_SIZE='2';$env:KC_DB_POOL_MAX_SIZE='4'
$exe=Join-Path $RuntimeHome $(if($IsWindows){'bin/kc.bat'}else{'bin/kc.sh'})
if(!$IsWindows){& chmod +x $exe}
$buildMarker=Join-Path $RuntimeHome 'data/kafdeck-upgrade-built'
if($Command -ne 'Metadata' -and !(Test-Path -LiteralPath $buildMarker)){
    & $exe build --db=postgres;if($LASTEXITCODE){exit $LASTEXITCODE}
    [IO.File]::WriteAllText($buildMarker,'pinned postgres build completed')
}
if($Command -eq 'Start'){& $exe start --optimized --import-realm}
elseif($Command -eq 'Metadata'){& $exe update-compatibility metadata --optimized "--file=$(Join-Path $m.scope ($Name+'-compatibility-private.json'))"}
else{& $exe update-compatibility check --optimized "--file=$(Join-Path $m.scope ($Name+'-compatibility-private.json'))"}
exit $LASTEXITCODE
'@ |Set-Content -LiteralPath (Child 'run-node.ps1') -Encoding utf8
    Write-Output 'Prepared fresh pinned26.8.0/26.8.1 runtimes and isolated shared database; no Java started.'
    exit 0
}
if(!(Test-Path -LiteralPath $manifest)){throw 'Prepare a fresh owned upgrade fixture first.'}
$m=Get-Content -LiteralPath $manifest -Raw|ConvertFrom-Json
if(![string]::Equals([IO.Path]::GetFullPath($m.scope),$scope,$comparison) -or $m.oldArchiveSha256 -cne $oldSha -or $m.newArchiveSha256 -cne $newSha){throw 'Owned upgrade manifest identity changed.'}
VerifyPath $m.databasePasswordFile 'database-password.txt'
if(@($m.nodes).Count -ne 2 -or $m.nodes[0].name -cne 'node-a' -or $m.nodes[1].name -cne 'node-b' -or
    $m.nodes[0].httpsPort -ne 20443 -or $m.nodes[1].httpsPort -ne 20444 -or $m.nodes[0].clusterPort -ne 7900 -or $m.nodes[1].clusterPort -ne 7910){throw 'Closed upgrade node definitions changed.'}
if($Action -eq 'Start'){
    foreach($entry in $m.nodes){if($Node -ne 'all' -and $Node -ne $entry.name){continue};if(!(ExactProcess $entry)){if(PortOpen $entry.httpsPort){throw 'Upgrade port is occupied by an unowned process.'};Launch $entry}}
    Write-Output 'Started exact owned upgrade launchers; verified HTTPS readiness is separate.';exit 0
}
if($Action -eq 'Stop'){foreach($entry in $m.nodes){if($Node -eq 'all' -or $Node -eq $entry.name){StopNode $entry}};Write-Output 'Stopped exact upgrade process trees; databases and runtime data retained.';exit 0}
if($Action -eq 'Status'){foreach($entry in $m.nodes){Write-Output "$($entry.name) version=$($entry.version) launcher=$($null -ne (ExactProcess $entry)) portOpen=$(PortOpen $entry.httpsPort)"};exit 0}
if($Action -eq 'Upgrade'){
    if($Node -eq 'all'){throw 'Rolling upgrade must target exactly one node.'}
    $entry=@($m.nodes|Where-Object name -eq $Node)[0]
    if($entry.version -cne '26.8.0'){throw 'Only the approved old-to-new patch transition is admitted.'}
    Launch $entry 'Metadata' '26.8.0';Launch $entry 'Check' '26.8.1'
    StopNode $entry;$entry.version='26.8.1';Save $manifest $m;Launch $entry
    Write-Output 'Replaced one exact old node with pinned newer patch after compatibilityexit0; peer remains available.';exit 0
}

# Prove runs real OAuth on normal TLS. Credential bodies and access tokens are
# never console output, process arguments or safe evidence. Private files stay ignored.
if(@($m.nodes|Where-Object version -cne '26.8.0').Count){throw 'Proof requires a fresh old/old fixture, never a same-version substitute.'}
$ca=[Security.Cryptography.X509Certificates.X509Certificate2]::CreateFromPem([IO.File]::ReadAllText($m.caPemFile))
$handler=[Net.Http.SocketsHttpHandler]::new();$handler.AllowAutoRedirect=$false;$handler.UseProxy=$false;$handler.UseCookies=$false
$chain=[Security.Cryptography.X509Certificates.X509ChainPolicy]::new();$chain.TrustMode=[Security.Cryptography.X509Certificates.X509ChainTrustMode]::CustomRootTrust
$chain.RevocationMode=[Security.Cryptography.X509Certificates.X509RevocationMode]::NoCheck;$chain.CustomTrustStore.Add($ca)|Out-Null;$handler.SslOptions.CertificateChainPolicy=$chain
$http=[Net.Http.HttpClient]::new($handler);$http.Timeout=[TimeSpan]::FromSeconds(10)
$rsa=[Security.Cryptography.RSA]::Create();$rsa.ImportFromPem([IO.File]::ReadAllText($m.clientPrivateKeyFile))
function Encode([byte[]]$b){return [Convert]::ToBase64String($b).TrimEnd('=').Replace('+','-').Replace('/','_')}
function Assertion {
    $now=[DateTimeOffset]::UtcNow.ToUnixTimeSeconds();$expiry=$now+300
    $header=Encode ([Text.Encoding]::UTF8.GetBytes('{"alg":"RS256","typ":"JWT"}'))
    $payload=Encode ([Text.Encoding]::UTF8.GetBytes((@{iss=$m.clientId;sub=$m.clientId;aud=($m.issuer+'/protocol/openid-connect/token');iat=$now;nbf=$now;exp=$expiry;jti=[Guid]::NewGuid().ToString('N')}|ConvertTo-Json -Compress)))
    $signingInput=$header+'.'+$payload
    return @{value=($signingInput+'.'+(Encode ($rsa.SignData([Text.Encoding]::ASCII.GetBytes($signingInput),[Security.Cryptography.HashAlgorithmName]::SHA256,[Security.Cryptography.RSASignaturePadding]::Pkcs1))));expiry=$expiry}
}
function Exchange([int]$index,$assertion){
    if([DateTimeOffset]::UtcNow.ToUnixTimeSeconds() -ge $assertion.expiry-2){throw 'Replay assertion is no longer time-valid; proof refused.'}
    $data=[Collections.Generic.Dictionary[string,string]]::new();$data['grant_type']='client_credentials';$data['client_id']=$m.clientId
    $data['client_assertion_type']='urn:ietf:params:oauth:client-assertion-type:jwt-bearer';$data['client_assertion']=$assertion.value
    $body=[Net.Http.FormUrlEncodedContent]::new($data)
    try{
        $response=$http.PostAsync(('https://localhost:'+$m.nodes[$index].httpsPort+'/realms/kafdeck-g2/protocol/openid-connect/token'),$body).GetAwaiter().GetResult()
        try{
            if($response.Content.Headers.ContentLength -gt 65536){throw 'Token response exceeds finite budget.'}
            $text=$response.Content.ReadAsStringAsync().GetAwaiter().GetResult();if($text.Length -gt 65536){throw 'Token response exceeds finite budget.'}
            if([int]$response.StatusCode -eq 200){$json=$text|ConvertFrom-Json;if([string]::IsNullOrWhiteSpace($json.access_token)){throw 'Genuine token response omitted access token.'};return @{status=200;token=$json.access_token}}
            return @{status=[int]$response.StatusCode}
        }finally{$response.Dispose()}
    }finally{$body.Dispose()}
}
function Ready([int]$index){
    # The first old server performs a real cold Quarkus build under the bounded
    # two-CPU laboratory limit; its build is distinct from HTTPS readiness.
    $until=[DateTimeOffset]::UtcNow.AddMinutes(4)
    do{
        if(!(ExactProcess $m.nodes[$index])){Refuse 'NodeExited' 'Owned upgrade node exited before verified TLS readiness.'}
        try{
            $response=$http.GetAsync(('https://localhost:'+$m.nodes[$index].httpsPort+'/realms/kafdeck-g2/.well-known/openid-configuration')).GetAwaiter().GetResult()
            try{if([int]$response.StatusCode -eq 200){$discovery=$response.Content.ReadAsStringAsync().GetAwaiter().GetResult()|ConvertFrom-Json;if($discovery.issuer -cne $m.issuer){throw 'Canonical issuer mismatch.'};return}}finally{$response.Dispose()}
        }catch{}
        Start-Sleep -Milliseconds 250
    }while([DateTimeOffset]::UtcNow -lt $until)
    Refuse 'ReadinessDeadline' 'Owned upgrade node failed normal verified TLS discovery readiness.'
}
function ConsumedOnBoth($assertion){foreach($index in @(0,1)){if((Exchange $index $assertion).status -ne 400){Refuse 'ReplayAccepted' 'Consumed time-valid assertion replay was accepted across node transition.'}}}
function Fresh([int]$index){$a=Assertion;$r=Exchange $index $a;if($r.status -ne 200){Refuse 'FreshOAuthRejected' 'Fresh genuine client assertion failed.'};return @{assertion=$a;token=$r.token}}
function PairedFirstUse {
    $a=Assertion;$tasks=@();$contents=@();$success=0;$denied=0;$winner=$null
    try{
        foreach($index in @(0,1)){
            $form=[Collections.Generic.Dictionary[string,string]]::new();$form['grant_type']='client_credentials';$form['client_id']=$m.clientId
            $form['client_assertion_type']='urn:ietf:params:oauth:client-assertion-type:jwt-bearer';$form['client_assertion']=$a.value
            $content=[Net.Http.FormUrlEncodedContent]::new($form);$contents+=,$content
            $tasks+=,$http.PostAsync(('https://localhost:'+$m.nodes[$index].httpsPort+'/realms/kafdeck-g2/protocol/openid-connect/token'),$content)
        }
        # Both actual wire requests are issued before waiting for either response.
        foreach($task in $tasks){$response=$task.GetAwaiter().GetResult();try{
            if([int]$response.StatusCode -eq 200){$success++;$body=$response.Content.ReadAsStringAsync().GetAwaiter().GetResult();if($body.Length -gt 65536){throw 'Token response exceeds finite budget.'};$winner=($body|ConvertFrom-Json).access_token}
            elseif([int]$response.StatusCode -eq 400){$denied++}else{throw 'Paired first-use assertion had an unexpected OAuth status.'}
        }finally{$response.Dispose()}}
        if($success -ne 1 -or $denied -ne 1 -or [string]::IsNullOrWhiteSpace($winner)){throw 'Paired fresh assertion did not have exactly one winner.'}
        ConsumedOnBoth $a
        return $winner
    }finally{foreach($content in $contents){$content.Dispose()}}
}
function Membership {
    $until=[DateTimeOffset]::UtcNow.AddSeconds(30)
    do{
        $joined=$true
        foreach($entry in $m.nodes){
            $log=Child ($entry.name+'-Start-'+$entry.version+'.log');$text=if(Test-Path -LiteralPath $log){PrivateLog $log}else{''}
            # JGroups annotates logical names with bounded address attributes,
            # e.g. node-a(...). Require both exact configured logical names.
            if(!(TwoMembers $text)){$joined=$false}
        }
        if($joined){return};Start-Sleep -Milliseconds 200
    }while([DateTimeOffset]::UtcNow -lt $until)
    Refuse 'MembershipMissing' 'Actual two-node distributed cache membership was not observed.'
}
$proofStage='OldNodes'
try{
    Diagnostic $proofStage 'Running'
    foreach($index in @(0,1)){& $PSCommandPath -Action Start -Node $m.nodes[$index].name -OutputDirectory $scope|Out-Null;Ready $index}
    $proofStage='OldMembership';Diagnostic $proofStage 'Running';Membership
    $baseline=Fresh 0;ConsumedOnBoth $baseline.assertion
    $proofStage='SameVersionBounce';Diagnostic $proofStage 'Running';StopNode $m.nodes[0];$continuity=Fresh 1;Launch $m.nodes[0];Ready 0;Membership;ConsumedOnBoth $baseline.assertion
    foreach($entry in $m.nodes){$text=PrivateLog (Child ($entry.name+'-Start-26.8.0.log'));if($text -notmatch 'Keycloak 26\.8\.0 .*started'){throw 'Actual old server version startup evidence is missing.'}}
    Write-Output 'UPGRADE_PROOF oldOld=true; sameVersionBounceReplayRejected=true'
    $proofStage='UpgradeA';Diagnostic $proofStage 'Running';$beforeA=Fresh 0
    & $PSCommandPath -Action Upgrade -Node node-a -OutputDirectory $scope|Out-Null
    $gapA=Fresh 1;Ready 0;$m=Get-Content -LiteralPath $manifest -Raw|ConvertFrom-Json;Membership;ConsumedOnBoth $beforeA.assertion
    $proofStage='MixedReplay';Diagnostic $proofStage 'Running';$mixed=Fresh 0;ConsumedOnBoth $mixed.assertion
    $mixedWinner=PairedFirstUse
    Write-Output 'UPGRADE_PROOF mixedVersions=26.8.1,26.8.0; compatibilityExit=0; replayRejected=true; livePeerIssuedToken=true'
    $proofStage='UpgradeB';Diagnostic $proofStage 'Running';$beforeB=Fresh 1
    & $PSCommandPath -Action Upgrade -Node node-b -OutputDirectory $scope|Out-Null
    $gapB=Fresh 0;Ready 1;$m=Get-Content -LiteralPath $manifest -Raw|ConvertFrom-Json;Membership;ConsumedOnBoth $beforeB.assertion
    $proofStage='NewReplay';Diagnostic $proofStage 'Running';$final=Fresh 1;ConsumedOnBoth $final.assertion
    $finalWinner=PairedFirstUse
    foreach($entry in $m.nodes){$text=PrivateLog (Child ($entry.name+'-Start-'+$entry.version+'.log'));if($text -notmatch 'Keycloak 26\.8\.1 .*started'){throw 'Actual new server version startup evidence is missing.'}}
    Private (Child 'final-access-token-private.txt') $finalWinner
    # Private bridge for the existing production-verifier replay suite. Include
    # its complete closed manifest contract rather than only OAuth fields.
    Save (Child 'production-verifier-lab-private.json') @{keycloakVersion='26.8.1';issuer=$m.issuer;audience=$m.audience;
        clientId=$m.clientId;subject=$m.subject;caPemFile=$m.caPemFile;clientPrivateKeyFile=$m.clientPrivateKeyFile;
        cache='ispn';stack='jdbc-ping';clusterMtls=$true;
        nodes=@(@{name='node-a';endpoint='https://localhost:20443/realms/kafdeck-g2'},@{name='node-b';endpoint='https://localhost:20444/realms/kafdeck-g2'})}
    Save (Child 'keycloak-patch-rolling-upgrade-evidence.json') @{observedAtUtc=[DateTimeOffset]::UtcNow.ToString('O');oldVersion='26.8.0';newVersion='26.8.1';
        oldArchiveSha256=$oldSha;newArchiveSha256=$newSha;freshIsolatedDatabase=$true;normalTls=$true;cache='ispn';stack='jdbc-ping';clusterMtls=$true;
        actualTwoNodeMembership=$true;compatibilityCheckExitCodes=@(0,0);sameVersionBounceDistinctFromUpgrade=$true;
        mixedVersionPhaseObserved=$true;bothNewVersionsObserved=$true;freshTokenDuringEachSingleNodeReplacement=$true;
        timeValidConsumedReplayRejections=16;pairedFirstUseRounds=2;requestsPerRound=2;firstUseWinnersPerRound=@(1,1);
        requestScheduling='both node requests issued before awaiting either response';assertionLifetimeSeconds=300;rawCredentialsInEvidence=$false;
        automaticRollback=$false;operatorOrLoadBalancerTopologyClaimed=$false;productionVerifierChecked=$false}
    Write-Output 'UPGRADE_PROOF allNew=26.8.1,26.8.1; timeValidReplayRejected=true; actualPatchUpgrade=true; safeEvidenceWritten=true'
    Diagnostic 'Complete' 'Succeeded'
}catch{
    # Nested control actions have their own PowerShell script scope. Transfer
    # only our exact closed failure literals, never exception text, to evidence.
    $compatibilityExit=[regex]::Match($_.Exception.Message,'^Keycloak upgrade compatibility command refused; exit (-?\d{1,10})$')
    if($compatibilityExit.Success){$script:proofFailure='CompatibilityRejected';$script:controlExit=[int]::Parse($compatibilityExit.Groups[1].Value,[Globalization.CultureInfo]::InvariantCulture)}
    elseif($_.Exception.Message -ceq 'Upgrade compatibility command exceeded its deadline.'){$script:proofFailure='CompatibilityDeadline'}
    Diagnostic $proofStage 'Failed'
    Write-Output ('UPGRADE_PROOF failedStage='+$proofStage+'; exceptionType='+$_.Exception.GetType().Name)
    foreach($entry in $m.nodes){try{StopNode $entry}catch{}}
    throw 'Actual Keycloak patch upgrade proof refused; inspect retained private laboratory diagnostics.'
}finally{$rsa.Dispose();$http.Dispose();$handler.Dispose();$ca.Dispose()}
