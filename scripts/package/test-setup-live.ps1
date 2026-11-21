#requires -Version 7.0
#requires -RunAsAdministrator
param([Parameter(Mandatory)][string]$Lab,[Parameter(Mandatory)][string]$PipeName)
$ErrorActionPreference='Stop'
$ProgressPreference='SilentlyContinue'
$fixture=Get-Content -LiteralPath (Join-Path $Lab 'fixture.json') -Raw|ConvertFrom-Json
if([IO.Path]::GetFullPath($Lab) -notmatch '^[A-Z]:\\KafdeckInstallerLab-[a-f0-9]{32}$' -or [Security.Principal.WindowsIdentity]::GetCurrent().User.Value -ne $fixture.sid){throw 'Invalid isolated acceptance identity.'}
$stage='bootstrap'
$proof=@{success=$false;stage=$stage;actualOsReboot=$false;nativeVersion='1.1.0';productionInstallationChanged=$false}
$password=$null
function Assert-True([bool]$Value,[string]$Message){if(!$Value){$script:assertion=$Message;throw 'Acceptance assertion failed.'}}
function Worker([object]$Request,[string]$Script=$script:worker){
    $info=[Diagnostics.ProcessStartInfo]::new((Join-Path $env:SystemRoot 'System32/WindowsPowerShell/v1.0/powershell.exe'))
    $info.UseShellExecute=$false;$info.CreateNoWindow=$true;$info.RedirectStandardInput=$true;$info.RedirectStandardOutput=$true;$info.RedirectStandardError=$true
    $info.StandardOutputEncoding=[Text.UTF8Encoding]::new($false)
    $info.Environment['PSModulePath']=[IO.Path]::Combine($env:SystemRoot,'System32\WindowsPowerShell\v1.0\Modules')
    foreach($arg in @('-NoLogo','-NoProfile','-NonInteractive','-ExecutionPolicy','Bypass','-File',$Script)){$info.ArgumentList.Add($arg)}
    $process=[Diagnostics.Process]::Start($info)
    try {
        $output=$process.StandardOutput.ReadToEndAsync();$errors=$process.StandardError.ReadToEndAsync()
        $process.StandardInput.WriteLine(($Request|ConvertTo-Json -Depth 8 -Compress));$process.StandardInput.Close()
        if(!$process.WaitForExit(1200000)){$process.Kill($true);throw 'Worker timeout.'}
        $text=$output.GetAwaiter().GetResult();$null=$errors.GetAwaiter().GetResult()
        Assert-True (!$text.Contains($script:password)) 'Windows password leaked into worker output.'
        $events=@($text -split '\r?\n'|Where-Object {$_ -match '^\{'}|ForEach-Object {$_|ConvertFrom-Json})
        $script:lastWorker=@{exit=$process.ExitCode;stage=$events[-1].stage;category=$events[-1].category;source=$events[-1].source;line=$events[-1].line}
        return @{exit=$process.ExitCode;event=$events[-1]}
    } finally {$process.Dispose()}
}
function Port {
    $listener=[Net.Sockets.TcpListener]::new([Net.IPAddress]::Loopback,0)
    try {$listener.Start();return $listener.LocalEndpoint.Port} finally {$listener.Stop()}
}
function Healthy {
    $null=Wait-OfflineUrl "$script:origin/healthz" 180
    $null=Wait-OfflineUrl "https://127.0.0.1:$script:idPort/realms/kafdeck/.well-known/openid-configuration" 600
}
function Owner-Count {
    $secret=Read-OfflineSecret (Join-Path $script:root 'secrets/postgres-admin.dpapi')
    $value=Invoke-OfflineNative -File (Join-Path $script:root 'runtime/postgresql/bin/psql.exe') -Arguments @('-X','--no-password','-h','127.0.0.1','-p',[string]$script:dbPort,'-U','kafdeck_pg_admin','-d','kafdeck_keycloak','-At','-c',"SELECT count(*) FROM user_entity WHERE realm_id=(SELECT id FROM realm WHERE name='kafdeck') AND email='setup-owner@example.invalid' AND email_verified=false") -Environment @{PGPASSWORD=$secret}
    return $value.Trim()
}
try {
    $pipe=[IO.Pipes.NamedPipeClientStream]::new('.',$PipeName,[IO.Pipes.PipeDirection]::In)
    try {$pipe.Connect(120000);$reader=[IO.StreamReader]::new($pipe);try {$password=$reader.ReadLine()}finally{$reader.Dispose()}}finally{$pipe.Dispose()}
    Assert-True ($password.Length -gt 24) 'Acceptance credential was not delivered.'
    Assert-True ([Environment]::Version.Major -ge 10) 'Acceptance loader requires a .NET 10 PowerShell host.'
    $stage='prepare-verified-cache';$proof.stage=$stage
    $candidate=[string]$fixture.candidate
    $build=Get-Content -LiteralPath (Join-Path $candidate 'build-evidence.json') -Raw|ConvertFrom-Json
    $exe=Join-Path $candidate 'publish/Kafdeck.Setup.exe'
    Assert-True ((Get-FileHash -LiteralPath $exe).Hash -eq $build.exeSha256) 'Candidate EXE differs from build evidence.'
    $dll=Get-ChildItem -LiteralPath (Join-Path $fixture.repo 'deploy/windows/gui/bin/Release') -Recurse -File -Filter Kafdeck.Setup.dll|Sort-Object Length -Descending|Select-Object -First 1
    $assembly=[Reflection.Assembly]::LoadFrom($dll.FullName)
    $module=$assembly.GetType('Kafdeck.Setup.SetupMedia',$true)
    $flags=[Reflection.BindingFlags]'NonPublic,Static'
    $cache=[string]$module.GetMethod('Prepare',$flags).Invoke($null,@([Action[string]]{param($value)}))
    # Invoke the production extraction module and resources without automating UAC/UI in Session 0.
    # Replace the loader's copied process image with the exact actual candidate maintenance EXE.
    Copy-Item -LiteralPath $exe -Destination (Join-Path $cache 'Kafdeck.Setup.exe') -Force
    Assert-True ((Get-FileHash -LiteralPath (Join-Path $cache 'Kafdeck.Setup.exe')).Hash -eq $build.exeSha256) 'Cached maintenance EXE differs.'
    $worker=Join-Path $cache 'Setup-Worker.ps1'
    $root=[string]$fixture.root
    $appPort=Port;$idPort=Port;$dbPort=Port
    Assert-True (@($appPort,$idPort,$dbPort|Select-Object -Unique).Count -eq 3) 'Acceptance port collision.'
    $origin="https://127.0.0.1:$appPort"
    $request=@{action='install';root=$root;address='127.0.0.1';kafkaHosts=@('127.0.0.1');clientNetworks=@('127.0.0.1');appPort=$appPort;identityPort=$idPort;databasePort=$dbPort;password=$password;accepted=$false;language='en'}
    $stage='reject-unaccepted-license';$proof.stage=$stage
    $result=Worker $request
    Assert-True ($result.exit -eq 1 -and $result.event.stage -eq 'failed' -and !(Test-Path -LiteralPath $root)) 'Unaccepted license had effects.'
    $request.accepted=$true
    $stage='prerequisite-reboot-resume';$proof.stage=$stage
    $wrapper=Join-Path $Lab 'Inject-Reboot.ps1'
    $vc=(Join-Path $cache 'media/media/vc_redist.x64.exe').Replace("'","''")
    $fixedWorker=$worker.Replace("'","''")
    $body="function global:Start-Process { param([string]`$FilePath,[string[]]`$ArgumentList,[string]`$WindowStyle,[switch]`$Wait,[switch]`$PassThru); if(`$FilePath -ne '$vc'){throw 'Unexpected injected process.'};[pscustomobject]@{ExitCode=3010} }; & '$fixedWorker'; exit `$LASTEXITCODE"
    [IO.File]::WriteAllText($wrapper,$body,[Text.UTF8Encoding]::new($true))
    $result=Worker $request $wrapper
    Assert-True ($result.exit -eq 3010 -and $result.event.stage -eq 'reboot') 'Prerequisite reboot was not surfaced.'
    $receiptPath=Join-Path $root 'config/offline-install.json'
    $receipt=Get-Content -LiteralPath $receiptPath -Raw|ConvertFrom-Json
    Assert-True ($receipt.phase -eq 'Restart required after Windows prerequisites') 'Reboot recovery phase was not retained.'
    $stage='real-install-after-resume';$proof.stage=$stage
    $result=Worker $request
    Assert-True ($result.exit -eq 0 -and $result.event.stage -eq 'complete' -and $result.event.firstAdmin) ('Real install failed: '+$result.event.category)
    Assert-True ([regex]::IsMatch([string]$result.event.code,'^[A-Za-z0-9_-]{32}$')) 'First administrator code is missing.'
    . (Join-Path $root 'scripts/Offline-Tools.ps1')
    Healthy
    $stage='create-real-first-administrator';$proof.stage=$stage
    $setup=Invoke-RestMethod -Uri "$origin/api/v1/setup" -SessionVariable browser -MaximumRedirection 0
    $ownerPassword='Ow!'+[guid]::NewGuid().ToString('N')+'Aa9'
    $owner=@{code=$result.event.code;email='setup-owner@example.invalid';password=$ownerPassword}|ConvertTo-Json
    $null=Invoke-WebRequest -Uri "$origin/api/v1/setup" -WebSession $browser -Method Post -MaximumRedirection 0 -Headers @{'X-Kafdeck-CSRF'=$setup.csrf} -ContentType 'application/json' -Body $owner
    Assert-True ((Invoke-RestMethod -Uri "$origin/api/v1/setup").state -eq 'complete' -and (Owner-Count) -eq '1') 'Real owner or no-email-verification state failed.'
    $owner=$null;$ownerPassword=$null;$result=$null
    $secretPath=Join-Path $root 'secrets/https-password.dpapi'
    $identityPath=Join-Path $root 'state/identity-state.json'
    $baselineSecret=(Get-FileHash -LiteralPath $secretPath).Hash
    $baselineIdentity=(Get-FileHash -LiteralPath $identityPath).Hash
    $stateFile=Join-Path $root 'state/acceptance-retained.txt'
    [IO.File]::WriteAllText($stateFile,'retained-acceptance-data')
    $stage='automatic-start-configuration-and-dependency-restart';$proof.stage=$stage
    foreach($name in @('Kafdeck','Kafdeck-Keycloak','Kafdeck-PostgreSQL')){
        Assert-True ((Get-CimInstance Win32_Service -Filter "Name='$name'").StartMode -eq 'Auto') 'Service is not automatic.'
        $controller=[ServiceProcess.ServiceController]::new($name)
        try {$controller.Stop();$controller.WaitForStatus('Stopped',[TimeSpan]::FromSeconds(120))}finally{$controller.Dispose()}
    }
    $controller=[ServiceProcess.ServiceController]::new('Kafdeck')
    try {$controller.Start();$controller.WaitForStatus('Running',[TimeSpan]::FromSeconds(60))}finally{$controller.Dispose()}
    Healthy
    $stage='same-native-installer-revision-upgrade';$proof.stage=$stage
    # A synthetic prior installer receipt forces the real cold-backup/update path without
    # inventing a new native version or repeating unchanged native runtime acceptance.
    $receipt=Get-Content -LiteralPath $receiptPath -Raw|ConvertFrom-Json
    $receipt.packageHash=('1'*64)
    $receipt|ConvertTo-Json -Depth 15|Set-Content -LiteralPath $receiptPath -Encoding UTF8
    $request.action='upgrade';$request.password=''
    $result=Worker $request
    Assert-True ($result.exit -eq 0 -and $result.event.stage -eq 'complete' -and !$result.event.firstAdmin) ('Upgrade failed: '+$result.event.category)
    $journal=Get-Content -LiteralPath (Join-Path $root 'offline-upgrade.json') -Raw|ConvertFrom-Json
    Assert-True ($journal.phase -eq 'complete' -and $journal.backupReady) 'Verified cold backup was not exercised.'
    $stage='remove-retain-and-restore';$proof.stage=$stage
    $request.action='remove'
    [GC]::Collect();[GC]::WaitForPendingFinalizers()
    $result=Worker $request
    Assert-True ($result.exit -eq 0 -and $result.event.stage -eq 'removed') ('Removal failed: '+$result.event.category)
    foreach($name in @('Kafdeck','Kafdeck-Keycloak','Kafdeck-PostgreSQL')){Assert-True (!(Get-CimInstance Win32_Service -Filter "Name='$name'")) 'A removed service remains.'}
    $request.action='install';$request.password=$password;$request.language='fa'
    $result=Worker $request
    Assert-True ($result.exit -eq 0 -and $result.event.stage -eq 'complete' -and !$result.event.firstAdmin) ('Restore failed: '+$result.event.category)
    Healthy
    Assert-True ((Get-FileHash -LiteralPath $secretPath).Hash -eq $baselineSecret -and (Get-FileHash -LiteralPath $identityPath).Hash -eq $baselineIdentity -and [IO.File]::ReadAllText($stateFile) -eq 'retained-acceptance-data' -and (Owner-Count) -eq '1') 'Owner, encrypted secrets or retained state changed.'
    $receipt=Get-Content -LiteralPath $receiptPath -Raw|ConvertFrom-Json
    Assert-True ($receipt.licenseAcceptance.language -eq 'fa') 'Acceptance language was not recorded.'
    $registration=Get-ItemProperty -LiteralPath 'HKLM:/Software/Microsoft/Windows/CurrentVersion/Uninstall/AradITC.Kafdeck'
    Assert-True ($registration.Publisher -eq 'Arad ITC' -and $registration.InstallLocation -eq $root) 'Installed apps registration is incorrect.'
    foreach($folder in @('config','secrets','logs','services')){
        foreach($file in Get-ChildItem -LiteralPath (Join-Path $root $folder) -Recurse -File|Where-Object {$_.Extension -in @('.json','.txt','.xml','.log','.conf','.ps1','.dpapi')}){
            Assert-True (![IO.File]::ReadAllText($file.FullName).Contains($password)) 'Windows credential was persisted in installation files.'
        }
    }
    $proof=@{success=$true;stage='complete';nativeVersion=$build.version;installerSource=$build.installerSource;exeSha256=$build.exeSha256;payloadSha256=$build.payload.PayloadSha256;productionModuleCache=$true;licenseRefusalNoEffects=$true;prerequisite3010Resume='injected return followed by real installation';actualOsReboot=$false;automaticServices=3;dependencyRestartHealthy=$true;firstOwnerCreatedInKeycloakPostgreSQL=$true;emailVerificationRequired=$false;upgradeBaseline='same native runtime, synthetic prior installer receipt';verifiedColdBackup=$true;serviceRemovalAndRestore=$true;ownerAndEncryptedSecretsPreserved=$true;noWindowsCredentialInFilesOrOutput=$true;languagesRecorded=@('en','fa');authenticode='not-signed';productionInstallationChanged=$false}
} catch {
    $proof.success=$false;$proof.stage=$stage;$proof.errorCategory=$_.Exception.GetType().Name;$proof.assertion=$script:assertion;$proof.lastWorker=$script:lastWorker
    if(Test-Path -LiteralPath (Join-Path $fixture.root 'config/offline-install.json')){$proof.installPhase=(Get-Content -LiteralPath (Join-Path $fixture.root 'config/offline-install.json') -Raw|ConvertFrom-Json).phase}
} finally {
    $password=$null
    $proof|ConvertTo-Json -Depth 8|Set-Content -LiteralPath $fixture.report -Encoding UTF8
}
if(!$proof.success){exit 1}
