#requires -Version 5.1
# Real component acceptance without creating host services/tasks or touching the live installation.
param([switch]$ReuseRuntime,[switch]$Resume)
$ErrorActionPreference='Stop'
[Net.ServicePointManager]::SecurityProtocol=[Net.SecurityProtocolType]::Tls12
$repo=[IO.Path]::GetFullPath((Join-Path $PSScriptRoot '../..'))
$bundle=Join-Path $repo 'artifacts/windows-offline/bundle'
$test=Join-Path $repo 'artifacts/windows-offline/acceptance'
if(Test-Path -LiteralPath $test){
    if(!$ReuseRuntime -and !$Resume){throw 'Use a fresh acceptance directory; existing data was preserved.'}
    if(Get-CimInstance Win32_Process|Where-Object {$_.Name -in @('postgres.exe','java.exe','Kafdeck.Api.exe') -and $_.CommandLine -like "*$test*"}){throw 'A fixture process is still running.'}
    foreach($relative in $(if($Resume){@()}else{@('config','secrets','state','logs','runtime/keycloak/data')})){
        $owned=[IO.Path]::GetFullPath((Join-Path $test $relative))
        if(!$owned.StartsWith([IO.Path]::GetFullPath($test)+'\',[StringComparison]::OrdinalIgnoreCase)){throw 'Fixture cleanup escaped its root.'}
        if(Test-Path -LiteralPath $owned){Remove-Item -LiteralPath $owned -Recurse -Force}
    }
}
New-Item -ItemType Directory -Force $test|Out-Null
Copy-Item -Path (Join-Path $bundle 'payload/*') -Destination $test -Recurse -Force
. (Join-Path $test 'scripts/Offline-Tools.ps1')
$null=Test-OfflineMedia $bundle
function Port {$s=[Net.Sockets.TcpListener]::new([Net.IPAddress]::Loopback,0);$s.Start();$p=$s.LocalEndpoint.Port;$s.Stop();return $p}
$appPort=Port;$identityPort=Port;$dbPort=Port
$pgStarted=$false;$processes=@();$certificate=$null
try {
    foreach($name in @('java','postgresql','keycloak')){
        $zip=if($name -eq 'java'){'java21.zip'}elseif($name -eq 'postgresql'){'postgresql17.zip'}else{'keycloak.zip'}
        if(!(Test-Path -LiteralPath (Join-Path $test "runtime/$name"))){Expand-OfflineArchive (Join-Path $bundle "media/$zip") (Join-Path $test "runtime/$name")}
    }
    if(!$Resume){& (Join-Path $test 'scripts/Prepare-NewServer.ps1') -ServerAddress 127.0.0.1 -KafkaNetworks 192.168.100.33/32 -KeycloakHome (Join-Path $test 'runtime/keycloak') -JavaHome (Join-Path $test 'runtime/java') -AppPort $appPort -IdentityPort $identityPort -DatabasePort $dbPort}
    $local=Get-Content (Join-Path $test 'config/local-keycloak.json') -Raw|ConvertFrom-Json
    if($Resume){
        $identityPort=([uri]$local.origin).Port
        $settings=Get-Content (Join-Path $test 'config/appsettings.Production.json') -Raw|ConvertFrom-Json
        $appPort=([uri]$settings.Kafdeck.Deployment.ListenUrls[0]).Port
        $dbPort=[int]([regex]::Matches((Get-Content (Join-Path $test 'state/postgresql/postgresql.conf') -Raw),'(?m)^port=(\d+)')[0].Groups[1].Value)
    }
    $certificate=[Security.Cryptography.X509Certificates.X509Certificate2]::new((Join-Path $test 'secrets/server-public.cer'))
    # This non-admin fixture uses an explicit private CA in this process only.
    # The actual elevated installer imports its CA into LocalMachine\Root.
    Add-Type -TypeDefinition @'
using System;
using System.Net;
using System.Net.Security;
using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;
public static class KafdeckFixtureTls {
  public static void Configure(string path,int appPort,int idpPort) {
    X509Certificate2 authority=new X509Certificate2(path);
    ServicePointManager.ServerCertificateValidationCallback=delegate(object sender,X509Certificate certificate,X509Chain supplied,SslPolicyErrors errors) {
      HttpWebRequest request=sender as HttpWebRequest;
      if(request==null || request.RequestUri.Host!="127.0.0.1" || (request.RequestUri.Port!=appPort && request.RequestUri.Port!=idpPort)) return errors==SslPolicyErrors.None;
      if(certificate==null || (errors & (SslPolicyErrors.RemoteCertificateNameMismatch|SslPolicyErrors.RemoteCertificateNotAvailable))!=0) return false;
      using(X509Chain chain=new X509Chain()) {
        chain.ChainPolicy.ExtraStore.Add(authority);
        chain.ChainPolicy.VerificationFlags=X509VerificationFlags.AllowUnknownCertificateAuthority;
        chain.ChainPolicy.RevocationMode=X509RevocationMode.NoCheck;
        chain.ChainPolicy.ApplicationPolicy.Add(new Oid("1.3.6.1.5.5.7.3.1"));
        if(!chain.Build(new X509Certificate2(certificate)) || chain.ChainElements.Count<2) return false;
        foreach(X509ChainStatus status in chain.ChainStatus) if(status.Status!=X509ChainStatusFlags.NoError && status.Status!=X509ChainStatusFlags.UntrustedRoot) return false;
        return chain.ChainElements[chain.ChainElements.Count-1].Certificate.Thumbprint==authority.Thumbprint;
      }
    };
  }
}
'@
    [KafdeckFixtureTls]::Configure((Join-Path $test 'secrets/server-public.cer'),$appPort,$identityPort)
    New-Item -ItemType Directory -Force (Join-Path $test 'logs'),(Join-Path $test 'state/keys')|Out-Null
    $pg=Join-Path $test 'runtime/postgresql/bin'
    $data=Join-Path $test 'state/postgresql'
    if($Resume){$adminSecret=Read-OfflineSecret (Join-Path $test 'secrets/pg-test-admin.dpapi')}else{
    $adminSecret=New-OfflineSecret
    Save-OfflineSecret (Join-Path $test 'secrets/pg-test-admin.dpapi') $adminSecret
    $pw=Join-Path $test 'secrets/pg-init.tmp';[IO.File]::WriteAllText($pw,$adminSecret)
    $null=Invoke-OfflineNative -File (Join-Path $pg 'initdb.exe') -Arguments @('-D',$data,'--username=kafdeck_pg_admin','--auth=scram-sha-256','--encoding=UTF8','--locale=C',"--pwfile=$pw") -Log (Join-Path $test 'logs/pg-init.log')
    Remove-Item -LiteralPath $pw
    @("listen_addresses='127.0.0.1'","port=$dbPort")|Add-Content (Join-Path $data 'postgresql.conf') -Encoding ascii
    }
    # A daemon retains inherited pipe handles; only capture terminating utilities.
    $launch=[Diagnostics.ProcessStartInfo]::new((Join-Path $pg 'pg_ctl.exe'))
    $launch.Arguments=(@('start','-D',$data,'-l',(Join-Path $test 'logs/postgresql.log'),'-w')|ForEach-Object {ConvertTo-OfflineArgument $_}) -join ' '
    $launch.UseShellExecute=$false;$launch.CreateNoWindow=$true
    $launcher=[Diagnostics.Process]::Start($launch)
    if(!$launcher.WaitForExit(60000) -or $launcher.ExitCode -ne 0){throw 'Fixture PostgreSQL failed to start'}
    $pgStarted=$true
    if(!$Resume){$null=Invoke-OfflineNative -File (Join-Path $pg 'psql.exe') -Arguments @('-X','--no-password','-h','127.0.0.1','-p',[string]$dbPort,'-U','kafdeck_pg_admin','-d','postgres','-v','ON_ERROR_STOP=1','-f',(Join-Path $test 'secrets/create-keycloak-db.sql')) -Environment @{PGPASSWORD=$adminSecret} -Log (Join-Path $test 'logs/pg-schema.log')}
    $bootstrapFile=Join-Path $test 'secrets/keycloak-bootstrap.dpapi'
    if(!$Resume){Save-OfflineSecret $bootstrapFile (New-OfflineSecret)}
    $local|Add-Member -NotePropertyName bootstrapPasswordFile -NotePropertyValue $bootstrapFile -Force
    $local|ConvertTo-Json|Set-Content (Join-Path $test 'config/local-keycloak.json') -Encoding utf8
    "`r`nhttp-host=127.0.0.1"|Add-Content (Join-Path $test 'config/keycloak.conf') -Encoding ascii
    $windowsPs=Join-Path $env:SystemRoot 'System32/WindowsPowerShell/v1.0/powershell.exe'
    $keycloak=Start-Process -FilePath $windowsPs -ArgumentList @('-NoProfile','-ExecutionPolicy','RemoteSigned','-File',(Join-Path $test 'scripts/Start-Keycloak.ps1')) -WorkingDirectory $test -WindowStyle Hidden -PassThru -RedirectStandardOutput (Join-Path $test 'logs/keycloak.out.log') -RedirectStandardError (Join-Path $test 'logs/keycloak.err.log')
    $processes+=$keycloak
    $idp="https://127.0.0.1:$identityPort"
    $discovery=Wait-OfflineUrl "$idp/realms/kafdeck/.well-known/openid-configuration" 600
    if($discovery.issuer -ne "$idp/realms/kafdeck"){throw 'Issuer mismatch'}
    Complete-OfflineIdentity $test $idp
    $settingsFile=Join-Path $test 'config/appsettings.Production.json'
    $settings=Get-Content -LiteralPath $settingsFile -Raw|ConvertFrom-Json
    $settings.Kafdeck|Add-Member -NotePropertyName DataProtection -NotePropertyValue @{KeyDirectory=(Join-Path $test 'state/keys');ApplicationName='Kafdeck.Offline.Acceptance'} -Force
    $settings|ConvertTo-Json -Depth 15|Set-Content -LiteralPath $settingsFile -Encoding utf8
    $api=Start-Process -FilePath $windowsPs -ArgumentList @('-NoProfile','-ExecutionPolicy','RemoteSigned','-File',(Join-Path $test 'scripts/Start-Kafdeck.ps1')) -WorkingDirectory $test -WindowStyle Hidden -PassThru -RedirectStandardOutput (Join-Path $test 'logs/api.out.log') -RedirectStandardError (Join-Path $test 'logs/api.err.log')
    $processes+=$api
    $origin="https://127.0.0.1:$appPort"
    $null=Wait-OfflineUrl "$origin/healthz" 180
    $tlsPassword=Read-OfflineSecret (Join-Path $test 'secrets/https-password.dpapi')
    $code=(Invoke-OfflineNative -File (Join-Path $test 'app/Kafdeck.Api.exe') -Arguments @('--contentRoot',(Join-Path $test 'config'),'--issue-setup-code') -Environment @{ASPNETCORE_ENVIRONMENT='Production';ASPNETCORE_Kestrel__Certificates__Default__Password=$tlsPassword}).Trim()
    if($code -notmatch '^[A-Za-z0-9_-]{32}$'){throw 'Setup code missing'}
    $setup=Invoke-RestMethod -Uri "$origin/api/v1/setup" -SessionVariable browser -MaximumRedirection 0
    if($setup.state -ne 'available' -or !$setup.csrf){throw 'First administrator setup unavailable'}
    $ownerPassword=New-OfflineSecret
    $body=@{code=$code;email='offline-owner@example.invalid';password=$ownerPassword}|ConvertTo-Json
    $result=Invoke-WebRequest -UseBasicParsing -Uri "$origin/api/v1/setup" -WebSession $browser -Method Post -MaximumRedirection 0 -Headers @{'X-Kafdeck-CSRF'=$setup.csrf} -ContentType 'application/json' -Body $body
    if($result.StatusCode -ne 204){throw 'First administrator creation failed'}
    $closed=Invoke-RestMethod -Uri "$origin/api/v1/setup" -WebSession $browser -MaximumRedirection 0
    if($closed.state -ne 'complete'){throw 'First administrator setup did not close'}
    $sql="SELECT count(*) FROM user_entity WHERE realm_id=(SELECT id FROM realm WHERE name='kafdeck') AND email='offline-owner@example.invalid' AND email_verified=false;"
    $count=Invoke-OfflineNative -File (Join-Path $pg 'psql.exe') -Arguments @('-X','--no-password','-h','127.0.0.1','-p',[string]$dbPort,'-U','kafdeck_pg_admin','-d','kafdeck_keycloak','-At','-c',$sql) -Environment @{PGPASSWORD=$adminSecret}
    if($count.Trim() -ne '1'){throw 'The owner was not persisted in real PostgreSQL without email verification'}
    $evidence=@{java='21.0.13';postgresql='17.12';keycloak='26.8.1';realPostgreSQL=$true;allIncludedBinaries=$true;tlsValidated=$true;identityRecoveryCreated=$true;temporaryAdminRemoved=$true;firstOwnerHttp=204;setupClosed=$true;emailVerificationRequired=$false;nativeApi=$true;existingInstallationChanged=$false;windowsServiceAndTaskRegistration='not executed: this host has no elevated administrator token';os=[Environment]::OSVersion.VersionString}
    $evidence|ConvertTo-Json -Depth 6|Set-Content (Join-Path $repo 'artifacts/windows-offline/acceptance-evidence.json') -Encoding utf8
    $evidence|ConvertTo-Json -Compress
}finally {
    # Kill only processes whose command line is rooted in this disposable fixture.
    $owned=Get-CimInstance Win32_Process|Where-Object {$_.Name -in @('java.exe','Kafdeck.Api.exe') -and $_.CommandLine -like "*$test*"}
    foreach($p in $owned){Stop-Process -Id $p.ProcessId -ErrorAction SilentlyContinue}
    foreach($p in $processes){if(!$p.HasExited){Stop-Process -Id $p.Id -ErrorAction SilentlyContinue}}
    if($pgStarted){try {$null=Invoke-OfflineNative -File (Join-Path $pg 'pg_ctl.exe') -Arguments @('stop','-D',$data,'-m','fast','-w')}catch{}}
    if($certificate){
        foreach($store in @('Root','My')){$path='Cert:\CurrentUser\'+$store+'\'+$certificate.Thumbprint;if(Test-Path -LiteralPath $path){Remove-Item -LiteralPath $path -Force}}
        if($local.certificateThumbprint -ne $certificate.Thumbprint){$path='Cert:\CurrentUser\My\'+$local.certificateThumbprint;if(Test-Path -LiteralPath $path){Remove-Item -LiteralPath $path -Force}}
    }
}
