#requires -Version 5.1
[CmdletBinding()]
param(
    [string]$InstallRoot='C:\Kafdeck',
    [string]$ServerAddress,
    [string[]]$KafkaHosts,
    [string[]]$AllowedClientNetworks=@('LocalSubnet'),
    [ValidateRange(1024,65535)][int]$AppPort=8080,
    [ValidateRange(1024,65535)][int]$IdentityPort=8443,
    [ValidateRange(1024,65535)][int]$DatabasePort=15432,
    [PSCredential]$Credential,
    [switch]$VerifyOnly
)
$ErrorActionPreference='Stop'
[Net.ServicePointManager]::SecurityProtocol=[Net.SecurityProtocolType]::Tls12
$mediaRoot=$PSScriptRoot
. (Join-Path $mediaRoot 'payload/scripts/Offline-Tools.ps1')
Write-Host 'Checking the offline package...'
$manifest=Test-OfflineMedia $mediaRoot
if($VerifyOnly){Write-Host "Verified $($manifest.files.Count) files. No installation changes made.";return}
$identity=[Security.Principal.WindowsIdentity]::GetCurrent()
if(!([Security.Principal.WindowsPrincipal]::new($identity)).IsInRole([Security.Principal.WindowsBuiltInRole]::Administrator)){throw 'Open Windows PowerShell using Run as administrator.'}
if(![Environment]::Is64BitProcess -or [Environment]::OSVersion.Version.Build -lt 17763){throw 'Use 64-bit Windows Server 2019 or newer and 64-bit Windows PowerShell.'}
$InstallRoot=[IO.Path]::GetFullPath($InstallRoot).TrimEnd('\')
if($InstallRoot -eq [IO.Path]::GetPathRoot($InstallRoot).TrimEnd('\') -or $InstallRoot.StartsWith($env:SystemRoot,[StringComparison]::OrdinalIgnoreCase) -or $InstallRoot -match '[\s\x80-\uffff]' -or $InstallRoot.StartsWith($mediaRoot+'\',[StringComparison]::OrdinalIgnoreCase) -or $mediaRoot.StartsWith($InstallRoot+'\',[StringComparison]::OrdinalIgnoreCase) -or $InstallRoot -eq $mediaRoot){throw 'Use a separate application directory such as C:\Kafdeck; extract this package to C:\Kafdeck-Setup. The application path must use ASCII without spaces.'}
$receiptFile=Join-Path $InstallRoot 'config/offline-install.json'
$packageHash=(Get-FileHash -LiteralPath (Join-Path $mediaRoot 'bundle-manifest.json')).Hash
$receipt=$null
if(Test-Path -LiteralPath $receiptFile){
    $receipt=Get-Content -LiteralPath $receiptFile -Raw|ConvertFrom-Json
    if($receipt.userSid -ne $identity.User.Value -or $receipt.packageHash -ne $packageHash -or $receipt.root -ne $InstallRoot){throw 'Installation receipt does not match this account/package/path. Existing state was preserved.'}
    if($ServerAddress -and $ServerAddress -ne $receipt.address){throw 'Use the original address when resuming this installation.'}
    $ServerAddress=$receipt.address;$AppPort=$receipt.appPort;$IdentityPort=$receipt.identityPort;$DatabasePort=$receipt.databasePort
}else {
    if((Test-Path -LiteralPath $InstallRoot) -and (Get-ChildItem -LiteralPath $InstallRoot -Force|Select-Object -First 1)){throw 'The target directory is not empty. Existing installations are not overwritten.'}
    if(!$ServerAddress){
        $suggested=(Get-NetIPConfiguration|Where-Object {$_.IPv4DefaultGateway}|ForEach-Object {$_.IPv4Address.IPAddress}|Select-Object -First 1)
        $ServerAddress=Read-Host "Windows Server IPv4 [$suggested]"
        if(!$ServerAddress){$ServerAddress=$suggested}
    }
    $ip=$null
    if(![Net.IPAddress]::TryParse($ServerAddress,[ref]$ip) -or $ip.AddressFamily -ne [Net.Sockets.AddressFamily]::InterNetwork){throw 'Enter the IPv4 address assigned to this server, without a port.'}
    $b=$ip.GetAddressBytes()
    if(!($b[0] -eq 10 -or $b[0] -eq 127 -or ($b[0] -eq 172 -and $b[1] -ge 16 -and $b[1] -le 31) -or ($b[0] -eq 192 -and $b[1] -eq 168))){throw 'This internal-network package requires a private server IPv4 address.'}
    if(!(Get-NetIPAddress -AddressFamily IPv4|Where-Object {$_.IPAddress -eq $ServerAddress})){throw 'The chosen IP is not assigned to this server.'}
    if(!$KafkaHosts){$KafkaHosts=(Read-Host 'Kafka host IPv4 addresses, separated by comma (Arad Office: 192.168.100.33)').Split(',')}
    $networks=@();foreach($hostIp in $KafkaHosts){$parsed=$null;$hostIp=$hostIp.Trim();if(![Net.IPAddress]::TryParse($hostIp,[ref]$parsed) -or $parsed.AddressFamily -ne [Net.Sockets.AddressFamily]::InterNetwork){throw 'KafkaHosts must contain IPv4 addresses, without ports.'};$networks+=$hostIp+'/32'}
    if(@($AppPort,$IdentityPort,$DatabasePort|Select-Object -Unique).Count -ne 3){throw 'Choose three distinct ports.'}
    $listeners=[Net.NetworkInformation.IPGlobalProperties]::GetIPGlobalProperties().GetActiveTcpListeners()
    foreach($port in @($AppPort,$IdentityPort,$DatabasePort)){if($listeners.Port -contains $port){throw "Port $port is already in use; no existing service was changed."}}
    foreach($name in @('Kafdeck','Kafdeck-Keycloak')){if(Get-ScheduledTask -TaskName $name -ErrorAction SilentlyContinue){throw "Task $name already exists."}}
    foreach($name in @('Kafdeck-PostgreSQL','Kafdeck-Keycloak','Kafdeck')){if(Get-Service -Name $name -ErrorAction SilentlyContinue){throw "Service $name already exists."}}
    if(([IO.DriveInfo]::new([IO.Path]::GetPathRoot($InstallRoot))).AvailableFreeSpace -lt 4GB){throw 'At least 4 GB free space is required for installation.'}
    New-Item -ItemType Directory -Force -Path $InstallRoot,(Join-Path $InstallRoot 'config')|Out-Null
    $receipt=[pscustomobject]@{root=$InstallRoot;userSid=$identity.User.Value;userName=$identity.Name;packageHash=$packageHash;address=$ServerAddress;appPort=$AppPort;identityPort=$IdentityPort;databasePort=$DatabasePort;origin="https://${ServerAddress}:$AppPort";identityOrigin="https://${ServerAddress}:$IdentityPort";networks=$networks;allowedClientNetworks=$AllowedClientNetworks;phase='created'}
    $receipt|ConvertTo-Json -Depth 8|Set-Content -LiteralPath $receiptFile -Encoding utf8
}
function Save-Phase([string]$Phase){$receipt.phase=$Phase;$receipt|ConvertTo-Json -Depth 8|Set-Content -LiteralPath $receiptFile -Encoding utf8;Write-Host $Phase}
if($receipt.phase -eq 'complete'){
    Write-Host "Installed at $($receipt.origin)/. To refresh an unused setup code, run $InstallRoot\scripts\New-SetupCode.ps1 using this Windows account."
    return
}
$Credential=Get-OfflineWindowsCredential $Credential
# Keep setup-code issuance, DPAPI and the service account on the same identity.
# Service credentials are held by Windows SCM, never in XML or command arguments.
foreach($folder in @('app','scripts','docs','agent')){
    New-Item -ItemType Directory -Force (Join-Path $InstallRoot $folder)|Out-Null
    Copy-Item -Path (Join-Path $mediaRoot "payload/$folder/*") -Destination (Join-Path $InstallRoot $folder) -Recurse -Force
}
. (Join-Path $InstallRoot 'scripts/Offline-Tools.ps1')
foreach($folder in @('runtime','state','logs')){New-Item -ItemType Directory -Force (Join-Path $InstallRoot $folder)|Out-Null}
Copy-Item -LiteralPath (Join-Path $mediaRoot 'media/winsw.exe') -Destination (Join-Path $InstallRoot 'runtime/winsw.exe') -Force
. (Join-Path $InstallRoot 'scripts/Windows-Services.ps1')
# Root-readable binaries; private configuration/state reset inherited access below.
& icacls.exe $InstallRoot /inheritance:r /grant:r "*$($identity.User.Value):(OI)(CI)F" '*S-1-5-18:(OI)(CI)F' '*S-1-5-32-544:(OI)(CI)F' '*S-1-5-20:(OI)(CI)RX'|Out-Null
if($LASTEXITCODE){throw 'Failed to secure the application root.'}
Save-Phase 'Installing included Windows runtime prerequisites'
$vc=Join-Path $mediaRoot 'media/vc_redist.x64.exe'
$vcSignature=Get-AuthenticodeSignature -LiteralPath $vc
if($vcSignature.Status -ne 'Valid' -or $vcSignature.SignerCertificate.Subject -notlike '*Microsoft Corporation*'){throw 'The Microsoft runtime signature is invalid.'}
$vcProcess=Start-Process -FilePath $vc -ArgumentList @('/install','/quiet','/norestart','/log',(Join-Path $InstallRoot 'logs/vcredist.log')) -WindowStyle Hidden -Wait -PassThru
if($vcProcess.ExitCode -eq 3010){Save-Phase 'Restart required after Windows prerequisites';throw 'Restart Windows, then run this SAME installer again.'}
if($vcProcess.ExitCode -notin @(0,1638,-2147023258)){throw "Visual C++ runtime installation failed ($($vcProcess.ExitCode))."}
foreach($component in @(@{name='java';archive='java21.zip';check='bin/java.exe'},@{name='postgresql';archive='postgresql17.zip';check='bin/pg_ctl.exe'},@{name='keycloak';archive='keycloak.zip';check='bin/kc.bat'})){
    $dest=Join-Path $InstallRoot ('runtime/'+$component.name)
    if(!(Test-Path -LiteralPath (Join-Path $dest $component.check))){
        if((Test-Path -LiteralPath $dest) -and (Get-ChildItem -LiteralPath $dest -Force|Select-Object -First 1)){throw "Incomplete runtime directory: $dest. Preserve it and inspect the log."}
        Expand-OfflineArchive (Join-Path $mediaRoot ('media/'+$component.archive)) $dest
    }
}
Save-Phase 'Preparing HTTPS and the login realm'
$productionFile=Join-Path $InstallRoot 'config/appsettings.Production.json'
if(!(Test-Path -LiteralPath $productionFile)){
    & (Join-Path $InstallRoot 'scripts/Prepare-NewServer.ps1') -ServerAddress $ServerAddress -KafkaNetworks $receipt.networks -KeycloakHome (Join-Path $InstallRoot 'runtime/keycloak') -JavaHome (Join-Path $InstallRoot 'runtime/java') -AppPort $AppPort -IdentityPort $IdentityPort -DatabasePort $DatabasePort
    "`r`nhttp-host=$ServerAddress"|Add-Content -LiteralPath (Join-Path $InstallRoot 'config/keycloak.conf') -Encoding ascii
}
$production=Get-Content -LiteralPath $productionFile -Raw|ConvertFrom-Json
$keys=Join-Path $InstallRoot 'state/keys';New-Item -ItemType Directory -Force $keys|Out-Null
$production.Kafdeck|Add-Member -NotePropertyName DataProtection -NotePropertyValue @{KeyDirectory=$keys;ApplicationName=('Kafdeck.Windows.'+$receipt.userSid)} -Force
$production|Add-Member -NotePropertyName Logging -NotePropertyValue @{LogLevel=@{Default='Warning';'Microsoft.AspNetCore'='Warning'}} -Force
$production|ConvertTo-Json -Depth 15|Set-Content -LiteralPath $productionFile -Encoding utf8
foreach($folder in @('config','secrets','state','logs')){
    & icacls.exe (Join-Path $InstallRoot $folder) /inheritance:r /grant:r "*$($identity.User.Value):(OI)(CI)F" '*S-1-5-18:(OI)(CI)F' '*S-1-5-32-544:(OI)(CI)F'|Out-Null
    if($LASTEXITCODE){throw 'Failed to secure persistent state.'}
}
$publicCert=Join-Path $InstallRoot 'secrets/server-public.cer'
$certificate=[Security.Cryptography.X509Certificates.X509Certificate2]::new($publicCert)
$null=Import-Certificate -FilePath $publicCert -CertStoreLocation Cert:\LocalMachine\Root
New-Item -ItemType Directory -Force (Join-Path $InstallRoot 'public')|Out-Null
Copy-Item -LiteralPath $publicCert -Destination (Join-Path $InstallRoot 'public/server-public.cer') -Force
Copy-Item -LiteralPath (Join-Path $InstallRoot 'secrets/server-public.pem') -Destination (Join-Path $InstallRoot 'public/server-public.pem') -Force
Save-Phase 'Initializing the private PostgreSQL service'
$pg=Join-Path $InstallRoot 'runtime/postgresql/bin'
$data=Join-Path $InstallRoot 'state/postgresql'
$pgSecret=Join-Path $InstallRoot 'secrets/postgres-admin.dpapi'
if(!(Test-Path -LiteralPath $pgSecret)){Save-OfflineSecret $pgSecret (New-OfflineSecret)}
$pgPassword=Read-OfflineSecret $pgSecret
if(!(Test-Path -LiteralPath (Join-Path $data 'PG_VERSION'))){
    if((Test-Path -LiteralPath $data) -and (Get-ChildItem -LiteralPath $data -Force|Select-Object -First 1)){throw 'An incomplete database directory exists. It was preserved for recovery.'}
    $pwFile=Join-Path $InstallRoot 'secrets/postgres-init.tmp'
    [IO.File]::WriteAllText($pwFile,$pgPassword)
    try {$null=Invoke-OfflineNative -File (Join-Path $pg 'initdb.exe') -Arguments @('-D',$data,'--username=kafdeck_pg_admin','--auth=scram-sha-256','--encoding=UTF8','--locale=C',"--pwfile=$pwFile") -Log (Join-Path $InstallRoot 'logs/postgres-init.log')}
    finally {if(Test-Path -LiteralPath $pwFile){Remove-Item -LiteralPath $pwFile}}
}
@("listen_addresses='127.0.0.1'","port=$DatabasePort","password_encryption='scram-sha-256'","logging_collector=on","log_directory='log'","log_min_messages=warning")|Add-Content -LiteralPath (Join-Path $data 'postgresql.conf') -Encoding ascii
& icacls.exe (Join-Path $InstallRoot 'state') /grant '*S-1-5-20:RX'|Out-Null
& icacls.exe $data /grant '*S-1-5-20:(OI)(CI)M' /T|Out-Null
if($LASTEXITCODE){throw 'Cannot grant PostgreSQL access to its own data directory.'}
$service=Get-CimInstance Win32_Service -Filter "Name='Kafdeck-PostgreSQL'"
if($service -and ($service.PathName -notlike "*$(Join-Path $pg 'pg_ctl.exe')*" -or $service.PathName -notlike "*$data*")){throw 'The existing PostgreSQL service does not match this installation.'}
if(!$service){
    $account=([Security.Principal.SecurityIdentifier]::new('S-1-5-20')).Translate([Security.Principal.NTAccount]).Value
    $null=Invoke-OfflineNative -File (Join-Path $pg 'pg_ctl.exe') -Arguments @('register','-N','Kafdeck-PostgreSQL','-D',$data,'-U',$account,'-S','auto','-e','Kafdeck-PostgreSQL') -Log (Join-Path $InstallRoot 'logs/postgres-register.log')
}
$databaseService=Start-Service Kafdeck-PostgreSQL -PassThru
$databaseService.WaitForStatus([ServiceProcess.ServiceControllerStatus]::Running,[TimeSpan]::FromSeconds(30))
$psqlArgs=@('-X','--no-password','-h','127.0.0.1','-p',[string]$DatabasePort,'-U','kafdeck_pg_admin','-d','postgres','-v','ON_ERROR_STOP=1')
$pgEnvironment=@{PGPASSWORD=$pgPassword;PGCONNECT_TIMEOUT='3'}
$databaseReady=$false;$deadline=(Get-Date).AddSeconds(60)
do {
    try {$null=Invoke-OfflineNative -File (Join-Path $pg 'psql.exe') -Arguments ($psqlArgs+@('-c','SELECT 1')) -Environment $pgEnvironment -Log (Join-Path $InstallRoot 'logs/postgres-ready.log');$databaseReady=$true}catch{Start-Sleep -Seconds 2}
}while(!$databaseReady -and (Get-Date) -lt $deadline)
if(!$databaseReady){throw 'PostgreSQL did not accept authenticated connections. See logs/postgres-ready.log; its data was preserved.'}
$counts=Invoke-OfflineNative -File (Join-Path $pg 'psql.exe') -Arguments ($psqlArgs+@('-At','-c',"SELECT (EXISTS(SELECT 1 FROM pg_roles WHERE rolname='kafdeck_keycloak'))::int || ':' || (EXISTS(SELECT 1 FROM pg_database WHERE datname='kafdeck_keycloak'))::int")) -Environment $pgEnvironment
if($counts.Trim() -eq '0:0'){
    $null=Invoke-OfflineNative -File (Join-Path $pg 'psql.exe') -Arguments ($psqlArgs+@('-f',(Join-Path $InstallRoot 'secrets/create-keycloak-db.sql'))) -Environment $pgEnvironment -Log (Join-Path $InstallRoot 'logs/keycloak-database.log')
}elseif($counts.Trim() -ne '1:1'){throw 'Partially created identity database detected; existing data was preserved.'}
$dbPassword=Read-OfflineSecret (Join-Path $InstallRoot 'secrets/keycloak-database-password.dpapi')
$null=Invoke-OfflineNative -File (Join-Path $pg 'psql.exe') -Arguments @('-X','--no-password','-h','127.0.0.1','-p',[string]$DatabasePort,'-U','kafdeck_keycloak','-d','kafdeck_keycloak','-c','SELECT 1') -Environment @{PGPASSWORD=$dbPassword} -Log (Join-Path $InstallRoot 'logs/keycloak-database-ready.log')
$bootstrapFile=Join-Path $InstallRoot 'secrets/keycloak-bootstrap.dpapi'
if(!(Test-Path -LiteralPath (Join-Path $InstallRoot 'secrets/identity-admin-password.dpapi')) -and !(Test-Path -LiteralPath $bootstrapFile)){Save-OfflineSecret $bootstrapFile (New-OfflineSecret)}
$localFile=Join-Path $InstallRoot 'config/local-keycloak.json'
$local=Get-Content -LiteralPath $localFile -Raw|ConvertFrom-Json
$local|Add-Member -NotePropertyName bootstrapPasswordFile -NotePropertyValue $bootstrapFile -Force
$local|ConvertTo-Json|Set-Content -LiteralPath $localFile -Encoding utf8
Install-OfflineWindowsServices $InstallRoot $Credential
$receipt|Add-Member -NotePropertyName servicesInstalledAt -NotePropertyValue ([DateTime]::UtcNow.ToString('o')) -Force
$receipt|Add-Member -NotePropertyName deploymentMode -NotePropertyValue 'windows-services' -Force
Save-Phase 'Starting Keycloak (first start can take several minutes)'
$null=Start-Service Kafdeck-Keycloak
$null=Wait-OfflineUrl "$($receipt.identityOrigin)/realms/kafdeck/.well-known/openid-configuration" 600
Complete-OfflineIdentity $InstallRoot $receipt.identityOrigin
Save-Phase 'Starting Kafdeck'
$null=Start-Service Kafdeck
$null=Wait-OfflineUrl "$($receipt.origin)/healthz" 180
$ruleName='Kafdeck-Offline-HTTPS'
if(!(Get-NetFirewallRule -Name $ruleName -ErrorAction SilentlyContinue)){
    $null=New-NetFirewallRule -Name $ruleName -DisplayName 'Kafdeck offline installation HTTPS' -Direction Inbound -Action Allow -Protocol TCP -LocalPort $AppPort,$IdentityPort -RemoteAddress @($receipt.allowedClientNetworks+$receipt.networks|Select-Object -Unique) -Profile Any
}
$setup=Invoke-RestMethod -Uri "$($receipt.origin)/api/v1/setup" -MaximumRedirection 0 -TimeoutSec 10
if($setup.state -eq 'available'){
    Save-Phase 'Issuing the first-administrator code'
    & (Join-Path $InstallRoot 'scripts/New-SetupCode.ps1')
}elseif($setup.state -eq 'complete'){
    Write-Host "The first administrator already exists. Sign in at $($receipt.origin)/"
}else{throw 'First-owner provisioning requires recovery. Its state was preserved; do not reinitialize.'}
Save-Phase 'complete'
& (Join-Path $InstallRoot 'scripts/Test-InstalledServices.ps1')
Write-Host "Installation complete. Public certificate for other clients: $InstallRoot\public\server-public.cer"
Write-Host "Keycloak recovery details: $InstallRoot\scripts\Show-RecoveryInfo.ps1"
