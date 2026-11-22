#requires -Version 5.1
#requires -RunAsAdministrator
# Fixed entry point. The only request is JSON on the anonymous stdin pipe, never command-line credentials.
$ErrorActionPreference='Stop'
$ProgressPreference='SilentlyContinue'
# A launch from PowerShell 7 can inherit its incompatible module directories.
# Load only the trusted operating-system Windows PowerShell 5.1 modules.
$env:PSModulePath=[IO.Path]::Combine($env:SystemRoot,'System32\WindowsPowerShell\v1.0\Modules')
[Console]::OutputEncoding=[Text.UTF8Encoding]::new($false)
$request=$null
function Send-Event($Value){[Console]::Out.WriteLine(($Value|ConvertTo-Json -Compress -Depth 5));[Console]::Out.Flush()}
try {
    $line=[Console]::In.ReadLine()
    if(!$line -or $line.Length -gt 32768){throw 'Invalid setup input.'}
    $request=$line|ConvertFrom-Json;$line=$null
    if($request.action -notin @('install','upgrade','remove') -or $request.accepted -ne $true){throw 'License acceptance is required.'}
    $cache=$PSScriptRoot
    $media=Join-Path $cache 'media'
    . (Join-Path $media 'payload/scripts/Offline-Tools.ps1')
    $manifest=Test-OfflineMedia $media
    $root=[IO.Path]::GetFullPath([string]$request.root).TrimEnd('\')
    if(!(Test-OfflineInstallPathCharacters $root) -or $root -eq [IO.Path]::GetPathRoot($root).TrimEnd('\') -or $root.StartsWith($env:SystemRoot,[StringComparison]::OrdinalIgnoreCase) -or $cache.StartsWith($root+'\',[StringComparison]::OrdinalIgnoreCase) -or $root.StartsWith($cache+'\',[StringComparison]::OrdinalIgnoreCase)){throw 'Invalid application directory.'}
    $registration='HKLM:\Software\Microsoft\Windows\CurrentVersion\Uninstall\AradITC.Kafdeck'
    if(Test-Path -LiteralPath $registration){$existing=Get-ItemProperty -LiteralPath $registration;if($existing.InstallLocation -ne $root){throw 'Another installation is already registered.'}}
    $receiptFile=Join-Path $root 'config/offline-install.json'
    # Upgrade owns receipt/journal validation under its exclusive lock. During
    # interrupted rollback the live config directory can legitimately be absent.
    if($request.action -ne 'upgrade' -and (Test-Path -LiteralPath $receiptFile)){
        $receipt=Get-Content -LiteralPath $receiptFile -Raw|ConvertFrom-Json
        if($receipt.userSid -ne [Security.Principal.WindowsIdentity]::GetCurrent().User.Value -or $receipt.root -ne $root){throw 'Use the original installation account and directory.'}
    } else {$receipt=$null}
    Send-Event @{stage='running'}
    if($request.action -eq 'remove'){
        & (Join-Path $media 'payload/scripts/Remove-OfflineServices.ps1') -InstallRoot $root *> $null
        if(Test-Path -LiteralPath $registration){Remove-Item -LiteralPath $registration}
        Send-Event @{stage='removed';root=$root};exit 0
    }
    if($request.action -eq 'upgrade'){
        & (Join-Path $media 'Upgrade-Kafdeck.ps1') -InstallRoot $root *> $null
    } else {
        if($receipt -and $receipt.phase -eq 'services-removed' -and @(Get-Service -Name 'Kafdeck','Kafdeck-Keycloak','Kafdeck-PostgreSQL' -ErrorAction SilentlyContinue).Count){throw 'Finish removing retained services before restoring them.'}
        if(!$request.password -or $request.password.Length -gt 1024){throw 'Enter the current Windows account password.'}
        $secure=ConvertTo-SecureString ([string]$request.password) -AsPlainText -Force
        $request.password=$null
        try {
            $credential=[PSCredential]::new([Security.Principal.WindowsIdentity]::GetCurrent().Name,$secure)
            & (Join-Path $media 'Install-Kafdeck.ps1') -InstallRoot $root -ServerAddress ([string]$request.address) -KafkaHosts @($request.kafkaHosts) -AllowedClientNetworks @($request.clientNetworks) -AppPort ([int]$request.appPort) -IdentityPort ([int]$request.identityPort) -DatabasePort ([int]$request.databasePort) -Credential $credential *> $null
        } finally {$credential=$null;$secure.Dispose()}
    }
    $receipt=Get-Content -LiteralPath $receiptFile -Raw|ConvertFrom-Json
    if($receipt.packageHash -ne (Get-FileHash -LiteralPath (Join-Path $media 'bundle-manifest.json')).Hash){Send-Event @{stage='recovered';root=$root};exit 0}
    $receipt|Add-Member -NotePropertyName licenseAcceptance -NotePropertyValue @{time=[DateTime]::UtcNow.ToString('o');sid=$receipt.userSid;license='Apache-2.0';language=$request.language;packageHash=$receipt.packageHash} -Force
    $receipt|ConvertTo-Json -Depth 15|Set-Content -LiteralPath $receiptFile -Encoding UTF8
    $null=New-Item -Path $registration -Force
    $values=@{DisplayName='Kafdeck';DisplayVersion=$receipt.version;Publisher='Arad ITC';URLInfoAbout='https://arad-itc.com';HelpLink='mailto:info@arad-itc.org';InstallLocation=$root;DisplayIcon=(Join-Path $cache 'Kafdeck.Setup.exe');UninstallString=('"'+(Join-Path $cache 'Kafdeck.Setup.exe')+'" --remove');NoModify=1;NoRepair=1}
    foreach($name in $values.Keys){$null=New-ItemProperty -LiteralPath $registration -Name $name -Value $values[$name] -PropertyType $(if($values[$name] -is [int]){'DWord'}else{'String'}) -Force}
    $setup=Invoke-RestMethod -Uri ($receipt.origin+'/api/v1/setup') -MaximumRedirection 0 -TimeoutSec 15
    $code=$null
    if($setup.state -eq 'available'){
        $firstAdmin=Join-Path $root 'secrets/FIRST-ADMIN.txt'
        if(Test-Path -LiteralPath $firstAdmin){$text=Get-Content -LiteralPath $firstAdmin -Raw;$match=[regex]::Match($text,'(?m)^One-time code \(15 minutes\): ([A-Za-z0-9_-]{32})\r?$');if($match.Success){$code=$match.Groups[1].Value}}
    }
    Send-Event @{stage='complete';origin=$receipt.origin;firstAdmin=($setup.state -eq 'available');code=$code;root=$root}
    exit 0
} catch {
    # Do not forward PowerShell records, native output, SQL, credentials or setup codes into GUI logs.
    if($_.Exception.Message -eq 'Restart Windows, then run this SAME installer again.') {Send-Event @{stage='reboot'};exit 3010}
    Send-Event @{stage='failed';category=$_.Exception.GetType().Name;source=[IO.Path]::GetFileName($_.InvocationInfo.ScriptName);line=$_.InvocationInfo.ScriptLineNumber}
    exit 1
} finally {if($request){$request.password=$null}}
