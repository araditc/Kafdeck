#requires -Version 5.1
#requires -RunAsAdministrator
$ErrorActionPreference='Stop'
$repo=[IO.Path]::GetFullPath((Join-Path $PSScriptRoot '../..'))
. (Join-Path $repo 'deploy/windows/Offline-Tools.ps1')
. (Join-Path $repo 'deploy/windows/Offline-Upgrade.ps1')
$id=[guid]::NewGuid().ToString('N')
$fixture=Join-Path $repo ('artifacts/service-upgrade-'+$id)
$root=Join-Path $fixture 'installed'
$media=Join-Path $fixture 'media'
foreach($folder in $OfflineUpgradeFolders){
    New-Item -ItemType Directory -Path (Join-Path $root $folder) -Force|Out-Null
    [IO.File]::WriteAllText((Join-Path $root "$folder/retained.txt"),'old-'+$folder)
}
foreach($folder in @('app','scripts','docs','agent')){
    New-Item -ItemType Directory -Path (Join-Path $media "payload/$folder") -Force|Out-Null
    [IO.File]::WriteAllText((Join-Path $media "payload/$folder/current.txt"),'new-'+$folder)
}
$privateFile=Join-Path $root 'secrets/retained.txt'
$acl=Get-Acl -LiteralPath $privateFile
$acl.SetAccessRuleProtection($true,$true)
Set-Acl -LiteralPath $privateFile -AclObject $acl
$privateAcl=(Get-Acl -LiteralPath $privateFile).Sddl
$source=@'
using System;
using System.IO;
using System.ServiceProcess;
public sealed class UpgradeFixture : ServiceBase {
    private readonly string path;
    private FileStream held;
    public UpgradeFixture(string name,string file) { ServiceName=name; path=file; }
    protected override void OnStart(string[] args) { held=new FileStream(path,FileMode.OpenOrCreate,FileAccess.ReadWrite,FileShare.None); }
    protected override void OnStop() { held.Dispose(); }
    public static void Main(string[] args) { Run(new UpgradeFixture(args[0],args[1])); }
}
'@
$cs=Join-Path $fixture 'Service.cs'
$exe=Join-Path $root 'runtime/ServiceFixture.exe'
[IO.File]::WriteAllText($cs,$source)
$csc=Join-Path $env:WINDIR 'Microsoft.NET/Framework64/v4.0.30319/csc.exe'
& $csc /nologo /target:exe "/out:$exe" /reference:System.ServiceProcess.dll $cs
if($LASTEXITCODE){throw 'Service fixture compilation failed'}
$names=@('Db','Identity','App')|ForEach-Object {'KafdeckUpgradeTest-'+$id+'-'+$_}
try {
    $previous=$null
    foreach($name in $names){
        $file=Join-Path $root "state/$name.db"
        [IO.File]::WriteAllText($file,'retained-database')
        $parameters=@{Name=$name;BinaryPathName=('"'+$exe+'" '+$name+' "'+$file+'"');StartupType='Manual'}
        if($previous){$parameters.DependsOn=$previous}
        $null=New-Service @parameters
        Start-Service $name
        $previous=$name
    }
    $stop={
        foreach($name in @($names[2],$names[1],$names[0])){
            $service=Get-Service $name
            if($service.Status -ne 'Stopped'){Stop-Service $name;$service.WaitForStatus('Stopped',[TimeSpan]::FromSeconds(30))}
        }
    }
    $start={foreach($name in $names){Start-Service $name;(Get-Service $name).WaitForStatus('Running',[TimeSpan]::FromSeconds(30))}}
    $check={param($NewVersion) foreach($name in $names){if((Get-Service $name).Status -ne 'Running'){throw 'Service not running'}}}
    Write-UpgradeJournal (Join-Path $root 'config/offline-install.json') @{root=$root;packageHash='previous';origin='https://fixture.invalid';phase='complete'}
    [IO.File]::WriteAllText((Join-Path $media 'bundle-manifest.json'),'{"version":"1.0.0"}')
    $manifest=[pscustomobject]@{version='1.0.0'}
    Invoke-OfflineUpgradeTransaction $root $media $manifest $stop $start $check
    if(!(Test-Path (Join-Path $root 'app/current.txt'))){throw 'New application missing'}
    [IO.File]::WriteAllText((Join-Path $media 'bundle-manifest.json'),'{"version":"1.0.1"}')
    $manifest.version='1.0.1'
    $check={param($NewVersion) if($NewVersion){throw 'Injected failed health check'};foreach($name in $names){if((Get-Service $name).Status -ne 'Running'){throw 'Recovery service not running'}}}
    try {Invoke-OfflineUpgradeTransaction $root $media $manifest $stop $start $check;throw 'Expected upgrade failure'}
    catch {if($_.Exception.Message -notlike 'Upgrade failed and the original installation was recovered.*'){throw}}
    & $stop
    foreach($name in $names){if([IO.File]::ReadAllText((Join-Path $root "state/$name.db")) -ne 'retained-database'){throw 'Database bytes changed'}}
    if((Get-Content (Join-Path $root 'config/offline-install.json') -Raw|ConvertFrom-Json).version -ne '1.0.0'){throw 'Previous receipt was not restored'}
    if((Get-Acl -LiteralPath $privateFile).Sddl -ne $privateAcl){throw 'Private file ACL changed during recovery'}
    Write-Host 'PASS real SCM dependency-order stop/start, exclusive database handles, ACL backup and failed-update recovery.'
} finally {
    foreach($name in @($names[2],$names[1],$names[0])){
        if(Get-Service $name -ErrorAction SilentlyContinue){Stop-Service $name -ErrorAction SilentlyContinue;$null=& sc.exe delete $name}
    }
}
