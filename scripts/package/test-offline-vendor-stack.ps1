#requires -Version 5.1
param([string]$JavaHome)
$ErrorActionPreference='Stop'
$repo=[IO.Path]::GetFullPath((Join-Path $PSScriptRoot '../..'))
. (Join-Path $repo 'deploy/windows/Offline-Upgrade.ps1')
$fixture=Join-Path $repo ('artifacts/vendor-stack-tests-'+[guid]::NewGuid().ToString('N'))
foreach($folder in @('runtime/java','state/postgresql','runtime/keycloak/lib/lib/main')){
    New-Item -ItemType Directory -Path (Join-Path $fixture $folder) -Force|Out-Null
}
$java=Join-Path $fixture 'runtime/java/release'
$postgres=Join-Path $fixture 'state/postgresql/PG_VERSION'
$core=Join-Path $fixture 'runtime/keycloak/lib/lib/main/org.keycloak.keycloak-core-26.8.1.jar'
[IO.File]::WriteAllText($postgres,"17`r`n")
[IO.File]::WriteAllText($core,'fixture')
$checks=0
foreach($version in @('17','17.0.1','17.0.13','21','21.0.1','21.0.12','21.0.13','21.0.13.0.2','21.0.13.0.3','21.0.14','21.0.13+8-LTS','25','25.0.1')){
    [IO.File]::WriteAllText($java,("IMPLEMENTOR=`"Microsoft`"`r`nJAVA_VERSION=`"$version`"`r`nJAVA_RUNTIME_VERSION=`"$version+1-LTS`"`r`n"))
    Assert-OfflineUpgradeVendorStack $fixture
    $checks++
}
foreach($version in @('1.8.0_432','11.0.25','16.0.2','18','22.0.1','26','21oops','21.0.13-ea','21..1')){
    [IO.File]::WriteAllText($java,('JAVA_VERSION="'+$version+'"'))
    $failed=$false
    try{Assert-OfflineUpgradeVendorStack $fixture}catch{
        if($_.Exception.Message -notlike "Unsupported installed Java version '$version'.*"){throw}
        $failed=$true
    }
    if(!$failed){throw "Accepted unsupported Java $version"}
    $checks++
}
[IO.File]::WriteAllText($java,'JAVA_RUNTIME_VERSION="21.0.13"')
try{Assert-OfflineUpgradeVendorStack $fixture;throw 'Missing JAVA_VERSION accepted'}catch{
    if($_.Exception.Message -ne 'Cannot read the installed Java version from runtime/java/release.'){throw}
    $checks++
}
[IO.File]::WriteAllText($java,'JAVA_VERSION="21.0.13.0.2"')
[IO.File]::WriteAllText($postgres,'18')
try{Assert-OfflineUpgradeVendorStack $fixture;throw 'PostgreSQL 18 accepted'}catch{
    if($_.Exception.Message -notlike "Unsupported installed PostgreSQL data version '18'.*"){throw}
    $checks++
}
[IO.File]::WriteAllText($postgres,'17')
Move-Item -LiteralPath $core -Destination ($core+'.disabled')
try{Assert-OfflineUpgradeVendorStack $fixture;throw 'Missing Keycloak accepted'}catch{
    if($_.Exception.Message -notlike 'Installed Keycloak core 26.8.1 was not found*'){throw}
    $checks++
}
Move-Item -LiteralPath ($core+'.disabled') -Destination $core
if($JavaHome){
    Copy-Item -LiteralPath (Join-Path $JavaHome 'release') -Destination $java -Force
    Assert-OfflineUpgradeVendorStack $fixture
    $checks++
}
@{checks=$checks;windowsPowerShell=$PSVersionTable.PSVersion.ToString();realJavaReleaseTested=[bool]$JavaHome;fixture=$fixture;existingServicesChanged=$false}|ConvertTo-Json
