#requires -Version 7.0
param(
    [Parameter(Mandatory=$true)][string]$ApiDll,
    [Parameter(Mandatory=$true)][string]$WorkingDirectory,
    [Parameter(Mandatory=$true)][string]$PrivateLogDirectory,
    [switch]$AdoptExisting,
    [string]$AdoptionEvidenceSha256,
    [switch]$AdmitGeneration,
    [Guid]$TargetGeneration=[Guid]::Empty,
    [string]$PreviousGeneration='',
    [string]$FenceEvidenceSha256
)
$ErrorActionPreference='Stop'
foreach($path in @($ApiDll,$WorkingDirectory,$PrivateLogDirectory)){
    if(![IO.Path]::IsPathFullyQualified($path)){throw 'Explicit absolute operator paths are required.'}
}
if(!(Test-Path -LiteralPath $ApiDll -PathType Leaf) -or !(Test-Path -LiteralPath $WorkingDirectory -PathType Container)){
    throw 'Select the reviewed API binary and its configured deployment directory.'
}
if($AdoptExisting -and $AdoptionEvidenceSha256 -cnotmatch '^[0-9a-f]{64}$'){
    throw 'Existing five-table adoption requires the reviewed backup/schema evidence SHA256.'
}
if($AdmitGeneration){
    if($AdoptExisting -or $TargetGeneration -eq [Guid]::Empty -or $FenceEvidenceSha256 -cnotmatch '^[0-9a-f]{64}$'){
        throw 'Generation admission is separate from schema adoption and requires an explicit target and fence evidence SHA256.'
    }
    if($PreviousGeneration){$parsedGeneration=[Guid]::Empty;if(![Guid]::TryParseExact($PreviousGeneration,'D',[ref]$parsedGeneration)){
        throw 'Previous generation must be canonical or empty for initial enrollment.'
    }}
}
if(Test-Path -LiteralPath $PrivateLogDirectory){throw 'Use a fresh private diagnostic directory; no logs are overwritten.'}
New-Item -ItemType Directory -Path $PrivateLogDirectory|Out-Null
if(!$IsWindows){& chmod 700 $PrivateLogDirectory;if($LASTEXITCODE){throw 'Private diagnostic directory permission failed.'}}
$previous=[Environment]::GetEnvironmentVariable('Kafdeck__Declarative__AdoptionEvidenceSha256')
try{
    if($AdoptExisting){$env:Kafdeck__Declarative__AdoptionEvidenceSha256=$AdoptionEvidenceSha256}
    $arguments=@(('"'+$ApiDll.Replace('"','\"')+'"'))
    if($AdmitGeneration){
        $arguments+='--declarative-generation-admit'
        $arguments+='--Kafdeck:Declarative:DeploymentGeneration='+$TargetGeneration.ToString('D')
        $arguments+='--Kafdeck:Declarative:PreviousDeploymentGeneration='+$PreviousGeneration
        $arguments+='--Kafdeck:Declarative:GenerationFenceEvidenceSha256='+$FenceEvidenceSha256
    }else{
        $arguments+='--declarative-schema-migrate'
        if($AdoptExisting){$arguments+='--declarative-adopt-existing'}
    }
    $settings=@{FilePath='dotnet';ArgumentList=$arguments;WorkingDirectory=$WorkingDirectory;PassThru=$true;
        RedirectStandardOutput=(Join-Path $PrivateLogDirectory 'migration-private.log');
        RedirectStandardError=(Join-Path $PrivateLogDirectory 'migration-private-error.log')}
    if($IsWindows){$settings.WindowStyle='Hidden'}
    $process=Start-Process @settings
    try{
        if(!$process.WaitForExit(300000)){$process.Kill($true);throw 'Schema operation exceeded its finite deadline; verify transaction outcome before retrying.'}
        if($process.ExitCode -ne 0){throw 'Schema operation refused; inspect only the retained private operator diagnostics.'}
    }finally{$process.Dispose()}
    Write-Output 'W68_SCHEMA_OPERATION verified=true; httpStarted=false; workersStarted=false'
}finally{[Environment]::SetEnvironmentVariable('Kafdeck__Declarative__AdoptionEvidenceSha256',$previous)}
