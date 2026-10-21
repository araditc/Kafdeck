param(
    [ValidateSet('Start','Stop')][string]$Action='Start',
    [Parameter(Mandatory)][string]$JavaHome,
    [Parameter(Mandatory)][string]$KafkaHome,
    [Parameter(Mandatory)][string]$SchemaRegistryHome,
    [string]$BootstrapServers='127.0.0.1:19092',
    [string]$OutputDirectory="$PSScriptRoot/.generated/g2/providers",
    [ValidateRange(1024,65535)][int]$SchemaRegistryPort=18081,
    [ValidateRange(1024,65535)][int]$ConnectPort=18083
)
$ErrorActionPreference='Stop'
$directory=[IO.Path]::GetFullPath($OutputDirectory)
$metadataPath=Join-Path $directory 'provider-processes.json'
$java=Join-Path ([IO.Path]::GetFullPath($JavaHome)) ('bin/java'+$(if($IsWindows){'.exe'}else{''}))
if($Action -eq 'Stop') {
    if(!(Test-Path -LiteralPath $metadataPath)){throw 'Owned provider process metadata is required.'}
    $metadata=Get-Content -LiteralPath $metadataPath -Raw | ConvertFrom-Json
    foreach($entry in $metadata.processes){
        $process=Get-Process -Id $entry.processId -ErrorAction SilentlyContinue
        if($null -eq $process){continue}
        $started=([DateTimeOffset]$entry.startedAtUtc).UtcDateTime
        if($process.Path -cne $java -or $process.StartTime.ToUniversalTime().Ticks -ne $started.Ticks){throw 'Provider process identity changed; refusing to stop it.'}
        Stop-Process -Id $process.Id
    }
    Write-Output 'Stopped only recorded provider processes; all laboratory data remains.'
    exit
}
if(Test-Path -LiteralPath $directory){throw 'Use a fresh provider directory; existing process metadata and data are preserved.'}
if(!(Test-Path -LiteralPath $java)){throw 'An explicit Java 21 runtime is required.'}
if($BootstrapServers -notmatch '^127\.0\.0\.1:[0-9]{4,5}$'){throw 'This disposable launcher admits one numeric-loopback Kafka broker only.'}
foreach($port in @($SchemaRegistryPort,$ConnectPort)){
    $listener=[Net.Sockets.TcpListener]::new([Net.IPAddress]::Loopback,$port)
    try{$listener.Start()}finally{$listener.Stop()}
}
$kafka=[IO.Path]::GetFullPath($KafkaHome);$registry=[IO.Path]::GetFullPath($SchemaRegistryHome)
if(!(Test-Path -LiteralPath (Join-Path $kafka 'libs')) -or !(Test-Path -LiteralPath (Join-Path $registry 'share/java/schema-registry'))){throw 'Verified Kafka 4.3.1 and portable Schema Registry 8.3.2 runtimes are required.'}
New-Item -ItemType Directory -Path $directory | Out-Null
# The pinned Kafka 4.3.1 FileStream connector example is not discovered
# automatically. Load only its exact JAR through an isolated owned plugin
# directory; never allow a caller-controlled or generic plugin path.
$sourceFileConnector=Join-Path $kafka 'libs/connect-file-4.3.1.jar'
if(!(Test-Path -LiteralPath $sourceFileConnector -PathType Leaf)){throw 'Pinned Kafka 4.3.1 FileStream connector artifact is missing.'}
$pluginRoot=Join-Path $directory 'connect-plugins'
$pluginDirectory=Join-Path $pluginRoot 'filestream'
New-Item -ItemType Directory -Path $pluginDirectory -Force|Out-Null
Copy-Item -LiteralPath $sourceFileConnector -Destination (Join-Path $pluginDirectory 'connect-file-4.3.1.jar')
$pluginPath=$pluginRoot.Replace('\\','/')
$suffix=[Guid]::NewGuid().ToString('N')
$schemaConfig=Join-Path $directory 'schema-registry.properties'
@"
listeners=http://127.0.0.1:$SchemaRegistryPort
host.name=127.0.0.1
kafkastore.bootstrap.servers=PLAINTEXT://$BootstrapServers
kafkastore.topic=_schemas_g2_$suffix
kafkastore.topic.replication.factor=1
schema.compatibility.level=BACKWARD
mode.mutability=true
"@ | Set-Content -LiteralPath $schemaConfig -Encoding utf8NoBOM
$connectConfig=Join-Path $directory 'connect.properties'
@"
bootstrap.servers=$BootstrapServers
group.id=g2-connect-$suffix
plugin.path=$pluginPath
listeners=http://127.0.0.1:$ConnectPort
rest.advertised.host.name=127.0.0.1
rest.advertised.port=$ConnectPort
config.storage.topic=_g2_connect_config_$suffix
offset.storage.topic=_g2_connect_offsets_$suffix
status.storage.topic=_g2_connect_status_$suffix
config.storage.replication.factor=1
offset.storage.replication.factor=1
status.storage.replication.factor=1
offset.storage.partitions=1
status.storage.partitions=1
key.converter=org.apache.kafka.connect.storage.StringConverter
value.converter=org.apache.kafka.connect.storage.StringConverter
"@ | Set-Content -LiteralPath $connectConfig -Encoding utf8NoBOM
$separator=[IO.Path]::PathSeparator
$schemaClasspath=(@('confluent-security/schema-registry','schema-registry-plugins','confluent-common','confluent-telemetry','rest-utils','schema-registry') |
    ForEach-Object {Join-Path $registry "share/java/$_/*"}) -join $separator
$launches=@(
    @{name='schema';arguments=@('-Xms128m','-Xmx512m',"-Dschema-registry.log.dir=$directory", "-Dlog4j2.configurationFile=$(Join-Path $registry 'etc/schema-registry/log4j2.yaml')",'-cp',$schemaClasspath,'io.confluent.kafka.schemaregistry.rest.SchemaRegistryMain',$schemaConfig)},
    @{name='connect';arguments=@('-Xms128m','-Xmx512m',"-Dkafka.logs.dir=$directory",'-cp',(Join-Path $kafka 'libs/*'),'org.apache.kafka.connect.cli.ConnectDistributed',$connectConfig)}
)
$processes=@()
try{
    foreach($launch in $launches){
        # ProcessStartInfo.ArgumentList preserves Windows paths and avoids shell command interpolation.
        $start=[Diagnostics.ProcessStartInfo]::new($java)
        $start.UseShellExecute=$false;$start.CreateNoWindow=$true;$start.WorkingDirectory=$directory
        foreach($argument in $launch.arguments){$start.ArgumentList.Add($argument)}
        $process=[Diagnostics.Process]::Start($start)
        $processes+=@{name=$launch.name;processId=$process.Id;startedAtUtc=$process.StartTime.ToUniversalTime().ToString('O')}
    }
    @{formatVersion=1;schemaRegistryVersion='8.3.2';connectVersion='4.3.1';processes=$processes;
        schemaRegistryUrl="http://127.0.0.1:$SchemaRegistryPort/";connectUrl="http://127.0.0.1:$ConnectPort/"} |
        ConvertTo-Json -Depth 5 | Set-Content -LiteralPath $metadataPath -Encoding utf8NoBOM
    $ready=$false
    for($attempt=0;$attempt -lt 150;$attempt++){
        try{
            $schema=Invoke-WebRequest "http://127.0.0.1:$SchemaRegistryPort/subjects" -TimeoutSec 2
            $connect=Invoke-WebRequest "http://127.0.0.1:$ConnectPort/connectors" -TimeoutSec 2
            if($schema.StatusCode -eq 200 -and $connect.StatusCode -eq 200){$ready=$true;break}
        }catch{}
        Start-Sleep -Seconds 1
    }
    if(!$ready){throw 'Actual provider processes failed bounded readiness; preserve local logs for diagnosis.'}
    # Verify genuine worker plugin discovery before claiming its physical
    # connector-create and task-control scenarios are available.
    $loadedPlugins=@(Invoke-RestMethod "http://127.0.0.1:$ConnectPort/connector-plugins" -TimeoutSec 3)
    $fileSinks=@($loadedPlugins|Where-Object {
        $_.class -ceq 'org.apache.kafka.connect.file.FileStreamSinkConnector' -and $_.version -ceq '4.3.1'
    })
    if($fileSinks.Count -ne 1){throw 'Pinned Kafka FileStream sink is not discoverable by the actual Connect worker.'}
    Write-Output 'Actual Schema Registry and Connect are ready with the exact pinned FileStream sink plugin.'
}catch{
    foreach($entry in $processes){Stop-Process -Id $entry.processId -ErrorAction SilentlyContinue}
    throw
}
