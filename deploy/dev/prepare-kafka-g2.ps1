param(
    [Parameter(Mandatory=$true)][string]$KafkaHome,
    [Parameter(Mandatory=$true)][string]$JavaHome,
    [string]$OutputDirectory = "$PSScriptRoot/.generated/g2/kafka/instance"
)
$ErrorActionPreference = 'Stop'
$runtime = [IO.Path]::GetFullPath($KafkaHome)
$instance = [IO.Path]::GetFullPath($OutputDirectory)
$java = Join-Path $JavaHome 'bin/java.exe'
if (!(Test-Path -LiteralPath $java)) { $java = Join-Path $JavaHome 'bin/java' }
if (!(Test-Path -LiteralPath $java) -or !(Test-Path -LiteralPath (Join-Path $runtime 'libs/kafka_2.13-4.3.1.jar'))) {
    throw 'Require Java 21 and the verified official Apache Kafka 4.3.1 binary distribution.'
}
if (Test-Path -LiteralPath $instance) { throw 'Select a fresh ignored instance directory; never reformat an existing broker.' }
New-Item -ItemType Directory -Path $instance -Force | Out-Null
$data = (Join-Path $instance 'data').Replace('\','/')
@"
process.roles=broker,controller
node.id=1
controller.quorum.bootstrap.servers=127.0.0.1:19093
listeners=PLAINTEXT://127.0.0.1:19092,CONTROLLER://127.0.0.1:19093
advertised.listeners=PLAINTEXT://127.0.0.1:19092,CONTROLLER://127.0.0.1:19093
controller.listener.names=CONTROLLER
inter.broker.listener.name=PLAINTEXT
listener.security.protocol.map=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT
log.dirs=$data
num.partitions=1
offsets.topic.replication.factor=1
share.coordinator.state.topic.replication.factor=1
share.coordinator.state.topic.min.isr=1
transaction.state.log.replication.factor=1
transaction.state.log.min.isr=1
auto.create.topics.enable=false
num.network.threads=2
num.io.threads=2
log.retention.hours=1
log.segment.bytes=16777216
"@ | Set-Content -LiteralPath (Join-Path $instance 'server.properties') -Encoding ascii
$id = [Convert]::ToBase64String([Guid]::NewGuid().ToByteArray()).TrimEnd('=').Replace('+','-').Replace('/','_')
& $java -Xms64m -Xmx256m ("-Dlog4j2.configurationFile=" + (Join-Path $runtime 'config/tools-log4j2.yaml')) -cp (Join-Path $runtime 'libs/*') kafka.tools.StorageTool format --standalone -t $id -c (Join-Path $instance 'server.properties')
if ($LASTEXITCODE -ne 0) { throw 'Kafka storage format failed; preserve this directory for diagnosis and choose a fresh directory for a retry.' }
@{
    kafkaVersion='4.3.1'; bootstrapServers='127.0.0.1:19092'
    kafkaHome=$runtime; configFile=(Join-Path $instance 'server.properties'); javaExecutable=$java
    sha512='c7d7b2318cb51aa0c61d3246a51c349210073c5c9b754947ef965a439f2f939e8600f204e134a75ac31faf3829c9370960ef7c6a9886c8a1dbf0339a21f4c54c'
} | ConvertTo-Json | Set-Content -LiteralPath (Join-Path $instance 'lab.json') -Encoding utf8
Write-Output 'Prepared an isolated single-node Kafka KRaft laboratory on numeric loopback ports 19092/19093.'
Write-Output 'This disposable plaintext loopback fixture does not establish production TLS/SASL, Kafka HA or OS support.'
