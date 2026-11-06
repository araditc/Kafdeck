# W77 H1–H3 bounded hardening and provider evidence

Status: implemented producers; runtime and protected-main admission must be recorded from actual executions. This document is not a passed result, leak-free assertion, blanket provider guarantee, or W82 numerical budget admission. Preserve the released v0.8 identity and its complete existing regression producers.

## H1: fixed populated governance workload

`V09W77HardeningLiveTests` runs SQLite standalone and PostgreSQL separately, in a collection that disables parallel test execution. Each case owns one native Apache Kafka 4.3.1 broker, one shipped API child, a private data directory and (for PostgreSQL) a unique schema. It reuses the G2 genuine HTTPS Keycloak authorization-code/PKCE laboratory and shipped CLI. Each case creates eight governed teams, a real staged and distinctly approved active policy, an approved business request and its actual Kafka topic effect, a governed catalog assignment and an activated rate card. The existing `V09W76MeasuredTrafficFixture` supplies an actual >=61-second HTTPS body-byte counter interval with 98,304 controlled bytes and collection through the real measured-counter adapter. This meter is explicitly not Kafka billable traffic.

The fixed profile has ten warm samples, 300 load samples and ten idle samples, each at a nominal one-second interval. Each load tick issues four concurrent authenticated HTTP reads over populated catalog, request, policy and attribution collections. Every thirtieth tick additionally reads actual cluster metadata. There are 1,220 measured/warm requests: 1,200 governance reads, ten warm reads and ten metadata reads. Setup/denial/restart CLI calls are additional and reported separately. Before a load batch, the producer checks the genuine session's actual token expiry and reacquires through real human authentication when twenty seconds or less remain. The prepared G2 realm declares a 120-second lifetime; the producer uses observed expiry rather than a fixed refresh interval. Failed requests fail the run; retries never erase an observed workload failure.

Raw samples record API and testhost CPU seconds, RSS, private bytes, handles, threads and Linux file descriptors; unsupported platform file-descriptor measurement is explicitly null. SQLite database/WAL size is recorded where applicable. The evidence reports latency median/p95/p99/max, observed throughput, RSS slope, idle RSS/handle/thread deltas, exact sample/load counts and elapsed time. The actual CLI reads the retained record, kills/restarts its owned API child and proves the same durable revision after load.

Emergency fixture ceilings are 2 GiB RSS per measured process, 10,000 handles, 1,000 threads and fifteen minutes per H1 case, including genuine identity/governance/measurement setup and teardown. They stop runaway test infrastructure. They are not admitted product budgets. A local PostgreSQL trial reached the earlier twelve-minute case ceiling during the fixed sample delay, with no observed failed read; it remains a failed trial, not a PostgreSQL pass. Raising the fixture ceiling requires a complete rerun with the same 1,220 requests and 320 samples. This finite five-minute profile is not the W82 two-hour soak, enterprise reference dataset, HA topology or production SLA described in [the budget protocol](../performance/v0.9-budget-protocol.md). Raw growth and cleanup evidence must be reviewed; a finite sample cannot establish indefinite leak freedom.

## H2: isolated physical faults and recovery

`V09W77FaultLiveTests` executes each persistence provider with a six-minute outer deadline. It creates a fresh broker through the existing verified `prepare-kafka-g2.ps1`; its ports, PID, configuration, logs and storage are privately owned. It never stops the shared G2 broker or changes host firewall/storage permissions.

An initial Linux CI trial failed all four cases before scenario execution with the closed formatter code `storage_classpath_invalid`, after Java/JAR preflight and configuration creation succeeded. The formatter now passes its unchanged JVM arguments through `ProcessStartInfo.ArgumentList`, preserving `libs/*` as one literal argument for Java, with concurrent output draining and a bounded owned-child timeout. PowerShell documents [native wildcard expansion on Unix](https://learn.microsoft.com/en-us/powershell/scripting/whats-new/unix-support?view=powershell-7.5); its project also records [computed/splatted wildcard expansion](https://github.com/PowerShell/PowerShell/issues/24178). Wildcard expansion is the diagnosis inferred from the closed failure and documented behavior; successful formatter/scenario reruns are required before calling the fix verified.

| Physical fault | Required observation/recovery |
|---|---|
| Kill the owned native broker process | Fresh real adapter observes a bounded failure, then a restarted broker answers actual metadata; persisted governance remains intact. |
| Cut owned API TCP proxy connections | The same bearer-admitted catalog read fails through the cut proxy while the direct authenticated read succeeds; reconnection succeeds after resume. `/healthz` is not used as a bearer route. |
| Cut PostgreSQL TCP proxy connections | Actual authority read returns 503; restored transport returns all eight unchanged records and the original receipt. |
| Hold SQLite's actual writer lock | A genuine CLI mutation preview fails while storage is locked; rollback restores normal authority without an extra committed team. |
| Hold PostgreSQL's actual exclusive catalog table lock | The real connection command timeout produces 503; rollback restores the same records. |
| Kill/restart the owned API process | The original operation remains applied/verified and every retained team revision remains one. |

Storage contention is reported as contention, not disk-full, corrupt media, power loss or read-only-filesystem evidence. Single-node restart is not broker quorum or database replica failover. Existing PostgreSQL failover/restore and v0.8 unknown/recovery producers remain mandatory separately.

Both H1/H2 stop owned children, close proxy transfers, drop the unique PostgreSQL schema and clear fixture SQLite pools. Successful runs remove their privately created directories after checking the final absolute target stays within the specific owned root. Failed runs still stop owned children and transports; the private broker directory/console remains for diagnosis. Secret configuration, identity inputs and raw API/broker console logs stay private and are not uploaded. The JSON evidence is written only after successful teardown; missing evidence fails completeness.

## H3: actual provider/version cells

[The dedicated workflow](../../.github/workflows/v09-w77-provider-profiles.yml) independently launches each declared image. The test performs the actual shipped read-only administration adapter capability probe and typed create/config-alter/partition-increase/delete lifecycle, with independent readback and verified topic absence. Each cell retains its full seven-capability inventory, image content identity, repository digests when available, actual runtime version output, runtime/client version, TRX, closed operation outcomes and container cleanup evidence. It does not infer a broker version from a metadata response.

| Provider | Exact image versions | Executed profile |
|---|---|---|
| Apache Kafka | 4.3.1, 4.2.1, 4.1.2, 3.9.2 | Separate actual loopback/plaintext core topic lifecycle per version. |
| Confluent Platform | `confluentinc/cp-kafka:8.3.2` | Separate actual loopback/plaintext core profile; no Confluent Cloud inference. |
| Redpanda | `docker.redpanda.com/redpandadata/redpanda:v25.3.11` | Separate actual loopback/plaintext core profile; provider-specific APIs not claimed. |

The exact Confluent image family/configuration comes from [Confluent's Docker image documentation](https://docs.confluent.io/platform/current/installation/docker/image-reference.html) and [configuration reference](https://docs.confluent.io/platform/current/installation/docker/config-reference.html). The Redpanda tag/profile comes from [its versioned quickstart](https://docs.redpanda.com/streaming/25.3/get-started/quick-start/). Pinning a producer version is not a runtime result or recommendation to deploy that old patch in production.

The pre-existing [Kafka security compatibility workflow](../../.github/workflows/kafka-compatibility.yml) remains mandatory for the four Apache versions: plaintext, mutual TLS, SASL PLAIN/SCRAM over plaintext/TLS, typed mutations and real partial authorization. The new plaintext cells do not replace it. No TLS/SASL/ACL/SCRAM support is newly inferred for Confluent/Redpanda. AWS MSK, Confluent Cloud, Aiven, Google Managed Kafka, Azure Event Hubs and other provider-specific/administration features remain not claimed by these results; an unsupported or unavailable capability never becomes an empty successful view or a blanket Supported claim.

## Reproduction and completeness

Use existing genuine G2 identity/PostgreSQL fixtures and prebuilt Release binaries. The two runtime homes must reference the verified native Apache 4.3.1/Java 21 installations. The existing formatter rejects a different Kafka runtime. Set the exact source SHA independently from checkout, including local runs:

```powershell
$env:KAFDECK_RUN_W77 = '1'
$env:KAFDECK_SOURCE_SHA = git rev-parse HEAD
$env:KAFDECK_W77_KAFKA_HOME = '<verified kafka_2.13-4.3.1 directory>'
$env:KAFDECK_W77_JAVA_HOME = '<verified Java 21 directory>'
$env:KAFDECK_KEYCLOAK_G2_LAB = '<private G2 identity lab.json>'
$env:KAFDECK_G2_KAFKA_BOOTSTRAP = '127.0.0.1:19092'
$env:KAFDECK_TEST_POSTGRES = '<private disposable PostgreSQL connection string>'
$env:KAFDECK_G2_EVIDENCE_DIRECTORY = '<ignored evidence directory>'
dotnet test tests/Kafdeck.Architecture.Tests/Kafdeck.Architecture.Tests.csproj -c Release --no-build --no-restore --filter 'FullyQualifiedName~V09W77HardeningLiveTests|FullyQualifiedName~V09W77FaultLiveTests' --results-directory artifacts/w77 --logger 'trx;LogFileName=hardening.trx'
python scripts/ci/require-complete-trx.py artifacts/w77/hardening.trx V09W77HardeningLiveTests:2 V09W77FaultLiveTests:2
python scripts/ci/require-w77-evidence.py --source-sha $env:KAFDECK_SOURCE_SHA --hardening-dir $env:KAFDECK_G2_EVIDENCE_DIRECTORY --manifest artifacts/w77/hardening-manifest.json
```

The `KAFDECK_G2_KAFKA_BOOTSTRAP` opt-in dependency preserves genuine G2 readiness; H1/H2 nevertheless create their own brokers. Only explicit W77 opt-in executes these long tests. Missing prerequisites produce visible skips, and completeness rejects them. Do not expose private environment values in public evidence.

Linux provider reproduction after locked restore: `bash scripts/ci/w77-provider-profile.sh apache 4.3.1`, substituting each declared provider/version pair. Docker containers are restricted to owned loopback ports with 1 GiB/one-CPU fixture limits; readiness is bounded, the test has a two-minute deadline and cleanup removes only its recorded container. After downloading all six workflow artifacts, run `python scripts/ci/require-w77-evidence.py --source-sha <exact-checkout-sha> --profiles-root <download-directory> --manifest <output.json>`. Any missing/duplicate/foreign-source cell, failed/absent cleanup or missing runtime/image identity fails; TRX completeness is additionally enforced by each producer.

Admission requires actual H1/H2 evidence for both persistence providers, all six H3 cells, the unchanged Apache security matrix, relevant v0.8 regression, protected-main checks, exact source/environment metadata and artifact hashes. This ledger stays producer-only until those concrete executions are linked. Partial local evidence cannot close the entire workstream.
