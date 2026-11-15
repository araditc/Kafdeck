# Monitoring agent authentication: CodeQL alert 9 triage

Reviewed on 2026-11-16 for [PR #479](https://github.com/araditc/Kafdeck/pull/479), rule `cs/user-controlled-bypass`, `KafkaMetricSources.Authenticate`.

## Control-flow finding

The request header determines whether malformed input is rejected early with `(null, Unauthorized())`. It cannot select an authenticated identity or suppress credential verification while continuing into ingestion. For a single bounded token the server hashes the candidate and selects a server-owned source only when `CryptographicOperations.FixedTimeEquals` matches its stored credential hash. A missing source returns another unauthorized result. Both mapped handlers return a non-null authentication error before parsing or writing any body. Only the returned server-owned source determines the ingestion cluster and instance.

The flagged conditional therefore skips the credential lookup only on the **denial** path. This is a false positive for an early rejection guard, not an accepted authentication risk. No authentication guard, TLS restriction, scan rule or credential check is removed or disabled.

## HTTP regression evidence

`KafkaMonitoringTests.Agent_header_validation_never_bypasses_authentication_or_parses_unauthenticated_bodies` executes both real local HTTP endpoints (`/monitoring/ingest` and `/monitoring/client-stats`), without a browser identity:

- Missing header, empty values, short token, oversized token, wrong bounded token and duplicated valid token all return 401, even with caller-controlled `authenticated=true&skipAuth=true` query hints.
- The bodies are deliberately malformed JSON. Receiving 401 rather than a parse error demonstrates rejection before body processing. The source metric catalog remains empty afterward.
- A valid enrolled token accepts a bounded ingestion batch with 204 and stores its metric only under the enrolled cluster.
- With that valid token, malformed client statistics return 400 from body parsing rather than bypassing validation or being confused with credential denial.
- The other cluster's metric catalog remains empty.

The two theory cases pass with zero skips (12 denied requests and two authenticated controls). Reproduce using the repository's normal restore/build conventions:

```sh
dotnet test tests/Kafdeck.Architecture.Tests/Kafdeck.Architecture.Tests.csproj --configuration Release --filter FullyQualifiedName~Agent_header_validation_never_bypasses
```

This is additive regression and review evidence. Runtime source and previously published preview packages are unchanged; the successful installation and other runtime tests are reused.
