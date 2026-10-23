using System.Diagnostics;
using System.Net;
using System.Net.Security;
using System.Reflection;
using System.Security.Cryptography.X509Certificates;
using System.Text.Json;
using System.Text.RegularExpressions;
using Kafdeck.Cli;
using Kafdeck.Core.Kafka;
using Kafdeck.Infrastructure.Configuration;
using Kafdeck.Infrastructure.Kafka;
using Kafdeck.Infrastructure.Persistence;
using Kafdeck.Modules.Administration;
using Microsoft.AspNetCore.WebUtilities;
using Xunit;
using Xunit.Abstractions;

namespace Kafdeck.Architecture.Tests;

public sealed class KeycloakG2FactAttribute : FactAttribute
{
    public KeycloakG2FactAttribute()
    {
        if (string.IsNullOrWhiteSpace(Environment.GetEnvironmentVariable("KAFDECK_KEYCLOAK_G2_LAB")))
            Skip = "Requires the isolated, genuine Keycloak G2 HTTPS laboratory; no imitation access tokens are admitted.";
    }
}

public sealed class KeycloakG2KafkaFactAttribute : FactAttribute
{
    public KeycloakG2KafkaFactAttribute()
    {
        if (string.IsNullOrWhiteSpace(Environment.GetEnvironmentVariable("KAFDECK_KEYCLOAK_G2_LAB")) ||
            string.IsNullOrWhiteSpace(Environment.GetEnvironmentVariable("KAFDECK_G2_KAFKA_BOOTSTRAP")))
            Skip = "Requires genuine Keycloak G2 and a disposable real Kafka broker; no synthetic effect acceptance.";
    }
}

public sealed class KeycloakG2CriticalKafkaFactAttribute : FactAttribute
{
    public KeycloakG2CriticalKafkaFactAttribute()
    {
        if (string.IsNullOrWhiteSpace(Environment.GetEnvironmentVariable("KAFDECK_KEYCLOAK_G2_LAB")) ||
            string.IsNullOrWhiteSpace(Environment.GetEnvironmentVariable("KAFDECK_G2_KAFKA_BOOTSTRAP")) ||
            Environment.GetEnvironmentVariable("KAFDECK_G2_KAFKA_DELETE_CAPABLE") != "1")
            Skip = "Requires real Keycloak and an explicitly disposable delete-capable Kafka fixture. Linux CI requires this proof; native Windows Kafka log-directory deletion is not an admitted fixture.";
    }
}

public sealed class KeycloakG2PostgreSqlKafkaFactAttribute : FactAttribute
{
    public KeycloakG2PostgreSqlKafkaFactAttribute()
    {
        if (string.IsNullOrWhiteSpace(Environment.GetEnvironmentVariable("KAFDECK_KEYCLOAK_G2_LAB")) ||
            string.IsNullOrWhiteSpace(Environment.GetEnvironmentVariable("KAFDECK_G2_KAFKA_BOOTSTRAP")) ||
            string.IsNullOrWhiteSpace(Environment.GetEnvironmentVariable("KAFDECK_TEST_POSTGRES")))
            Skip = "Requires genuine Keycloak, Kafka and PostgreSQL for an actual shipped CLI/API/provider effect.";
    }
}

public sealed class V08G2HumanCliLiveTests(ITestOutputHelper testOutput)
{
    [KeycloakG2Fact]
    public Task Real_human_authorization_code_PKCE_reaches_shipped_CLI_and_API_with_scope_denial() => RunScenarioAsync(false);

    [KeycloakG2KafkaFact]
    public Task Real_Keycloak_human_CLI_previews_confirms_executes_real_Kafka_topic_and_reads_durable_status() => RunScenarioAsync(true);

    [KeycloakG2PostgreSqlKafkaFact]
    public Task Real_PostgreSql_governed_operator_subscription_and_human_CLI_Kafka_product_notification_reaches_TLS() => RunScenarioAsync(true, postgres: true);

    [KeycloakG2KafkaFact]
    public Task Real_Keycloak_service_CLI_previews_confirms_executes_real_Kafka_topic_with_distinct_durable_actor() => RunScenarioAsync(true, service: true);

    [KeycloakG2CriticalKafkaFact]
    public Task Real_human_CLI_CRITICAL_topic_delete_requires_distinct_registered_human_approval() => RunScenarioAsync(true, critical: true);

    [KeycloakG2KafkaFact]
    public Task Real_human_CLI_alters_and_increases_real_Kafka_topic_and_preserves_cancel_reject_outcomes() => RunScenarioAsync(true, extendedTopics: true);

    [KeycloakG2PostgreSqlKafkaFact]
    public Task Real_PostgreSql_API_and_human_CLI_preserve_actual_Kafka_effects_and_durable_cancel_reject_outcomes() => RunScenarioAsync(true, extendedTopics: true, postgres: true);

    private async Task RunScenarioAsync(bool includeEffect, bool service = false, bool critical = false, bool extendedTopics = false, bool postgres = false)
    {
        var labFile = Environment.GetEnvironmentVariable("KAFDECK_KEYCLOAK_G2_LAB")!;
        using var lab = JsonDocument.Parse(await File.ReadAllTextAsync(labFile));
        string Field(string name) => lab.RootElement.GetProperty(name).GetString()!;
        var evidenceDirectory = Environment.GetEnvironmentVariable("KAFDECK_G2_EVIDENCE_DIRECTORY") ?? Path.GetDirectoryName(labFile)!;
        Directory.CreateDirectory(evidenceDirectory);
        var subject = Field(service ? "subject" : "humanSubject");
        var clientId = Field(service ? "clientId" : "cliClientId");
        var registration = service ? "g2-service" : "g2-human";
        var registrationCollection = service ? "Registrations" : "HumanRegistrations";
        var actorKind = service ? "oidc-service" : "oidc-cli-human";
        var publicProfile = Path.Combine(evidenceDirectory, service ? "cli-service-profile.json" : "cli-public-profile.json");
        object profile = service ? new
        {
            issuer = Field("issuer"), audience = Field("audience"), clientId, expectedSubject = subject,
            privateKeyFile = Field("clientPrivateKeyFile"), trustedCaPemFile = Field("caPemFile"),
        } : new
        {
            issuer = Field("issuer"), audience = Field("audience"), clientId, trustedCaPemFile = Field("caPemFile"),
        };
        await File.WriteAllTextAsync(publicProfile, JsonSerializer.Serialize(profile));
        var root = FindRoot();
        var build = typeof(V08G2HumanCliLiveTests).Assembly.GetCustomAttribute<AssemblyConfigurationAttribute>()?.Configuration ?? "Debug";
        var apiDll = Path.Combine(root, "src/backend/Kafdeck.Api/bin", build, "net10.0/Kafdeck.Api.dll");
        using var portReservation = new System.Net.Sockets.TcpListener(IPAddress.Loopback, 0);
        portReservation.Start();
        var apiPort = ((IPEndPoint)portReservation.LocalEndpoint).Port;
        portReservation.Stop();
        var apiBase = $"http://127.0.0.1:{apiPort}";
        var topicName = "g2-cli-proof-" + Guid.NewGuid().ToString("N");
        var mutationDb = Path.Combine(evidenceDirectory, "mutation-" + Guid.NewGuid().ToString("N") + ".db");
        await using var postgresDatabase = postgres ? await G2CliPostgreSqlDatabase.CreateAsync() : null;
        var start = new ProcessStartInfo("dotnet")
        {
            WorkingDirectory = Path.GetDirectoryName(apiDll)!, UseShellExecute = false, CreateNoWindow = true,
            RedirectStandardOutput = true, RedirectStandardError = true,
        };
        start.ArgumentList.Add(apiDll);
        var configuration = new Dictionary<string, string>
        {
            ["Kafdeck:Deployment:ListenUrls:0"] = apiBase,
            ["Kafdeck:NonBrowserRead:Enabled"] = "true",
            ["Kafdeck:NonBrowserRead:Issuer"] = Field("issuer"),
            ["Kafdeck:NonBrowserRead:ApiAudience"] = Field("audience"),
            ["Kafdeck:NonBrowserRead:TrustedCaPemFile"] = Field("caPemFile"),
            [$"Kafdeck:NonBrowserRead:{registrationCollection}:0:Id"] = registration,
            [$"Kafdeck:NonBrowserRead:{registrationCollection}:0:OwnerId"] = "local-g2-owner",
            [$"Kafdeck:NonBrowserRead:{registrationCollection}:0:Subject"] = subject,
            [$"Kafdeck:NonBrowserRead:{registrationCollection}:0:ClientId"] = clientId,
            [$"Kafdeck:NonBrowserRead:{registrationCollection}:0:Enabled"] = "true",
            [$"Kafdeck:NonBrowserRead:{registrationCollection}:0:ExpiresAtUtc"] = DateTimeOffset.UtcNow.AddMinutes(10).ToString("O"),
            [$"Kafdeck:NonBrowserRead:{registrationCollection}:0:ReadActions:0"] = "SystemRead",
            [$"Kafdeck:NonBrowserRead:{registrationCollection}:0:ReadActions:1"] = "ClusterRead",
            ["Kafdeck:Authorization:Roles:0:Id"] = "g2-human-reader",
            ["Kafdeck:Authorization:Roles:0:Permissions:0:Action"] = "SystemRead",
            ["Kafdeck:Authorization:Roles:0:Permissions:1:Action"] = "ClusterRead",
            ["Kafdeck:Authorization:Roles:0:Permissions:1:ClusterIds:0"] = "permitted",
            ["Kafdeck:Authorization:SubjectBindings:0:Issuer"] = Field("issuer"),
            ["Kafdeck:Authorization:SubjectBindings:0:Subject"] = subject,
            ["Kafdeck:Authorization:SubjectBindings:0:RoleIds:0"] = "g2-human-reader",
        };
        if (includeEffect)
        {
            configuration["Kafdeck:Deployment:AccessMode"] = "Oidc";
            configuration["Kafdeck:Deployment:Oidc:Issuer"] = Field("issuer");
            configuration["Kafdeck:Deployment:Oidc:ClientId"] = "g2-browser-lab";
            configuration["Kafdeck:Deployment:Oidc:Scopes:0"] = "openid";
            configuration["Kafdeck:Clusters:0:Id"] = "permitted";
            configuration["Kafdeck:Clusters:0:BootstrapServers:0"] = Environment.GetEnvironmentVariable("KAFDECK_G2_KAFKA_BOOTSTRAP")!;
            configuration["Kafdeck:Clusters:0:SecurityProtocol"] = "Plaintext";
            configuration["Kafdeck:Administration:Mutations:Enabled"] = "true";
            configuration["Kafdeck:Administration:Mutations:MaterialDigestKey"] = "env:KAFDECK_G2_FIXTURE_DIGEST_KEY";
            configuration["Kafdeck:Administration:Mutations:Persistence:Provider"] = "Sqlite";
            configuration["Kafdeck:Administration:Mutations:Persistence:ExecutionMode"] = "Standalone";
            configuration["Kafdeck:Administration:Mutations:Persistence:SqliteDatabasePath"] = mutationDb;
            configuration["Kafdeck:NonBrowserRead:GovernedMutationsEnabled"] = "true";
            configuration[$"Kafdeck:NonBrowserRead:{registrationCollection}:0:MutationActions:0"] = "TopicCreate";
            configuration[$"Kafdeck:NonBrowserRead:{registrationCollection}:0:ReadActions:2"] = "TopicRead";
            foreach (var (index, action) in new[] { (2, "TopicCreate"), (3, "TopicRead") })
            {
                configuration[$"Kafdeck:Authorization:Roles:0:Permissions:{index}:Action"] = action;
                configuration[$"Kafdeck:Authorization:Roles:0:Permissions:{index}:ClusterIds:0"] = "permitted";
                configuration[$"Kafdeck:Authorization:Roles:0:Permissions:{index}:ResourcePatterns:0"] = topicName;
            }
            start.Environment["KAFDECK_G2_FIXTURE_DIGEST_KEY"] = Convert.ToHexString(System.Security.Cryptography.RandomNumberGenerator.GetBytes(32));
        }
        if (postgresDatabase is not null)
        {
            configuration["Kafdeck:Administration:Mutations:Persistence:Provider"] = "PostgreSql";
            configuration.Remove("Kafdeck:Administration:Mutations:Persistence:SqliteDatabasePath");
            configuration["Kafdeck:Administration:Mutations:Persistence:ConnectionString"] = "env:KAFDECK_G2_CLI_DATABASE_CONNECTION";
            start.Environment["KAFDECK_G2_CLI_DATABASE_CONNECTION"] = postgresDatabase.ConnectionString;
        }
        if (critical || extendedTopics)
        {
            configuration["Kafdeck:NonBrowserRead:GovernedApprovalsEnabled"] = "true";
            configuration["Kafdeck:NonBrowserRead:HumanRegistrations:0:MutationActions:1"] = "TopicDelete";
            configuration["Kafdeck:NonBrowserRead:HumanRegistrations:0:ApprovalActions:0"] = "TopicDelete";
            foreach (var (index, action) in new[] { (4, "TopicDelete"), (5, "MutationApprove") })
            {
                configuration[$"Kafdeck:Authorization:Roles:0:Permissions:{index}:Action"] = action;
                configuration[$"Kafdeck:Authorization:Roles:0:Permissions:{index}:ClusterIds:0"] = "permitted";
                configuration[$"Kafdeck:Authorization:Roles:0:Permissions:{index}:ResourcePatterns:0"] = topicName;
            }
            configuration["Kafdeck:NonBrowserRead:HumanRegistrations:1:Id"] = "g2-approver";
            configuration["Kafdeck:NonBrowserRead:HumanRegistrations:1:OwnerId"] = "local-g2-owner";
            configuration["Kafdeck:NonBrowserRead:HumanRegistrations:1:Subject"] = Field("approverSubject");
            configuration["Kafdeck:NonBrowserRead:HumanRegistrations:1:ClientId"] = Field("cliClientId");
            configuration["Kafdeck:NonBrowserRead:HumanRegistrations:1:Enabled"] = "true";
            configuration["Kafdeck:NonBrowserRead:HumanRegistrations:1:ExpiresAtUtc"] = DateTimeOffset.UtcNow.AddMinutes(10).ToString("O");
            configuration["Kafdeck:NonBrowserRead:HumanRegistrations:1:ReadActions:0"] = "SystemRead";
            configuration["Kafdeck:NonBrowserRead:HumanRegistrations:1:ApprovalActions:0"] = "TopicDelete";
            configuration["Kafdeck:Authorization:Roles:1:Id"] = "g2-independent-approver";
            foreach (var (index, action) in new[] { (0, "TopicDelete"), (1, "MutationApprove") })
            {
                configuration[$"Kafdeck:Authorization:Roles:1:Permissions:{index}:Action"] = action;
                configuration[$"Kafdeck:Authorization:Roles:1:Permissions:{index}:ClusterIds:0"] = "permitted";
                configuration[$"Kafdeck:Authorization:Roles:1:Permissions:{index}:ResourcePatterns:0"] = topicName;
            }
            configuration["Kafdeck:Authorization:SubjectBindings:1:Issuer"] = Field("issuer");
            configuration["Kafdeck:Authorization:SubjectBindings:1:Subject"] = Field("approverSubject");
            configuration["Kafdeck:Authorization:SubjectBindings:1:RoleIds:0"] = "g2-independent-approver";
        }
        if (extendedTopics)
        {
            configuration["Kafdeck:NonBrowserRead:HumanRegistrations:0:MutationActions:2"] = "TopicAlter";
            configuration["Kafdeck:Authorization:Roles:0:Permissions:6:Action"] = "TopicAlter";
            configuration["Kafdeck:Authorization:Roles:0:Permissions:6:ClusterIds:0"] = "permitted";
            configuration["Kafdeck:Authorization:Roles:0:Permissions:6:ResourcePatterns:0"] = topicName;
        }
        await using var productBridge = includeEffect && !service && !critical && !extendedTopics
            ? await G2KafkaProductNotificationBridge.PrepareAsync(evidenceDirectory, configuration, postgresDatabase?.ConnectionString) : null;
        foreach (var setting in configuration)
            start.Environment[setting.Key.Replace(":", "__", StringComparison.Ordinal)] = setting.Value;
        var host = Process.Start(start)!;
        var stdout = host.StandardOutput.ReadToEndAsync();
        var stderr = host.StandardError.ReadToEndAsync();
        var completedHostLogs = "";
        var password = await File.ReadAllTextAsync(Field("humanPasswordFile"));
        var approverPassword = critical || extendedTopics ? await File.ReadAllTextAsync(Field("approverPasswordFile")) : null;
        var asApprover = false;
        var browserLogins = 0;
        var browserNavigations = 0;
        var callbackPorts = new HashSet<int>();
        var transcripts = new List<string>();
        var browserMode = "normal";
        var rejectedCallbacks = 0;
        using var deadline = new CancellationTokenSource(TimeSpan.FromMinutes(extendedTopics ? 5 : 3));
        var lastOutput = "";
        try
        {
            using var api = new HttpClient { BaseAddress = new Uri(apiBase), Timeout = TimeSpan.FromSeconds(3) };
            var ready = false;
            for (var attempt = 0; attempt < 100 && !host.HasExited; attempt++)
            {
                try { using var response = await api.GetAsync("/healthz", deadline.Token); ready = response.IsSuccessStatusCode; if (ready) break; }
                catch (HttpRequestException) { }
                catch (OperationCanceledException) when (!deadline.IsCancellationRequested) { }
                await Task.Delay(200, deadline.Token);
            }
            Assert.True(ready, "The shipped API did not become ready for genuine CLI acceptance.");
            if (productBridge is not null)
                await productBridge.ConfigureViaOperatorApiAsync(new Uri(apiBase), lab.RootElement, deadline.Token);

            // This bounded browser-form driver follows the real authorization endpoint
            // and login form. It never uses direct-access/password grant or makes JWTs.
            async Task OpenBrowser(Uri authorization, CancellationToken cancellationToken)
            {
                browserNavigations++;
                var query = QueryHelpers.ParseQuery(authorization.Query);
                Assert.Equal("code", query["response_type"].ToString());
                Assert.Equal("S256", query["code_challenge_method"].ToString());
                Assert.Equal("openid", query["scope"].ToString());
                var redirect = new Uri(query["redirect_uri"].ToString());
                Assert.Equal("127.0.0.1", redirect.Host);
                Assert.Equal("/oauth/callback", redirect.AbsolutePath);
                Assert.InRange(redirect.Port, 1024, 65535);
                callbackPorts.Add(redirect.Port);
                if (browserMode is "nonce" or "pkce")
                {
                    var changed = query.ToDictionary(p => p.Key, p => p.Value.ToString(), StringComparer.Ordinal);
                    changed[browserMode == "nonce" ? "nonce" : "code_challenge"] = new string('A', 43);
                    authorization = new Uri(QueryHelpers.AddQueryString(authorization.GetLeftPart(UriPartial.Path), changed!));
                }
                using var ca = X509Certificate2.CreateFromPem(await File.ReadAllTextAsync(Field("caPemFile"), cancellationToken));
                using var handler = new SocketsHttpHandler
                {
                    AllowAutoRedirect = false, UseProxy = false, CookieContainer = new CookieContainer(),
                    SslOptions = new SslClientAuthenticationOptions
                    {
                        CertificateChainPolicy = new X509ChainPolicy
                        {
                            TrustMode = X509ChainTrustMode.CustomRootTrust, RevocationMode = X509RevocationMode.NoCheck, CustomTrustStore = { ca },
                        },
                    },
                };
                using var browser = new HttpClient(handler) { Timeout = TimeSpan.FromSeconds(15) };
                using var login = await browser.GetAsync(authorization, cancellationToken);
                Assert.True(login.IsSuccessStatusCode, "Keycloak rejected the real authorization-code request.");
                var html = await login.Content.ReadAsStringAsync(cancellationToken);
                Assert.InRange(html.Length, 1, 256 * 1024);
                var action = Regex.Match(html, "<form\\b[^>]*\\baction=\"([^\"]+)\"", RegexOptions.CultureInvariant, TimeSpan.FromSeconds(1));
                Assert.True(action.Success, "The pinned Keycloak login form was not found.");
                var form = new Uri(WebUtility.HtmlDecode(action.Groups[1].Value));
                Assert.Equal(new Uri(Field("issuer")).GetLeftPart(UriPartial.Authority), form.GetLeftPart(UriPartial.Authority));
                using var submitted = await browser.PostAsync(form, new FormUrlEncodedContent(new Dictionary<string, string>
                {
                    ["username"] = Field(asApprover ? "approverUsername" : "humanUsername"),
                    ["password"] = asApprover ? approverPassword! : password, ["credentialId"] = "",
                }), cancellationToken);
                Assert.Equal(HttpStatusCode.Found, submitted.StatusCode);
                var location = submitted.Headers.Location;
                Assert.NotNull(location);
                Assert.True(location.GetLeftPart(UriPartial.Path) == redirect.AbsoluteUri, "Keycloak returned an unexpected callback target.");
                var returned = QueryHelpers.ParseQuery(location.Query);
                Assert.True(returned.ContainsKey("code") && returned["state"].ToString() == query["state"].ToString(), "Keycloak did not return the bound code/state.");
                if (browserMode == "normal")
                {
                    var badState = new Uri(QueryHelpers.AddQueryString(redirect.AbsoluteUri, new Dictionary<string, string?>
                    {
                        ["code"] = "rejected-code-sentinel", ["state"] = "wrong-state-sentinel",
                    }));
                    using var rejected = await browser.GetAsync(badState, cancellationToken);
                    Assert.Equal(HttpStatusCode.BadRequest, rejected.StatusCode);
                    rejectedCallbacks++;
                    using var wrongPath = await browser.GetAsync(new Uri(redirect.GetLeftPart(UriPartial.Authority) + "/wrong" + location.Query), cancellationToken);
                    Assert.Equal(HttpStatusCode.BadRequest, wrongPath.StatusCode);
                    rejectedCallbacks++;
                }
                using var accepted = await browser.GetAsync(location, cancellationToken);
                Assert.True(accepted.IsSuccessStatusCode, "The already-bound CLI callback listener rejected the real Keycloak response.");
                browserLogins++;
            }

            async Task<int> Run(params string[] command)
            {
                using var output = new StringWriter();
                using var error = new StringWriter();
                var args = new[] { "--url", apiBase, service ? "--service-profile" : "--oidc-profile", publicProfile }.Concat(command).ToArray();
                var result = await CliApplication.RunAsync(args, output, error, deadline.Token, OpenBrowser,
                    stage => testOutput.WriteLine($"OIDC_FAILURE_STAGE={stage}"));
                var transcript = output.ToString() + error;
                Assert.False(transcript.Contains(password, StringComparison.Ordinal), "CLI output reflected the disposable login password.");
                if (approverPassword is not null) Assert.False(transcript.Contains(approverPassword, StringComparison.Ordinal),
                    "CLI output reflected the disposable approver password.");
                // Usage text legitimately names KAFDECK_ACCESS_TOKEN. Match
                // serialized credential fields/assignments, not that public
                // environment-variable name; JWT material remains forbidden.
                Assert.False(new[] { "\"access_token\"", "\"id_token\"", "\"code_verifier\"",
                    "access_token=", "id_token=", "code_verifier=", "Bearer eyJ" }.Any(marker =>
                    transcript.Contains(marker, StringComparison.OrdinalIgnoreCase)), "CLI output exposed a protected OIDC field.");
                transcripts.Add(transcript);
                lastOutput = output.ToString();
                testOutput.WriteLine($"REAL_CLI mode={browserMode}; exit={result}; browserNavigations={browserNavigations}; completedCallbacks={browserLogins}; invalidCallbacksRejected={rejectedCallbacks}");
                if (result == CliApplication.Success) using (JsonDocument.Parse(output.ToString())) { }
                return result;
            }

            async Task RestartApi()
            {
                host.Kill(entireProcessTree: true);
                await host.WaitForExitAsync(deadline.Token);
                var previousLogs = await stdout + await stderr;
                Assert.False(previousLogs.Contains(password, StringComparison.Ordinal), "The previous API log exposed the lab password.");
                if (approverPassword is not null) Assert.False(previousLogs.Contains(approverPassword, StringComparison.Ordinal),
                    "The previous API log exposed the approver password.");
                Assert.False(previousLogs.Contains("Bearer eyJ", StringComparison.Ordinal), "The previous API log exposed JWT material.");
                completedHostLogs += previousLogs;
                host.Dispose();
                host = Process.Start(start)!;
                stdout = host.StandardOutput.ReadToEndAsync();
                stderr = host.StandardError.ReadToEndAsync();
                var restarted = false;
                for (var attempt = 0; attempt < 100 && !host.HasExited; attempt++)
                {
                    try { using var response = await api.GetAsync("/healthz", deadline.Token); restarted = response.IsSuccessStatusCode; if (restarted) break; }
                    catch (HttpRequestException) { }
                    catch (OperationCanceledException) when (!deadline.IsCancellationRequested) { }
                    await Task.Delay(200, deadline.Token);
                }
                Assert.True(restarted, "The actual API did not become ready after the preview-preserving process restart.");
            }

            Assert.Equal(CliApplication.Success, await Run("system", "info"));
            Assert.Equal(CliApplication.AuthenticationOrAuthorizationError, await Run("clusters", "get", "denied"));
            if (includeEffect)
            {
                var idempotency = "g2-create-" + Guid.NewGuid().ToString("N");
                var createRequest = new[] { "topics", "create-preview", "permitted", topicName, "1", "1", "--idempotency-key", idempotency };
                if (extendedTopics) createRequest = createRequest.Concat(["--config", "segment.bytes=1048576"]).ToArray();
                Assert.Equal(CliApplication.Success, await Run(createRequest));
                using var preview = JsonDocument.Parse(lastOutput);
                var operationId = preview.RootElement.GetProperty("operationId").GetGuid();
                var previewHash = preview.RootElement.GetProperty("previewHash").GetString()!;
                var challenge = preview.RootElement.GetProperty("confirmationChallenge").GetString();
                Assert.False(preview.RootElement.GetProperty("requiresIndependentApproval").GetBoolean());
                Assert.Equal(CliApplication.Success, await Run(createRequest));
                using (var duplicate = JsonDocument.Parse(lastOutput)) Assert.Equal(operationId, duplicate.RootElement.GetProperty("operationId").GetGuid());
                var changedCreateRequest = (string[])createRequest.Clone();
                changedCreateRequest[4] = "2";
                Assert.Equal(CliApplication.Conflict, await Run(changedCreateRequest));
                Assert.Equal(CliApplication.Conflict, await Run("operations", "confirm", operationId.ToString("D"), new string('a', 64)));
                var confirm = new[] { "operations", "confirm", operationId.ToString("D"), previewHash };
                if (challenge is not null) confirm = confirm.Concat(new[] { "--challenge", challenge }).ToArray();
                Assert.Equal(CliApplication.Success, await Run(confirm));
                Assert.Equal(CliApplication.Success, await Run("operations", "execute", operationId.ToString("D")));
                using (var executed = JsonDocument.Parse(lastOutput)) Assert.Equal("appliedVerified", executed.RootElement.GetProperty("state").GetString());
                Assert.Equal(CliApplication.Success, await Run("operations", "status", operationId.ToString("D")));
                using (var status = JsonDocument.Parse(lastOutput)) Assert.Equal("appliedVerified", status.RootElement.GetProperty("state").GetString());
                if (service)
                {
                    // The registered service has current exact TopicRead scope.
                    // Keep the independent physical Kafka readback as well.
                    Assert.Equal(CliApplication.Success, await Run("topics", "get", "permitted", topicName));
                    Assert.Contains(topicName, lastOutput, StringComparison.Ordinal);
                    using var reads = new ConfluentKafkaAdministrationAdapter([
                        new ClusterProfile("permitted", [Environment.GetEnvironmentVariable("KAFDECK_G2_KAFKA_BOOTSTRAP")!],
                            KafkaSecurityProtocol.Plaintext, null, null)], new SecretResolver());
                    var metadata = await reads.GetTopicMetadataAsync("permitted", topicName,
                        new KafkaOperationContext(DateTimeOffset.UtcNow.AddSeconds(10)), deadline.Token);
                    Assert.True(metadata.IsSuccess);
                    Assert.NotNull(metadata.Value);
                    Assert.Single(metadata.Value.Partitions);
                }
                else
                {
                    Assert.Equal(CliApplication.Success, await Run("topics", "get", "permitted", topicName));
                    Assert.Contains(topicName, lastOutput, StringComparison.Ordinal);
                }
                // Reopen the production repository independently of the HTTP host.
                var reopened = new AdoMutationOperationRepository(postgresDatabase is null
                    ? new SqliteMutationDbConnectionFactory(mutationDb)
                    : new PostgreSqlMutationDbConnectionFactory(postgresDatabase.ConnectionString));
                var persisted = await reopened.GetAsync(operationId, deadline.Token);
                Assert.NotNull(persisted);
                Assert.Equal(MutationOperationState.AppliedVerified, persisted.State);
                Assert.Equal(actorKind + ":" + Uri.EscapeDataString(Field("issuer")) + "|" +
                    Uri.EscapeDataString(subject) + "|" + Uri.EscapeDataString(clientId) + "|" + registration, persisted.RequesterPrincipalId);
                Assert.StartsWith("v0.8-bearer:", persisted.PolicyVersion, StringComparison.Ordinal);
                Assert.Equal(previewHash, persisted.PreviewHash);
                if (service) Assert.Equal(0, browserNavigations);
                if (extendedTopics)
                {
                    await G2ExtendedTopicCliScenario.RunAsync(command => Run(command), () => lastOutput,
                        value => asApprover = value, reopened, topicName, persisted.RequesterPrincipalId,
                        "oidc-cli-human:" + Uri.EscapeDataString(Field("issuer")) + "|" + Uri.EscapeDataString(Field("approverSubject")) +
                        "|" + Uri.EscapeDataString(Field("cliClientId")) + "|g2-approver", postgres ? "PostgreSQL" : "SQLite", evidenceDirectory, RestartApi, deadline.Token);
                    return;
                }
                if (critical)
                {
                    Assert.Equal(CliApplication.Success, await Run("topics", "delete-preview", "permitted", topicName,
                        "--idempotency-key", "g2-delete-" + Guid.NewGuid().ToString("N")));
                    using var deletionPreview = JsonDocument.Parse(lastOutput);
                    var deletionId = deletionPreview.RootElement.GetProperty("operationId").GetGuid();
                    var deletionHash = deletionPreview.RootElement.GetProperty("previewHash").GetString()!;
                    var deletionChallenge = deletionPreview.RootElement.GetProperty("confirmationChallenge").GetString()!;
                    Assert.True(deletionPreview.RootElement.GetProperty("requiresIndependentApproval").GetBoolean());
                    Assert.Equal("critical", deletionPreview.RootElement.GetProperty("riskClass").GetString());
                    Assert.Equal(CliApplication.PendingApproval, await Run("operations", "confirm", deletionId.ToString("D"),
                        deletionHash, "--challenge", deletionChallenge));
                    Assert.Equal(CliApplication.Conflict, await Run("operations", "execute", deletionId.ToString("D")));
                    // Same human has both explicit grants, but independence still denies self-approval.
                    Assert.Equal(CliApplication.AuthenticationOrAuthorizationError,
                        await Run("operations", "approve", deletionId.ToString("D"), deletionHash));
                    asApprover = true;
                    Assert.Equal(CliApplication.AuthenticationOrAuthorizationError,
                        await Run("operations", "status", deletionId.ToString("D")));
                    var inspectionExit = await Run("operations", "approval", deletionId.ToString("D"));
                    Assert.Equal(CliApplication.PendingApproval, inspectionExit);
                    Assert.Equal(CliApplication.Conflict, await Run("operations", "approve", deletionId.ToString("D"), new string('b', 64)));
                    Assert.Equal(CliApplication.Success, await Run("operations", "approve", deletionId.ToString("D"), deletionHash));
                    // Approver has no requester mutation grant and cannot execute another actor's operation.
                    Assert.Equal(CliApplication.AuthenticationOrAuthorizationError,
                        await Run("operations", "execute", deletionId.ToString("D")));
                    asApprover = false;
                    Assert.Equal(CliApplication.Success, await Run("operations", "execute", deletionId.ToString("D")));
                    using (var deleted = JsonDocument.Parse(lastOutput))
                        Assert.Equal("appliedVerified", deleted.RootElement.GetProperty("state").GetString());
                    Assert.Equal(CliApplication.Success, await Run("operations", "status", deletionId.ToString("D")));
                    var durableDeletion = await reopened.GetAsync(deletionId, deadline.Token);
                    Assert.NotNull(durableDeletion);
                    Assert.Equal(MutationOperationState.AppliedVerified, durableDeletion.State);
                    Assert.Equal(persisted.RequesterPrincipalId, durableDeletion.RequesterPrincipalId);
                    Assert.Equal("oidc-cli-human:" + Uri.EscapeDataString(Field("issuer")) + "|" +
                        Uri.EscapeDataString(Field("approverSubject")) + "|" + Uri.EscapeDataString(Field("cliClientId")) +
                        "|g2-approver", durableDeletion.ApprovedByPrincipalId);
                    Assert.NotNull(durableDeletion.ApprovalAuthorizationEvidenceHash);
                    using var readback = new ConfluentKafkaAdministrationAdapter([
                        new ClusterProfile("permitted", [Environment.GetEnvironmentVariable("KAFDECK_G2_KAFKA_BOOTSTRAP")!],
                            KafkaSecurityProtocol.Plaintext, null, null)], new SecretResolver());
                    // Kafka's cluster metadata can temporarily retain a deleted
                    // name while DescribeTopics already rejects it. Retry only
                    // this observation; never retry the delete effect.
                    using var observationDeadline = CancellationTokenSource.CreateLinkedTokenSource(deadline.Token);
                    observationDeadline.CancelAfter(TimeSpan.FromSeconds(30));
                    var absenceObserved = false;
                    while (!observationDeadline.IsCancellationRequested)
                    {
                        var observedTopics = await readback.ListTopicsAsync("permitted",
                            new KafkaOperationContext(DateTimeOffset.UtcNow.AddSeconds(5)), observationDeadline.Token);
                        if (observedTopics.IsSuccess && observedTopics.Value is not null &&
                            observedTopics.Value.All(topic => topic.Name != topicName))
                        {
                            absenceObserved = true;
                            break;
                        }
                        testOutput.WriteLine($"KAFKA_DELETE_OBSERVATION success={observedTopics.IsSuccess}; category={observedTopics.Failure?.Category}; code={observedTopics.Failure?.Code}");
                        await Task.Delay(200, observationDeadline.Token);
                    }
                    Assert.True(absenceObserved, "A successful independent Kafka listing must establish the deleted topic's absence.");
                    await File.WriteAllTextAsync(Path.Combine(evidenceDirectory, "critical-human-approval-kafka-evidence.json"),
                        JsonSerializer.Serialize(new
                        {
                            observedAtUtc = DateTimeOffset.UtcNow, issuer = Field("issuer"), family = "TopicDelete",
                            flow = "two genuine Keycloak human PKCE identities -> typed CLI -> CRITICAL preview/confirm -> distinct registered human approval -> original requester execute -> real Kafka absence -> durable SQLite reopen",
                            awaitingApprovalExit = 6, preApprovalExecuteExit = 8, sameHumanApprovalExit = 3,
                            approverPrivateStatusExit = 3, wrongPreviewHashExit = 8, approverExecuteExit = 3,
                            durableState = durableDeletion.State.ToString(), exactRequesterAndApprover = true,
                            currentApprovalEvidenceRetained = true, realKafkaAbsence = true, browserLogins,
                            caveat = "One CRITICAL TopicDelete; no other mutation family, PostgreSQL effect, graphical browser, key rotation, database restore or HA approval acceptance.",
                        }, new JsonSerializerOptions { WriteIndented = true }), deadline.Token);
                    return;
                }
                if (productBridge is not null)
                    await productBridge.DeliverAfterApiRestartAsync(host, mutationDb, persisted, topicName, evidenceDirectory, deadline.Token);
                await File.WriteAllTextAsync(Path.Combine(evidenceDirectory, service ? "service-cli-kafka-effect-evidence.json" : postgres ? "postgres-human-cli-kafka-effect-evidence.json" : "human-cli-kafka-effect-evidence.json"), JsonSerializer.Serialize(new
                {
                    observedAtUtc = DateTimeOffset.UtcNow, issuer = Field("issuer"), audience = Field("audience"),
                    flow = (service ? "real Keycloak private_key_jwt" : "real Keycloak human PKCE") + " -> shipped CLI -> bearer-only API -> durable preview/confirm/execute -> real Kafka -> status/readback",
                    family = "TopicCreate", persistence = postgres ? "PostgreSQL" : "SQLite", durableState = persisted.State.ToString(),
                    requesterKind = actorKind, exactInitiatingPrincipal = true, serverPolicyFingerprintBound = true,
                    idempotentPreviewSameOperation = true, changedIntentSameKeyExit = 8, wrongPreviewHashExit = 8,
                    realTopicReadback = true, durableRepositoryReopen = true, browserLogins,
                    caveat = "Low-risk TopicCreate only. No CRITICAL approval, other-family parity, database failover/restore or unknown-effect acceptance in this scenario.",
                }, new JsonSerializerOptions { WriteIndented = true }), deadline.Token);
                return;
            }
            browserMode = "nonce";
            Assert.Equal(CliApplication.AuthenticationOrAuthorizationError, await Run("system", "info"));
            browserMode = "pkce";
            Assert.Equal(CliApplication.AuthenticationOrAuthorizationError, await Run("system", "info"));
            Assert.Equal(4, browserLogins);
            Assert.Equal(4, rejectedCallbacks);
            Assert.True(callbackPorts.Count >= 1);
            await File.WriteAllTextAsync(Path.Combine(evidenceDirectory, "human-cli-evidence.json"), JsonSerializer.Serialize(new
            {
                observedAtUtc = DateTimeOffset.UtcNow, keycloak = Field("keycloakVersion"), java = Field("javaVersion"),
                issuer = Field("issuer"), audience = Field("audience"), clientId = Field("cliClientId"),
                flow = "real authorization endpoint + login form + authorization_code/PKCE S256 + signed ID/access tokens",
                browserLogins, rejectedCallbacks, callback = "127.0.0.1:ephemeral/oauth/callback", systemInfoExit = 0, deniedClusterExit = 3,
                wrongNonceExit = 3, wrongPkceExit = 3,
                tokens = "memory only; no token or authorization code retained",
                caveat = "Single-node identity laboratory and HTTP form driver; does not establish graphical browser, HA, upgrade or governed mutation acceptance.",
            }, new JsonSerializerOptions { WriteIndented = true }), deadline.Token);
        }
        finally
        {
            if (!host.HasExited) host.Kill(entireProcessTree: true);
            await host.WaitForExitAsync(CancellationToken.None);
            var logs = completedHostLogs + await stdout + await stderr;
            host.Dispose();
            Assert.False(logs.Contains(password, StringComparison.Ordinal), "The API log reflected the lab password.");
            if (approverPassword is not null) Assert.False(logs.Contains(approverPassword, StringComparison.Ordinal),
                "The API log reflected the disposable approver password.");
            Assert.False(logs.Contains("Bearer eyJ", StringComparison.Ordinal), "The API log exposed bearer JWT material.");
            await File.WriteAllTextAsync(Path.Combine(evidenceDirectory, postgres && !extendedTopics ? "g2-postgres-product-api.log" : postgres ? "g2-postgres-topic-lifecycle-api.log" : extendedTopics ? "g2-topic-lifecycle-api.log" : critical ? "g2-critical-api.log" : service ? "g2-service-effect-api.log" : includeEffect ? "g2-human-effect-api.log" : "g2-human-api.log"), logs);
        }
    }

    private static string FindRoot()
    {
        var directory = new DirectoryInfo(AppContext.BaseDirectory);
        while (directory is not null && !File.Exists(Path.Combine(directory.FullName, "Kafdeck.slnx"))) directory = directory.Parent;
        return directory?.FullName ?? throw new InvalidOperationException("Repository root not found.");
    }
}
