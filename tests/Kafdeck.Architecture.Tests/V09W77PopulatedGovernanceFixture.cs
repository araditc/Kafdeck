using System.Globalization;
using System.Text.Json;
using Kafdeck.Api;
using Kafdeck.Cli;
using Kafdeck.Core.Attribution;
using Kafdeck.Infrastructure.Persistence;
using Kafdeck.Modules.Administration;
using Xunit;

namespace Kafdeck.Architecture.Tests;

internal sealed record W77GovernanceDataset(string Topic, string RequestId, string PolicyVersion, string FromUtc, string ToUtc, string WindowId)
{
    internal static async Task<W77GovernanceDataset> CreateAsync(W77Laboratory lab, CancellationToken token, int ruleCount = 1)
    {
        var api = lab.Api;
        IMutationDbConnectionFactory connections = lab.Database is null ? new SqliteMutationDbConnectionFactory(lab.SqlitePath) : new PostgreSqlMutationDbConnectionFactory(lab.Database.ConnectionString);
        var generation = Guid.Parse(api.Settings["Kafdeck:Governance:DeploymentGeneration"]);
        var gate = new AdoGovernanceGenerationGate(connections, generation);
        var operations = new AdoMutationOperationRepository(connections);
        async Task<JsonElement> Success(params string[] args)
        {
            Assert.True(await api.RunAsync(token, args) == CliApplication.Success, "W77 real populated governance setup failed: " + api.SafeOutcome);
            using var document = JsonDocument.Parse(api.Output); return document.RootElement.Clone();
        }
        static Guid Id(JsonElement operation) => operation.GetProperty("operationId").GetGuid();
        async Task Commit(JsonElement operation, bool critical = false)
        {
            var args = new List<string> { "operations", "confirm", Id(operation).ToString("D"), operation.GetProperty("previewHash").GetString()! };
            if (operation.TryGetProperty("confirmationChallenge", out var challenge) && challenge.ValueKind == JsonValueKind.String) args.AddRange(["--challenge", challenge.GetString()!]);
            Assert.Equal(critical ? CliApplication.PendingApproval : CliApplication.Success, await api.RunAsync(token, args.ToArray()));
            if (critical)
            {
                api.Approver = true;
                try { await Success("operations", "approve", Id(operation).ToString("D"), operation.GetProperty("previewHash").GetString()!); }
                finally { api.Approver = false; }
            }
            var executed = await Success("operations", "execute", Id(operation).ToString("D")); Assert.Equal("appliedVerified", executed.GetProperty("state").GetString());
        }
        async Task<JsonElement> Preview(string family, string command, object body)
        {
            var file = await api.RequestFileAsync(body, token);
            return await Success(family, command, "permitted", "--request-file", file, "--idempotency-key", Guid.NewGuid().ToString("N"));
        }
        var topic = "w77-soak-" + Guid.NewGuid().ToString("N");
        if (ruleCount is < 1 or > 100) throw new ArgumentOutOfRangeException(nameof(ruleCount));
        var rules = Enumerable.Range(1, ruleCount).Select(index => new
        {
            id = index == ruleCount ? "w77-real-approval" : $"w82-reference-rule-{index:D3}",
            priority = index,
            scope = new
            {
                clusterId = "permitted", resourceKind = "topic",
                resourceIds = new[] { index == ruleCount ? topic : $"w82-ref-topic-{index:D4}" },
                action = "TopicCreate"
            },
            conditions = Array.Empty<object>(),
            outcome = "RequireApprovedRequest"
        });
        var policyDocument = JsonSerializer.Serialize(new { schemaVersion = 1, rules });
        var policy = await Preview("policies", "version-preview", new { clusterId = "permitted", document = policyDocument }); await Commit(policy);
        var policyPlan = await Success("policies", "plan", Id(policy).ToString("D")); var policyVersion = policyPlan.GetProperty("versionId").GetString()!;
        await Commit(await Preview("policies", "activate-preview", new { clusterId = "permitted", versionId = policyVersion, expectedRevision = 0, enabled = true }), true);
        var target = await Success("topics", "create-preview", "permitted", topic, "1", "1", "--idempotency-key", Guid.NewGuid().ToString("N"));
        Assert.Equal(CliApplication.Success, await api.RunAsync(token, "operations", "confirm", Id(target).ToString("D"), target.GetProperty("previewHash").GetString()!));
        var targetSnapshot = await operations.GetAsync(Id(target), token); Assert.NotNull(targetSnapshot);
        var expiresAtUtc = targetSnapshot.PreviewExpiresAtUtc.ToUniversalTime().ToString("yyyy-MM-ddTHH:mm:ss.fffffffZ", CultureInfo.InvariantCulture);
        var draft = await Preview("requests", "change-preview", new { clusterId = "permitted", requestId = (string?)null, expectedVersion = 0,
            command = "Draft", operationId = Id(target), reason = "W77 bounded real approved topic", expiresAtUtc }); await Commit(draft);
        var requestPlan = await Success("requests", "plan", Id(draft).ToString("D")); var requestId = requestPlan.GetProperty("plan").GetProperty("requestId").GetString()!;
        await Commit(await Preview("requests", "change-preview", new { clusterId = "permitted", requestId, expectedVersion = 1, command = "Submit", reason = "W77 submit" }));
        api.Approver = true;
        try { await Commit(await Preview("requests", "change-preview", new { clusterId = "permitted", requestId, expectedVersion = 2, command = "Approve", reason = "W77 distinct reviewer" })); }
        finally { api.Approver = false; }
        var effect = await Success("operations", "execute", Id(target).ToString("D")); Assert.Equal("appliedVerified", effect.GetProperty("state").GetString());
        var assignedFile = await api.RequestFileAsync(new { clusterId = "permitted", changes = new[] { new { kind = "Topic", expectedRevision = 0,
            name = "W77 measured approved topic", topicName = topic, teamId = lab.TeamId } } }, token);
        var assigned = await Success("catalog", "change-preview", "permitted", "topics", "--request-file", assignedFile, "--idempotency-key", Guid.NewGuid().ToString("N"));
        await Commit(assigned.GetProperty("operation"));
        var identity = assigned.GetProperty("changes")[0].GetProperty("topicIdentity").GetString()!;
        await using var meter = new V09W76MeasuredTrafficFixture(); meter.Begin(topic, identity);
        await meter.SendTrafficAsync(65536, token); await meter.SendTrafficAsync(32768, token); Assert.Equal(98304, meter.PhysicalBytes);
        var rateDocument = new RateCardDocument("trafficBytes", "bytes", "USD", "0.00001", meter.Started.AddMinutes(-1), meter.Started.AddMinutes(20), []);
        var rate = await Preview("rate-cards", "version-preview", JsonSerializer.SerializeToElement(new { clusterId = "permitted", document = rateDocument }, AttributionContract.Json)); await Commit(rate);
        var ratePlan = await Success("rate-cards", "plan", Id(rate).ToString("D"));
        await Commit(await Preview("rate-cards", "activate-preview", new { clusterId = "permitted", versionId = ratePlan.GetProperty("versionId").GetString(), expectedRevision = 0, enabled = true }));
        await meter.CloseAsync(token);
        var measured = new AdoAttributionStore(connections, gate); var catalog = new AdoCatalogStore(connections, generation.ToString("D"), gate);
        var collector = new AttributionCollectionService([meter.Source("permitted")], measured, catalog);
        Assert.Equal(1, await collector.CollectAsync(token));
        var from = meter.Window.StartUtc.UtcDateTime.ToString("O", CultureInfo.InvariantCulture); var to = meter.Window.EndUtc.UtcDateTime.ToString("O", CultureInfo.InvariantCulture);
        var observed = await Success("attribution", "windows", "permitted", from, to, topic);
        var row = Assert.Single(observed.GetProperty("items").EnumerateArray()); Assert.Equal("0.98", row.GetProperty("roundedAmount").GetString());
        return new(topic, requestId, policyVersion, from, to, row.GetProperty("measurement").GetProperty("windowId").GetString()!);
    }
}
