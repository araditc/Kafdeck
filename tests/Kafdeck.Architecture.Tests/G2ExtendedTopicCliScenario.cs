using System.Text.Json;
using Kafdeck.Cli;
using Kafdeck.Core.Kafka;
using Kafdeck.Infrastructure.Configuration;
using Kafdeck.Infrastructure.Kafka;
using Kafdeck.Infrastructure.Persistence;
using Kafdeck.Modules.Administration;
using Xunit;

namespace Kafdeck.Architecture.Tests;

internal static class G2ExtendedTopicCliScenario
{
    internal static async Task RunAsync(Func<string[], Task<int>> run, Func<string> output, Action<bool> asApprover,
        AdoMutationOperationRepository repository, string topic, string requester, string approver, string persistence,
        string evidenceDirectory, Func<Task> restartApi, CancellationToken token)
    {
        using var reads = new ConfluentKafkaAdministrationAdapter([
            new ClusterProfile("permitted", [Environment.GetEnvironmentVariable("KAFDECK_G2_KAFKA_BOOTSTRAP")!],
                KafkaSecurityProtocol.Plaintext, null, null)], new SecretResolver());
        var operations = new List<Guid>();
        var initialConfiguration = await reads.GetTopicConfigurationAsync("permitted", topic,
            new KafkaOperationContext(DateTimeOffset.UtcNow.AddSeconds(10)), token);
        Assert.True(initialConfiguration.IsSuccess);
        Assert.NotNull(initialConfiguration.Value);
        var initialRetention = Assert.Single(initialConfiguration.Value, entry => entry.Name == "retention.ms").Value;
        Assert.False(string.IsNullOrWhiteSpace(initialRetention), "The initial real Kafka retention default must be observable.");
        Assert.Contains(initialConfiguration.Value, entry => entry.Name == "segment.bytes" && entry.Value == "1048576" && !entry.IsSensitive);

        async Task<(Guid Id, string Hash, string? Challenge)> Preview(params string[] command)
        {
            Assert.Equal(CliApplication.Success, await run(command));
            using var json = JsonDocument.Parse(output());
            var root = json.RootElement;
            var id = root.GetProperty("operationId").GetGuid();
            operations.Add(id);
            return (id, root.GetProperty("previewHash").GetString()!, root.GetProperty("confirmationChallenge").GetString());
        }
        async Task Confirm((Guid Id, string Hash, string? Challenge) operation, int expected = CliApplication.Success)
        {
            var command = new[] { "operations", "confirm", operation.Id.ToString("D"), operation.Hash };
            if (operation.Challenge is not null) command = command.Concat(["--challenge", operation.Challenge]).ToArray();
            Assert.Equal(expected, await run(command));
        }
        async Task Execute((Guid Id, string Hash, string? Challenge) operation)
        {
            await Confirm(operation);
            Assert.Equal(CliApplication.Success, await run(["operations", "execute", operation.Id.ToString("D")]));
            using var json = JsonDocument.Parse(output());
            Assert.Equal("appliedVerified", json.RootElement.GetProperty("state").GetString());
            var row = await repository.GetAsync(operation.Id, token);
            Assert.NotNull(row);
            Assert.Equal(MutationOperationState.AppliedVerified, row.State);
            Assert.Equal(requester, row.RequesterPrincipalId);
            Assert.StartsWith("v0.8-bearer:", row.PolicyVersion, StringComparison.Ordinal);
        }
        async Task AssertConfiguration(string value)
        {
            var observed = await reads.GetTopicConfigurationAsync("permitted", topic,
                new KafkaOperationContext(DateTimeOffset.UtcNow.AddSeconds(10)), token);
            Assert.True(observed.IsSuccess, "Independent real Kafka configuration observation must succeed.");
            Assert.NotNull(observed.Value);
            Assert.Contains(observed.Value, entry => entry.Name == "retention.ms" && entry.Value == value && !entry.IsSensitive);
        }

        var alteration = await Preview("topics", "alter-preview", "permitted", topic, "--config", "retention.ms=60000",
            "--idempotency-key", "g2-alter-" + Guid.NewGuid().ToString("N"));
        // The server-stored immutable preview must survive a real API process
        // restart; the next command obtains a fresh genuine human token.
        await restartApi();
        await Execute(alteration);
        await AssertConfiguration("60000");

        var increase = await Preview("topics", "increase-partitions-preview", "permitted", topic, "2",
            "--idempotency-key", "g2-increase-" + Guid.NewGuid().ToString("N"));
        Assert.NotNull(increase.Challenge);
        await Execute(increase);
        var metadata = await reads.GetTopicMetadataAsync("permitted", topic,
            new KafkaOperationContext(DateTimeOffset.UtcNow.AddSeconds(10)), token);
        Assert.True(metadata.IsSuccess);
        Assert.NotNull(metadata.Value);
        Assert.Equal(2, metadata.Value.Partitions.Count);
        Assert.Equal(CliApplication.RemoteError, await run(["topics", "increase-partitions-preview", "permitted", topic, "1",
            "--idempotency-key", "g2-no-decrease-" + Guid.NewGuid().ToString("N")]));
        Assert.Equal(CliApplication.UsageError, await run(["topics", "alter-preview", "permitted", topic,
            "--config", "bootstrap.servers=unsafe-sentinel", "--idempotency-key", "rejected-key"]));

        var cancelled = await Preview("topics", "alter-preview", "permitted", topic, "--config", "retention.ms=90000",
            "--idempotency-key", "g2-cancel-" + Guid.NewGuid().ToString("N"));
        Assert.Equal(CliApplication.Success, await run(["operations", "cancel", cancelled.Id.ToString("D"), cancelled.Hash]));
        Assert.Equal(CliApplication.RemoteError, await run(["operations", "status", cancelled.Id.ToString("D")]));
        using (var status = JsonDocument.Parse(output())) Assert.Equal("cancelled", status.RootElement.GetProperty("state").GetString());
        Assert.Equal(CliApplication.Conflict, await run(["operations", "execute", cancelled.Id.ToString("D")]));
        await AssertConfiguration("60000");
        Assert.Equal(MutationOperationState.Cancelled, (await repository.GetAsync(cancelled.Id, token))!.State);

        var rejected = await Preview("topics", "delete-preview", "permitted", topic,
            "--idempotency-key", "g2-reject-" + Guid.NewGuid().ToString("N"));
        await Confirm(rejected, CliApplication.PendingApproval);
        Assert.Equal(CliApplication.AuthenticationOrAuthorizationError,
            await run(["operations", "reject", rejected.Id.ToString("D"), rejected.Hash]));
        asApprover(true);
        try
        {
            Assert.Equal(CliApplication.Conflict, await run(["operations", "reject", rejected.Id.ToString("D"), new string('c', 64)]));
            Assert.Equal(CliApplication.Success, await run(["operations", "reject", rejected.Id.ToString("D"), rejected.Hash]));
        }
        finally { asApprover(false); }
        Assert.Equal(CliApplication.RemoteError, await run(["operations", "status", rejected.Id.ToString("D")]));
        using (var status = JsonDocument.Parse(output())) Assert.Equal("rejected", status.RootElement.GetProperty("state").GetString());
        Assert.Equal(CliApplication.Conflict, await run(["operations", "execute", rejected.Id.ToString("D")]));
        var rejectedRow = await repository.GetAsync(rejected.Id, token);
        Assert.NotNull(rejectedRow);
        Assert.Equal(MutationOperationState.Rejected, rejectedRow.State);
        Assert.Equal(requester, rejectedRow.RequesterPrincipalId);
        Assert.Equal(approver, rejectedRow.RejectedByPrincipalId);
        Assert.NotNull(rejectedRow.RejectionAuthorizationEvidenceHash);
        metadata = await reads.GetTopicMetadataAsync("permitted", topic,
            new KafkaOperationContext(DateTimeOffset.UtcNow.AddSeconds(10)), token);
        Assert.True(metadata.IsSuccess);
        Assert.NotNull(metadata.Value);
        Assert.Equal(2, metadata.Value.Partitions.Count);
        await AssertConfiguration("60000");
        var reset = await Preview("topics", "alter-preview", "permitted", topic, "--reset-config", "retention.ms",
            "--idempotency-key", "g2-reset-" + Guid.NewGuid().ToString("N"));
        await Execute(reset);
        await AssertConfiguration(initialRetention!);
        await File.WriteAllTextAsync(Path.Combine(evidenceDirectory,
            persistence == "PostgreSQL" ? "postgres-topic-cli-lifecycle-evidence.json" : "topic-cli-lifecycle-evidence.json"),
            JsonSerializer.Serialize(new
            {
                observedAtUtc = DateTimeOffset.UtcNow, persistence,
                flow = "real Keycloak human PKCE -> shipped CLI/API -> real Kafka TopicCreate/TopicAlter/partition increase -> cancelled alternate configuration -> independently rejected CRITICAL delete -> durable repository reopen",
                alterationState = "AppliedVerified", retentionMs = 60000, partitionIncreaseState = "AppliedVerified", partitions = 2,
                decreaseRefused = true, unsupportedConfigRefused = true, cancelCommandExit = 0, cancelledStatusExit = 4,
                rejectCommandExit = 0, rejectedStatusExit = 4, sameHumanRejectExit = 3, wrongRejectionHashExit = 8,
                cancelledAndRejectedExecutionExit = 8, exactRequesterAndRejector = true, cancelledAndRejectedEffectsAbsent = true,
                createConfigurationObserved = true, resetToObservedDefault = true,
                durableOperationCount = operations.Count, actualApiProcessRestarts = 1, immutablePreviewSurvivedRestart = true,
                caveat = "Four admitted topic families only; this case never executes delete. It does not establish mutation primary failover, restore resumption, other-family parity or the complete G2 exit.",
            }, new JsonSerializerOptions { WriteIndented = true }), token);
    }
}
