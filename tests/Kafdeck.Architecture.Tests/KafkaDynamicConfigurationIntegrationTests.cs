using Kafdeck.Core.Kafka;
using Kafdeck.Infrastructure.Configuration;
using Kafdeck.Infrastructure.Kafka;
using Kafdeck.Modules.Administration;
using Xunit;

namespace Kafdeck.Architecture.Tests;

public sealed class KafkaDynamicConfigurationIntegrationTests
{
    [Fact]
    public async Task Typed_dynamic_config_adapter_sets_observes_and_resets_cluster_default()
    {
        if (!RunKafkaIntegration())
        {
            return;
        }

        var profile = new ClusterProfile(
            "plaintext",
            ["localhost:9092"],
            KafkaSecurityProtocol.Plaintext,
            null,
            null);
        var target = new DynamicConfigurationTarget(
            profile.Id,
            DynamicConfigurationScope.ClusterDefault,
            null,
            "log.cleaner.backoff.ms");

        using var adapter =
            new ConfluentKafkaDynamicConfigurationAdapter(
                new[] { profile },
                new SecretResolver());
        using var cancellation =
            new CancellationTokenSource(
                TimeSpan.FromSeconds(60));

        var before = await adapter.DescribeAsync(
            target,
            Operation(),
            cancellation.Token);
        Assert.True(before.IsSuccess);
        Assert.NotNull(before.Value);
        Assert.False(before.Value!.IsSensitive);
        Assert.False(before.Value.IsReadOnly);

        try
        {
            var set = await adapter.AlterAsync(
                new DynamicConfigurationMutation(
                    target,
                    "17001"),
                Operation(),
                cancellation.Token);
            Assert.Equal(
                MutationExecutionResultKind.AppliedUnverified,
                set.ResultKind);

            await EventuallyAsync(
                async () =>
                {
                    var observed =
                        await adapter.DescribeAsync(
                            target,
                            Operation(),
                            cancellation.Token);
                    return observed.IsSuccess &&
                           observed.Value is not null &&
                           string.Equals(
                               observed.Value.EffectiveValue,
                               "17001",
                               StringComparison.Ordinal) &&
                           string.Equals(
                               observed.Value.EffectiveSource,
                               "DynamicDefaultBrokerConfig",
                               StringComparison.Ordinal);
                },
                cancellation.Token);

            var reset = await adapter.AlterAsync(
                new DynamicConfigurationMutation(
                    target,
                    Value: null),
                Operation(),
                cancellation.Token);
            Assert.Equal(
                MutationExecutionResultKind.AppliedUnverified,
                reset.ResultKind);

            await EventuallyAsync(
                async () =>
                {
                    var observed =
                        await adapter.DescribeAsync(
                            target,
                            Operation(),
                            cancellation.Token);
                    return observed.IsSuccess &&
                           observed.Value is not null &&
                           !string.Equals(
                               observed.Value.EffectiveSource,
                               "DynamicDefaultBrokerConfig",
                               StringComparison.Ordinal);
                },
                cancellation.Token);
        }
        finally
        {
            try
            {
                _ = await adapter.AlterAsync(
                    new DynamicConfigurationMutation(
                        target,
                        Value: null),
                    Operation(),
                    CancellationToken.None);
            }
            catch
            {
                // Best-effort cleanup for isolated CI Kafka.
            }
        }
    }

    private static async Task EventuallyAsync(
        Func<Task<bool>> condition,
        CancellationToken cancellationToken)
    {
        var deadline =
            DateTimeOffset.UtcNow.AddSeconds(20);
        while (DateTimeOffset.UtcNow < deadline)
        {
            cancellationToken.ThrowIfCancellationRequested();
            if (await condition().ConfigureAwait(false))
            {
                return;
            }

            await Task.Delay(
                    TimeSpan.FromMilliseconds(200),
                    cancellationToken)
                .ConfigureAwait(false);
        }

        Assert.Fail(
            "Kafka dynamic configuration post-condition was not observed before the bounded integration-test deadline.");
    }

    private static KafkaOperationContext Operation() =>
        new(DateTimeOffset.UtcNow.AddSeconds(10));

    private static bool RunKafkaIntegration() =>
        string.Equals(
            Environment.GetEnvironmentVariable(
                "KAFDECK_RUN_KAFKA_INTEGRATION"),
            "1",
            StringComparison.Ordinal);
}
