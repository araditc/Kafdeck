using Kafdeck.Core.Kafka;
using Kafdeck.Infrastructure.Configuration;
using Kafdeck.Infrastructure.Kafka;
using Kafdeck.Modules.Administration;
using Xunit;

namespace Kafdeck.Architecture.Tests;

public sealed class KafkaAclMutationIntegrationTests
{
    [Fact]
    public async Task Typed_acl_adapter_creates_observes_and_removes_allow_deny_prefix_and_wildcard_bindings()
    {
        if (!RunKafkaIntegration())
        {
            return;
        }

        var suffix = Guid.NewGuid().ToString("N");
        var principal = $"User:kafdeck-w42-{suffix}";
        var literalTopic = $"kafdeck-w42-{suffix}";
        var prefix = $"kafdeck-w42-{suffix}-";
        var profile = new ClusterProfile(
            "plaintext",
            ["localhost:9092"],
            KafkaSecurityProtocol.Plaintext,
            null,
            null);

        var bindings = new[]
        {
            new KafkaAclBinding(
                KafkaAclResourceType.Topic,
                literalTopic,
                KafkaAclPatternType.Literal,
                principal,
                "*",
                KafkaAclOperation.Read,
                KafkaAclPermissionType.Allow),
            new KafkaAclBinding(
                KafkaAclResourceType.Topic,
                literalTopic,
                KafkaAclPatternType.Literal,
                principal,
                "*",
                KafkaAclOperation.Write,
                KafkaAclPermissionType.Deny),
            new KafkaAclBinding(
                KafkaAclResourceType.Topic,
                prefix,
                KafkaAclPatternType.Prefixed,
                principal,
                "*",
                KafkaAclOperation.Describe,
                KafkaAclPermissionType.Allow),
            new KafkaAclBinding(
                KafkaAclResourceType.Topic,
                "*",
                KafkaAclPatternType.Literal,
                principal,
                "*",
                KafkaAclOperation.DescribeConfigs,
                KafkaAclPermissionType.Deny),
        };

        using var mutations = new ConfluentKafkaAclMutationAdapter(
            new[] { profile },
            new SecretResolver());
        using var observations = new ConfluentKafkaAclObservationAdapter(
            new[] { profile },
            new SecretResolver());
        using var cancellation =
            new CancellationTokenSource(TimeSpan.FromSeconds(60));

        try
        {
            var created = await mutations.CreateAsync(
                new AclCreateMutation(
                    profile.Id,
                    bindings),
                Operation(),
                cancellation.Token);

            Assert.Equal(
                MutationExecutionResultKind.AppliedUnverified,
                created.ResultKind);

            foreach (var binding in bindings)
            {
                await EventuallyAsync(
                    async () =>
                    {
                        var observed = await observations.DescribeAsync(
                            profile.Id,
                            AclMutationPolicy.ExactFilter(binding),
                            Operation(),
                            cancellation.Token);
                        return observed.IsSuccess &&
                               observed.Value is not null &&
                               observed.Value.Contains(binding);
                    },
                    cancellation.Token);
            }

            var removed = await mutations.RemoveAsync(
                new AclRemoveMutation(
                    profile.Id,
                    bindings),
                Operation(),
                cancellation.Token);

            Assert.Equal(
                MutationExecutionResultKind.AppliedUnverified,
                removed.ResultKind);

            foreach (var binding in bindings)
            {
                await EventuallyAsync(
                    async () =>
                    {
                        var observed = await observations.DescribeAsync(
                            profile.Id,
                            AclMutationPolicy.ExactFilter(binding),
                            Operation(),
                            cancellation.Token);
                        return observed.IsSuccess &&
                               observed.Value is not null &&
                               !observed.Value.Contains(binding);
                    },
                    cancellation.Token);
            }
        }
        finally
        {
            try
            {
                _ = await mutations.RemoveAsync(
                    new AclRemoveMutation(
                        profile.Id,
                        bindings),
                    Operation(),
                    CancellationToken.None);
            }
            catch
            {
                // Best-effort cleanup for unique W42 ACL identities.
            }
        }
    }

    private static async Task EventuallyAsync(
        Func<Task<bool>> condition,
        CancellationToken cancellationToken)
    {
        var deadline = DateTimeOffset.UtcNow.AddSeconds(20);
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
            "Kafka ACL post-condition was not observed before the bounded integration-test deadline.");
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
