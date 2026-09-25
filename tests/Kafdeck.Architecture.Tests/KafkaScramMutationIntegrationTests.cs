using System.Security.Cryptography;
using System.Text;
using Confluent.Kafka;
using Kafdeck.Core.Kafka;
using Kafdeck.Infrastructure.Configuration;
using Kafdeck.Infrastructure.Kafka;
using Kafdeck.Modules.Administration;
using Xunit;

namespace Kafdeck.Architecture.Tests;

public sealed class KafkaScramMutationIntegrationTests
{
    [Fact]
    public async Task Typed_scram_adapter_creates_rotates_observes_and_deletes_synthetic_credential()
    {
        if (!RunKafkaIntegration())
        {
            return;
        }

        var user = $"kafdeck-w43-{Guid.NewGuid():N}";
        var profile = new ClusterProfile(
            "plaintext",
            ["localhost:9092"],
            KafkaSecurityProtocol.Plaintext,
            null,
            null);
        var firstPassword = Encoding.UTF8.GetBytes(
            Convert.ToHexString(RandomNumberGenerator.GetBytes(32)));
        var rotatedPassword = Encoding.UTF8.GetBytes(
            Convert.ToHexString(RandomNumberGenerator.GetBytes(32)));

        using var mutations = new ConfluentKafkaScramMutationAdapter(
            new[] { profile },
            new SecretResolver());
        using var observations = new ConfluentKafkaScramObservationAdapter(
            new[] { profile },
            new SecretResolver());
        using var cancellation =
            new CancellationTokenSource(TimeSpan.FromSeconds(60));

        try
        {
            var absent = await observations.DescribeUserAsync(
                profile.Id,
                user,
                Operation(),
                cancellation.Token);
            Assert.True(absent.IsSuccess);
            Assert.NotNull(absent.Value);
            Assert.Empty(absent.Value!);

            var created = await mutations.UpsertAsync(
                new ScramUpsertMutation(
                    profile.Id,
                    user,
                    KafkaScramMechanism.ScramSha256,
                    4096),
                firstPassword,
                Operation(),
                cancellation.Token);
            Assert.Equal(
                MutationExecutionResultKind.AppliedUnverified,
                created.ResultKind);

            await EventuallyAsync(
                observations,
                profile.Id,
                user,
                expectedIterations: 4096,
                shouldExist: true,
                cancellation.Token);
            AssertScramAuthentication(
                user,
                firstPassword,
                shouldSucceed: true);

            var rotated = await mutations.UpsertAsync(
                new ScramUpsertMutation(
                    profile.Id,
                    user,
                    KafkaScramMechanism.ScramSha256,
                    8192),
                rotatedPassword,
                Operation(),
                cancellation.Token);
            Assert.Equal(
                MutationExecutionResultKind.AppliedUnverified,
                rotated.ResultKind);

            await EventuallyAsync(
                observations,
                profile.Id,
                user,
                expectedIterations: 8192,
                shouldExist: true,
                cancellation.Token);
            AssertScramAuthentication(
                user,
                firstPassword,
                shouldSucceed: false);
            AssertScramAuthentication(
                user,
                rotatedPassword,
                shouldSucceed: true);

            var deleted = await mutations.DeleteAsync(
                new ScramDeleteMutation(
                    profile.Id,
                    user,
                    KafkaScramMechanism.ScramSha256),
                Operation(),
                cancellation.Token);
            Assert.Equal(
                MutationExecutionResultKind.AppliedUnverified,
                deleted.ResultKind);

            await EventuallyAsync(
                observations,
                profile.Id,
                user,
                expectedIterations: 8192,
                shouldExist: false,
                cancellation.Token);
            AssertScramAuthentication(
                user,
                rotatedPassword,
                shouldSucceed: false);
        }
        finally
        {
            try
            {
                _ = await mutations.DeleteAsync(
                    new ScramDeleteMutation(
                        profile.Id,
                        user,
                        KafkaScramMechanism.ScramSha256),
                    Operation(),
                    CancellationToken.None);
            }
            catch
            {
                // Best-effort cleanup for the unique synthetic W43 user.
            }

            CryptographicOperations.ZeroMemory(firstPassword);
            CryptographicOperations.ZeroMemory(rotatedPassword);
        }
    }

    private static async Task EventuallyAsync(
        IScramObservationPort observations,
        string clusterId,
        string user,
        int expectedIterations,
        bool shouldExist,
        CancellationToken cancellationToken)
    {
        var deadline = DateTimeOffset.UtcNow.AddSeconds(20);
        while (DateTimeOffset.UtcNow < deadline)
        {
            cancellationToken.ThrowIfCancellationRequested();
            var observed = await observations.DescribeUserAsync(
                clusterId,
                user,
                Operation(),
                cancellationToken);
            if (observed.IsSuccess && observed.Value is not null)
            {
                var match = observed.Value.Any(item =>
                    item.Mechanism ==
                        KafkaScramMechanism.ScramSha256 &&
                    item.Iterations == expectedIterations);
                if (match == shouldExist)
                {
                    return;
                }
            }

            await Task.Delay(
                    TimeSpan.FromMilliseconds(200),
                    cancellationToken)
                .ConfigureAwait(false);
        }

        Assert.Fail(
            "Kafka SCRAM post-condition was not observed before the bounded integration-test deadline.");
    }

    private static void AssertScramAuthentication(
        string user,
        ReadOnlySpan<byte> password,
        bool shouldSucceed)
    {
        var config = new AdminClientConfig
        {
            BootstrapServers = "localhost:9094",
            SecurityProtocol = SecurityProtocol.SaslPlaintext,
            SaslMechanism = Confluent.Kafka.SaslMechanism.ScramSha256,
            SaslUsername = user,
            SaslPassword = Encoding.UTF8.GetString(password),
        };

        using var client = new AdminClientBuilder(config).Build();
        if (shouldSucceed)
        {
            var metadata = client.GetMetadata(TimeSpan.FromSeconds(5));
            Assert.NotNull(metadata);
            Assert.NotEmpty(metadata.Brokers);
            return;
        }

        Assert.ThrowsAny<KafkaException>(
            () => client.GetMetadata(TimeSpan.FromSeconds(5)));
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
