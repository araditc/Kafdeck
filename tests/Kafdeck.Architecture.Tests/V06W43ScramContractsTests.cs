using Confluent.Kafka;
using Confluent.Kafka.Admin;
using Kafdeck.Infrastructure.Kafka;
using Kafdeck.Modules.Administration;
using Xunit;

namespace Kafdeck.Architecture.Tests;

public sealed class V06W43ScramContractsTests
{
    [Fact]
    public void Scram_metadata_contract_exposes_no_credential_material()
    {
        var properties = typeof(KafkaScramCredentialMetadata).GetProperties();

        Assert.Equal(
            new[] { "Iterations", "Mechanism", "User" },
            properties
                .Select(property => property.Name)
                .OrderBy(name => name, StringComparer.Ordinal)
                .ToArray());
        Assert.DoesNotContain(
            properties,
            property => property.PropertyType == typeof(byte[]));
        Assert.DoesNotContain(
            properties,
            property =>
                property.Name.Contains("Password", StringComparison.OrdinalIgnoreCase) ||
                property.Name.Contains("Salt", StringComparison.OrdinalIgnoreCase) ||
                property.Name.Contains("Secret", StringComparison.OrdinalIgnoreCase) ||
                property.Name.Contains("Verifier", StringComparison.OrdinalIgnoreCase));
    }

    [Fact]
    public void Metadata_set_is_exact_bounded_and_mechanism_unique()
    {
        var normalized = ScramCredentialPolicy.NormalizeMetadataSet(
            "User:alice",
            new[]
            {
                new KafkaScramCredentialMetadata(
                    "User:alice",
                    KafkaScramMechanism.ScramSha512,
                    8192),
                new KafkaScramCredentialMetadata(
                    "User:alice",
                    KafkaScramMechanism.ScramSha256,
                    4096),
            });

        Assert.Equal(2, normalized.Count);
        Assert.Equal(KafkaScramMechanism.ScramSha256, normalized[0].Mechanism);
        Assert.Equal(KafkaScramMechanism.ScramSha512, normalized[1].Mechanism);

        Assert.Throws<ArgumentException>(() =>
            ScramCredentialPolicy.NormalizeMetadataSet(
                "User:alice",
                new[]
                {
                    new KafkaScramCredentialMetadata(
                        "User:bob",
                        KafkaScramMechanism.ScramSha256,
                        4096),
                }));

        Assert.Throws<ArgumentException>(() =>
            ScramCredentialPolicy.NormalizeMetadataSet(
                "User:alice",
                new[]
                {
                    new KafkaScramCredentialMetadata(
                        "User:alice",
                        KafkaScramMechanism.ScramSha256,
                        4096),
                    new KafkaScramCredentialMetadata(
                        "User:alice",
                        KafkaScramMechanism.ScramSha256,
                        8192),
                }));
    }

    [Fact]
    public void Provider_mapper_projects_only_supported_metadata()
    {
        var credential = new ScramCredentialInfo
        {
            Mechanism = ScramMechanism.ScramSha512,
            Iterations = 8192,
        };

        Assert.True(
            ConfluentKafkaScramMapper.TryFromProvider(
                "User:alice",
                credential,
                out var metadata));
        Assert.NotNull(metadata);
        Assert.Equal("User:alice", metadata!.User);
        Assert.Equal(KafkaScramMechanism.ScramSha512, metadata.Mechanism);
        Assert.Equal(8192, metadata.Iterations);

        credential.Mechanism = ScramMechanism.Unknown;
        Assert.False(
            ConfluentKafkaScramMapper.TryFromProvider(
                "User:alice",
                credential,
                out _));
    }

    [Fact]
    public void Pinned_client_exposes_typed_scram_describe_and_alter_symbols()
    {
        var describe = typeof(IAdminClient)
            .GetMethods()
            .Single(method =>
                method.Name ==
                nameof(IAdminClient.DescribeUserScramCredentialsAsync));

        Assert.Equal(
            typeof(Task<DescribeUserScramCredentialsResult>),
            describe.ReturnType);
        Assert.Equal(
            typeof(IEnumerable<string>),
            describe.GetParameters()[0].ParameterType);
        Assert.Equal(
            typeof(DescribeUserScramCredentialsOptions),
            describe.GetParameters()[1].ParameterType);

        var alter = typeof(IAdminClient)
            .GetMethods()
            .Single(method =>
                method.Name ==
                nameof(IAdminClient.AlterUserScramCredentialsAsync));

        Assert.Equal(typeof(Task), alter.ReturnType);
        Assert.Equal(
            typeof(IEnumerable<UserScramCredentialAlteration>),
            alter.GetParameters()[0].ParameterType);
        Assert.Equal(
            typeof(AlterUserScramCredentialsOptions),
            alter.GetParameters()[1].ParameterType);
    }

    [Fact]
    public void Observation_adapter_has_no_scram_mutation_surface()
    {
        var publicMethods = typeof(ConfluentKafkaScramObservationAdapter)
            .GetMethods()
            .Where(method => method.DeclaringType ==
                             typeof(ConfluentKafkaScramObservationAdapter))
            .Select(method => method.Name)
            .OrderBy(name => name, StringComparer.Ordinal)
            .ToArray();

        Assert.Equal(
            new[] { "DescribeUserAsync", "Dispose" },
            publicMethods);
    }

    [Fact]
    public void Scram_conflict_identity_binds_cluster_user_and_mechanism()
    {
        var baseline = ScramCredentialPolicy.ConflictIdentity(
            "prod",
            "User:alice",
            KafkaScramMechanism.ScramSha256);

        Assert.NotEqual(
            baseline,
            ScramCredentialPolicy.ConflictIdentity(
                "prod-dr",
                "User:alice",
                KafkaScramMechanism.ScramSha256));
        Assert.NotEqual(
            baseline,
            ScramCredentialPolicy.ConflictIdentity(
                "prod",
                "User:bob",
                KafkaScramMechanism.ScramSha256));
        Assert.NotEqual(
            baseline,
            ScramCredentialPolicy.ConflictIdentity(
                "prod",
                "User:alice",
                KafkaScramMechanism.ScramSha512));
    }
}
