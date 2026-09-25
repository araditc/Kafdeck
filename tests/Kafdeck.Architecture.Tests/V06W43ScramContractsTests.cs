using Confluent.Kafka;
using Confluent.Kafka.Admin;
using Kafdeck.Infrastructure.Kafka;
using Kafdeck.Modules.Administration;
using System.Text;
using System.Text.Json;
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
    public void Scram_material_binding_is_domain_bound_and_fixed_time_matchable()
    {
        using var digest = new HmacMutationMaterialDigestService(
            new string('k', 32));
        var password = Encoding.UTF8.GetBytes("synthetic-w43-secret");
        try
        {
            var descriptor = new ScramCredentialBindingDescriptor(
                "prod",
                "User:alice",
                KafkaScramMechanism.ScramSha256,
                4096);

            var binding = ScramCredentialMaterialBinding.Compute(
                digest,
                descriptor,
                password);

            Assert.True(
                ScramCredentialMaterialBinding.Matches(
                    digest,
                    descriptor,
                    password,
                    binding));
            Assert.False(
                ScramCredentialMaterialBinding.Matches(
                    digest,
                    descriptor with { User = "User:bob" },
                    password,
                    binding));
            Assert.False(
                ScramCredentialMaterialBinding.Matches(
                    digest,
                    descriptor with
                    {
                        Mechanism = KafkaScramMechanism.ScramSha512,
                    },
                    password,
                    binding));
            Assert.False(
                ScramCredentialMaterialBinding.Matches(
                    digest,
                    descriptor with { Iterations = 8192 },
                    password,
                    binding));
            Assert.False(
                ScramCredentialMaterialBinding.Matches(
                    digest,
                    descriptor with { ClusterId = "prod-dr" },
                    password,
                    binding));
        }
        finally
        {
            System.Security.Cryptography.CryptographicOperations.ZeroMemory(
                password);
        }
    }

    [Fact]
    public void Common_executor_digest_matches_the_metadata_bound_scram_envelope()
    {
        using var digest = new HmacMutationMaterialDigestService(
            new string('k', 32));
        var password = Encoding.UTF8.GetBytes("synthetic-w43-secret");
        var descriptor = new ScramCredentialBindingDescriptor(
            "prod",
            "User:alice",
            KafkaScramMechanism.ScramSha512,
            8192);
        var envelope = ScramCredentialMaterialCodec.Encode(
            descriptor,
            password);

        try
        {
            var expected = digest.ComputeDigest(envelope);
            var w43Binding = ScramCredentialMaterialBinding.Compute(
                digest,
                descriptor,
                password);

            Assert.Equal(expected, w43Binding);

            using var decoded =
                ScramCredentialMaterialCodec.Decode(envelope);
            Assert.Equal(descriptor, decoded.Descriptor);
            Assert.True(
                decoded.Password.Span.SequenceEqual(password));
        }
        finally
        {
            System.Security.Cryptography.CryptographicOperations.ZeroMemory(
                envelope);
            System.Security.Cryptography.CryptographicOperations.ZeroMemory(
                password);
        }
    }

    [Fact]
    public void Scram_execution_envelope_rejects_metadata_or_shape_tampering()
    {
        var password = Encoding.UTF8.GetBytes("synthetic-w43-secret");
        var envelope = ScramCredentialMaterialCodec.Encode(
            new ScramCredentialBindingDescriptor(
                "prod",
                "User:alice",
                KafkaScramMechanism.ScramSha256,
                4096),
            password);

        try
        {
            envelope[0] ^= 0x01;
            Assert.Throws<MutationStateException>(() =>
                ScramCredentialMaterialCodec.Decode(envelope));
        }
        finally
        {
            System.Security.Cryptography.CryptographicOperations.ZeroMemory(
                envelope);
            System.Security.Cryptography.CryptographicOperations.ZeroMemory(
                password);
        }
    }

    [Fact]
    public void Scram_execution_material_is_json_hidden_and_zeroed_on_dispose()
    {
        var source = Encoding.UTF8.GetBytes("synthetic-w43-secret");
        try
        {
            var material = new ScramCredentialExecutionMaterial(source);
            var borrowed = material.Password;

            Assert.Equal("{}", JsonSerializer.Serialize(material));
            Assert.Contains(borrowed.Span.ToArray(), value => value != 0);

            material.Dispose();

            Assert.All(borrowed.Span.ToArray(), value => Assert.Equal(0, value));
            Assert.Throws<ObjectDisposedException>(() =>
            {
                _ = material.Password;
            });
        }
        finally
        {
            System.Security.Cryptography.CryptographicOperations.ZeroMemory(
                source);
        }
    }

    [Fact]
    public void Scram_mutation_port_keeps_password_outside_safe_request_records()
    {
        Assert.DoesNotContain(
            typeof(ScramUpsertMutation).GetProperties(),
            property =>
                property.Name.Contains(
                    "Password",
                    StringComparison.OrdinalIgnoreCase) ||
                property.PropertyType == typeof(byte[]));

        var upsert = typeof(IScramMutationPort)
            .GetMethod(nameof(IScramMutationPort.UpsertAsync));
        Assert.NotNull(upsert);
        Assert.Equal(
            typeof(ScramUpsertMutation),
            upsert!.GetParameters()[0].ParameterType);
        Assert.Equal(
            typeof(ReadOnlyMemory<byte>),
            upsert.GetParameters()[1].ParameterType);

        var adapterMethods =
            typeof(ConfluentKafkaScramMutationAdapter)
                .GetMethods()
                .Where(method =>
                    method.DeclaringType ==
                    typeof(ConfluentKafkaScramMutationAdapter))
                .Select(method => method.Name)
                .OrderBy(name => name, StringComparer.Ordinal)
                .ToArray();

        Assert.Equal(
            new[] { "DeleteAsync", "Dispose", "UpsertAsync" },
            adapterMethods);
    }

    [Fact]
    public void Scram_provider_error_classifier_preserves_ambiguous_outcomes()
    {
        var timeout = ConfluentKafkaScramMutationResultClassifier
            .ClassifyException(
                "scram_upsert",
                "User:alice",
                new[]
                {
                    new AlterUserScramCredentialsReport
                    {
                        User = "User:alice",
                        Error = new Error(ErrorCode.Local_TimedOut),
                    },
                });
        Assert.Equal(
            MutationExecutionResultKind.ExecutionUnknown,
            timeout.ResultKind);

        var denied = ConfluentKafkaScramMutationResultClassifier
            .ClassifyException(
                "scram_upsert",
                "User:alice",
                new[]
                {
                    new AlterUserScramCredentialsReport
                    {
                        User = "User:alice",
                        Error = new Error(
                            ErrorCode.ClusterAuthorizationFailed),
                    },
                });
        Assert.Equal(
            MutationExecutionResultKind.FailedDefinitive,
            denied.ResultKind);

        var unexpected = ConfluentKafkaScramMutationResultClassifier
            .ClassifyException(
                "scram_delete",
                "User:alice",
                new[]
                {
                    new AlterUserScramCredentialsReport
                    {
                        User = "User:bob",
                        Error = new Error(
                            ErrorCode.ClusterAuthorizationFailed),
                    },
                });
        Assert.Equal(
            MutationExecutionResultKind.ExecutionUnknown,
            unexpected.ResultKind);
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
