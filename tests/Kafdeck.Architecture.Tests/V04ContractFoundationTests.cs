using System.Reflection;
using System.Text.Json;
using Kafdeck.Core.Catalog;
using Kafdeck.Core.Consumers;
using Kafdeck.Core.Ecosystem;
using Kafdeck.Core.ReadViews;
using Kafdeck.Core.Schemas;
using Kafdeck.Core.Security;
using Kafdeck.Infrastructure.Configuration;
using Microsoft.Extensions.Configuration;
using Xunit;

namespace Kafdeck.Architecture.Tests;

public sealed class V04ContractFoundationTests
{
    [Fact]
    public void V04_read_actions_are_independent_from_record_access()
    {
        AuthorizationAction[] readViews =
        [
            AuthorizationAction.ConsumerRead,
            AuthorizationAction.SchemaRead,
            AuthorizationAction.ConnectRead,
            AuthorizationAction.KsqlRead,
            AuthorizationAction.CatalogRead,
        ];

        foreach (var action in readViews)
        {
            var policy = AuthorizationPolicyCompiler.Compile(
                new AuthorizationPolicyDefinition(
                    [
                        new AuthorizationRoleDefinition(
                            "reader",
                            [new AuthorizationPermissionDefinition(action, ["prod"], ["allowed*"])])
                    ],
                    [
                        new AuthorizationSubjectBindingDefinition(
                            "https://idp.example",
                            "alice",
                            ["reader"])
                    ],
                    Array.Empty<AuthorizationGroupBindingDefinition>()));

            var evaluator = new AuthorizationPolicyEvaluator(policy);
            var identity = new OperatorIdentity(new OperatorIdentityKey("https://idp.example", "alice"));

            Assert.True(evaluator.Evaluate(
                identity,
                new AuthorizationRequest(action, "prod", "allowed-resource")).IsAllowed);

            Assert.False(evaluator.Evaluate(
                identity,
                new AuthorizationRequest(AuthorizationAction.RecordRead, "prod", "allowed-resource")).IsAllowed);

            Assert.False(evaluator.Evaluate(
                identity,
                new AuthorizationRequest(AuthorizationAction.RecordExport, "prod", "allowed-resource")).IsAllowed);
        }
    }

    [Fact]
    public void V04_core_read_ports_expose_no_mutating_operation_names()
    {
        Type[] ports =
        [
            typeof(IConsumerGroupReadPort),
            typeof(ISchemaCatalogReadPort),
            typeof(IConnectReadPort),
            typeof(IKsqlMetadataReadPort),
            typeof(IMetricsObservationPort),
            typeof(IHistoryObservationPort),
            typeof(ITopicCatalogProvider),
        ];

        string[] forbidden =
        [
            "Create", "Alter", "Delete", "Commit", "Reset", "Shift",
            "Produce", "Replay", "Pause", "Resume", "Restart", "Execute", "Submit",
        ];

        foreach (var method in ports.SelectMany(port => port.GetMethods()))
        {
            Assert.DoesNotContain(
                forbidden,
                token => method.Name.Contains(token, StringComparison.OrdinalIgnoreCase));
        }
    }

    [Fact]
    public void External_read_ports_are_bounded_and_cancellable()
    {
        Type[] ports =
        [
            typeof(IConsumerGroupReadPort),
            typeof(ISchemaCatalogReadPort),
            typeof(IConnectReadPort),
            typeof(IKsqlMetadataReadPort),
            typeof(IMetricsObservationPort),
            typeof(IHistoryObservationPort),
        ];

        foreach (var method in ports.SelectMany(port => port.GetMethods()))
        {
            var parameterTypes = method.GetParameters().Select(parameter => parameter.ParameterType).ToArray();
            Assert.Contains(typeof(ReadViewOperationContext), parameterTypes);
            Assert.Contains(typeof(CancellationToken), parameterTypes);
        }
    }

    [Fact]
    public void Read_view_operation_context_rejects_unbounded_client_budgets()
    {
        Assert.Throws<ArgumentOutOfRangeException>(() =>
            new ReadViewOperationContext(
                DateTimeOffset.UtcNow.AddSeconds(10),
                ReadViewOperationContext.MaxAllowedItems + 1));

        Assert.Throws<ArgumentOutOfRangeException>(() =>
            new ReadViewOperationContext(
                DateTimeOffset.UtcNow.AddSeconds(10),
                maxResponseBytes: ReadViewOperationContext.MaxAllowedResponseBytes + 1));
    }

    [Fact]
    public void Loader_parses_v04_ecosystem_profiles_and_catalog_without_exposing_secrets()
    {
        var values = new Dictionary<string, string?>
        {
            ["Kafdeck:Deployment:ListenUrl"] = "http://127.0.0.1:8080",
            ["Kafdeck:Clusters:0:Id"] = "prod",
            ["Kafdeck:Clusters:0:BootstrapServers:0"] = "broker.example:9092",
            ["Kafdeck:Clusters:0:SecurityProtocol"] = "Plaintext",
            ["Kafdeck:Clusters:0:Connect:Url"] = "https://connect.example",
            ["Kafdeck:Clusters:0:Connect:Username"] = "env:KAFDECK_CONNECT_USER",
            ["Kafdeck:Clusters:0:Connect:Password"] = "env:KAFDECK_CONNECT_PASSWORD",
            ["Kafdeck:Clusters:0:KsqlDb:Url"] = "https://ksql.example",
            ["Kafdeck:Catalog:Topics:0:ClusterId"] = "prod",
            ["Kafdeck:Catalog:Topics:0:TopicName"] = "payments",
            ["Kafdeck:Catalog:Topics:0:Owner"] = "payments-team",
            ["Kafdeck:Catalog:Topics:0:Tags:0"] = "pci",
        };

        var configuration = new ConfigurationBuilder()
            .AddInMemoryCollection(values)
            .Build();

        var options = KafdeckConfigurationLoader.Load(configuration);
        KafdeckConfigurationValidator.ValidateAndThrow(options);

        Assert.NotNull(options.Clusters[0].Connect);
        Assert.NotNull(options.Clusters[0].KsqlDb);
        Assert.NotNull(options.Catalog);
        Assert.Single(options.Catalog!.Topics);
        Assert.Equal("payments", options.Catalog.Topics[0].TopicName);

        var diagnostics = JsonSerializer.Serialize(SafeConfigurationDiagnostics.Create(options));
        Assert.Contains("ConnectConfigured", diagnostics, StringComparison.Ordinal);
        Assert.Contains("KsqlDbConfigured", diagnostics, StringComparison.Ordinal);
        Assert.DoesNotContain("KAFDECK_CONNECT_USER", diagnostics, StringComparison.Ordinal);
        Assert.DoesNotContain("KAFDECK_CONNECT_PASSWORD", diagnostics, StringComparison.Ordinal);
    }

    [Fact]
    public void Remote_ecosystem_basic_auth_requires_https()
    {
        var cluster = new ClusterProfile(
            "prod",
            ["broker.example:9092"],
            KafkaSecurityProtocol.Plaintext,
            null,
            null,
            null,
            new KafkaConnectProfile(
                "http://connect.example",
                SecretReference.Parse("env:KAFDECK_CONNECT_USER"),
                SecretReference.Parse("env:KAFDECK_CONNECT_PASSWORD")));

        var options = new KafdeckOptions(
            new DeploymentOptions("http://127.0.0.1:8080", null),
            [cluster]);

        var exception = Assert.Throws<KafdeckConfigurationException>(
            () => KafdeckConfigurationValidator.ValidateAndThrow(options));

        Assert.Contains("requires HTTPS", exception.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void Ecosystem_base_urls_reject_embedded_credentials_query_and_fragment()
    {
        var cluster = new ClusterProfile(
            "prod",
            ["broker.example:9092"],
            KafkaSecurityProtocol.Plaintext,
            null,
            null,
            null,
            null,
            new KsqlDbProfile("https://user:pass@ksql.example/base?x=1#fragment", null, null));

        var options = new KafdeckOptions(
            new DeploymentOptions("http://127.0.0.1:8080", null),
            [cluster]);

        var exception = Assert.Throws<KafdeckConfigurationException>(
            () => KafdeckConfigurationValidator.ValidateAndThrow(options));

        Assert.Contains("must not contain user-info, query, or fragment", exception.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void Catalog_rejects_unknown_cluster_and_duplicate_topic_entries()
    {
        var options = new KafdeckOptions(
            new DeploymentOptions("http://127.0.0.1:8080", null),
            Array.Empty<ClusterProfile>(),
            null,
            new TopicCatalogOptions(
            [
                new TopicCatalogEntryProfile("missing", "payments", null, null, null, [], null, null),
                new TopicCatalogEntryProfile("missing", "payments", null, null, null, [], null, null),
            ]));

        var exception = Assert.Throws<KafdeckConfigurationException>(
            () => KafdeckConfigurationValidator.ValidateAndThrow(options));

        Assert.Contains("unknown cluster", exception.Message, StringComparison.Ordinal);
        Assert.Contains("duplicate entry", exception.Message, StringComparison.Ordinal);
    }
}
