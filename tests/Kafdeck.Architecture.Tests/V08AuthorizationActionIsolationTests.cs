using System.Text.Json;
using Kafdeck.Core.Security;
using Kafdeck.Infrastructure.Configuration;
using Kafdeck.Modules.Administration;
using Microsoft.Extensions.Configuration;
using Xunit;

namespace Kafdeck.Architecture.Tests;

public sealed class V08AuthorizationActionIsolationTests
{
    [Fact]
    public void Authorization_actions_are_unique_and_preserve_original_persisted_mutation_numbers()
    {
        var actions = Enum.GetValues<AuthorizationAction>();
        Assert.Equal(actions.Length, actions.Distinct().Count());
        string[] originalNames = ["SystemRead", "ClusterRead", "BrokerRead", "TopicList", "TopicRead", "TopicConfigRead",
            "BrokerConfigRead", "RecordRead", "RecordExport", "ConsumerRead", "SchemaRead", "ConnectRead", "KsqlRead",
            "CatalogRead", "TopicCreate", "TopicAlter", "TopicDelete", "RecordProduce", "ConsumerOffsetAlter", "ConsumerDelete",
            "SchemaCreate", "SchemaAlter", "SchemaDelete", "ConnectCreate", "ConnectAlter", "ConnectDelete", "RecordsPurge",
            "AclRead", "AclAlter", "ScramRead", "ScramAlter", "QuotaRead", "QuotaAlter", "ClusterConfigRead", "ClusterConfigAlter",
            "LeaderElect", "PartitionReassign", "ReplicationFactorAlter", "ReassignmentThrottle", "BrokerMaintenance", "LogdirMaintenance",
            "ClusterTransferPlan", "ClusterTransferExecute", "ReplicationIntegrationAlter", "MutationReconcile", "ConnectRestart",
            "ConnectAutoRestartManage", "DataJobPlan", "DataJobExecute", "DataGeneratorPlan", "DataGeneratorExecute",
            "DataQualityRead", "DataQualityManage", "NotificationRead", "NotificationManage"];
        for (var index = 0; index < originalNames.Length; index++)
            Assert.Equal(index + 1, (int)Enum.Parse<AuthorizationAction>(originalNames[index]));
        Assert.Equal(56, (int)AuthorizationAction.MutationApprove);
        Assert.Equal(57, (int)AuthorizationAction.StreamsRead);
        Assert.Equal(58, (int)AuthorizationAction.LineageRead);
    }

    [Theory]
    [InlineData(AuthorizationAction.StreamsRead, AuthorizationAction.ConnectRestart)]
    [InlineData(AuthorizationAction.StreamsRead, AuthorizationAction.ConnectAutoRestartManage)]
    [InlineData(AuthorizationAction.LineageRead, AuthorizationAction.ConnectRestart)]
    [InlineData(AuthorizationAction.LineageRead, AuthorizationAction.ConnectAutoRestartManage)]
    public void Streams_or_lineage_read_grant_never_authorizes_Connect_restart_or_policy_management(
        AuthorizationAction read, AuthorizationAction write)
    {
        var definition = new AuthorizationPolicyDefinition(
            [new("reader", [new(read, ["prod"], ["*"])])],
            [new("https://idp.example", "reader", ["reader"])], []);
        var evaluator = new AuthorizationPolicyEvaluator(AuthorizationPolicyCompiler.Compile(definition));
        var identity = new OperatorIdentity(new("https://idp.example", "reader"));
        Assert.True(evaluator.Evaluate(identity, new(read, "prod", "connector/orders")).IsAllowed);
        Assert.False(evaluator.Evaluate(identity, new(write, "prod", "connector/orders")).IsAllowed);
        Assert.False(evaluator.HasApplicablePermission(identity, write, "prod"));
    }

    [Theory]
    [InlineData("streams.read", AuthorizationAction.StreamsRead)]
    [InlineData("lineage-read", AuthorizationAction.LineageRead)]
    [InlineData("connect.restart", AuthorizationAction.ConnectRestart)]
    [InlineData("connect_auto_restart_manage", AuthorizationAction.ConnectAutoRestartManage)]
    [InlineData("mutation.approve", AuthorizationAction.MutationApprove)]
    public void Named_configuration_actions_resolve_to_their_distinct_authority(string configured, AuthorizationAction expected)
    {
        var policy = AuthorizationPolicyConfigurationLoader.Load(Configuration(configured));
        Assert.Equal(expected, Assert.Single(Assert.Single(policy.Roles).Permissions).Action);
    }

    [Theory]
    [InlineData("46")]
    [InlineData("47")]
    [InlineData("57")]
    [InlineData("58")]
    public void Numeric_role_configuration_is_rejected_instead_of_guessing_historical_read_or_write_intent(string configured) =>
        Assert.Throws<KafdeckConfigurationException>(() => AuthorizationPolicyConfigurationLoader.Load(Configuration(configured)));

    [Theory]
    [InlineData(46, AuthorizationAction.ConnectRestart)]
    [InlineData(47, AuthorizationAction.ConnectAutoRestartManage)]
    public void Numeric_persisted_mutation_authorization_targets_retain_the_original_Connect_semantics(int number, AuthorizationAction expected)
    {
        var options = new JsonSerializerOptions(JsonSerializerDefaults.Web);
        var target = JsonSerializer.Deserialize<MutationAuthorizationTarget>(
            $$"""{"action":{{number}},"clusterId":"prod","resourceName":"connector/orders"}""", options);
        Assert.NotNull(target);
        Assert.Equal(expected, target.Action);
        Assert.Contains($"\"action\":{number}", JsonSerializer.Serialize(target, options), StringComparison.Ordinal);
    }

    private static IConfiguration Configuration(string action) => new ConfigurationBuilder().AddInMemoryCollection(
        new Dictionary<string, string?>
        {
            ["Kafdeck:Authorization:Roles:0:Id"] = "fixture",
            ["Kafdeck:Authorization:Roles:0:Permissions:0:Action"] = action,
        }).Build();
}
