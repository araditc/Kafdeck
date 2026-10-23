using System.Text.Json;
using Kafdeck.Api;
using Kafdeck.Core.Security;
using Kafdeck.Modules.Administration;
using Microsoft.Extensions.Configuration;
using Xunit;
using Kafdeck.Cli;

namespace Kafdeck.Architecture.Tests;

public sealed class V08W67FamilyBoundaryTests
{
    [Fact]
    public void Hosted_family_catalog_is_closed_and_does_not_claim_missing_runtime_families()
    {
        Assert.Equal(38, BearerFamilyRoutes.Contracts.Count);
        Assert.Equal(38, BearerFamilyRoutes.Contracts.Select(c => c.Name).Distinct().Count());
        Assert.Equal(13, BearerFamilyRoutes.Contracts.Select(c => c.Kind).Distinct().Count());
        Assert.False(BearerFamilyRoutes.IsKind(MutationOperationKind.AclAlter));
        Assert.False(BearerFamilyRoutes.IsKind(MutationOperationKind.PartitionReassign));
        Assert.False(BearerFamilyRoutes.IsAction(AuthorizationAction.NotificationManage));
        Assert.Equal("/api/v1/automation/mutations/{operationId:guid}/record-production/execute",
            BearerFamilyRoutes.Path(true, "/api/v1/mutations/{operationId:guid}/record-production/execute"));
        Assert.Equal("/api/v1/mutations/{operationId:guid}/record-production/execute",
            BearerFamilyRoutes.Path(false, "/api/v1/mutations/{operationId:guid}/record-production/execute"));
    }

    [Theory]
    [InlineData("{\"records\":[{\"value\":\"eA==\",\"headers\":[{\"name\":\"safe\",\"value\":\"eQ==\"}]}]}", true)]
    [InlineData("{\"records\":[{\"value\":\"eA==\",\"requesterPrincipalId\":\"override\"}]}", false)]
    [InlineData("{\"records\":[],\"Records\":[]}", false)]
    [InlineData("{\"records\":[{\"value\":\"eA==\",\"headers\":[{\"name\":\"safe\",\"value\":\"eQ==\",\"providerUrl\":\"secret\"}]}]}", false)]
    [InlineData("{\"records\":[],\"force\":true}", false)]
    public void Record_request_recursively_rejects_unknown_and_duplicate_fields(string json, bool expected)
    {
        using var document = JsonDocument.Parse(json);
        Assert.Equal(expected, BearerFamilyRoutes.IsClosed(document.RootElement, typeof(RecordProductionPreviewRequest)));
    }

    [Theory]
    [InlineData("{\"configuration\":{\"name\":\"connector\",\"connector.class\":\"class\"}}", true)]
    [InlineData("{\"configuration\":{\"name\":\"a\",\"name\":\"b\"}}", false)]
    [InlineData("{\"configuration\":{},\"operationKind\":15}", false)]
    public void Connect_configuration_is_typed_but_provider_keys_still_belong_to_existing_planner(string json, bool expected)
    {
        using var document = JsonDocument.Parse(json);
        Assert.Equal(expected, BearerFamilyRoutes.IsClosed(document.RootElement, typeof(ConnectConfigurationPreviewRequest)));
    }

    [Fact]
    public void Material_route_cannot_cross_families_or_require_unrelated_alter_grant_for_create()
    {
        var configuration = BearerFamilyRoutes.Contracts.Single(c => c.OriginalName == "v05-connect-configuration-execute");
        Assert.True(configuration.AcceptsKind(MutationOperationKind.ConnectCreate));
        Assert.True(configuration.AcceptsKind(MutationOperationKind.ConnectAlter));
        Assert.False(configuration.AcceptsKind(MutationOperationKind.ConnectDelete));
        Assert.False(configuration.AcceptsKind(MutationOperationKind.RecordProduce));
        var records = BearerFamilyRoutes.Contracts.Single(c => c.OriginalName == "v05-record-production-execute");
        Assert.True(records.AcceptsKind(MutationOperationKind.RecordProduce));
        Assert.False(records.AcceptsKind(MutationOperationKind.TopicDelete));
    }

    [Fact]
    public void Explicit_key_revocation_is_immediate_and_malformed_reloads_fail_closed()
    {
        var values = new Dictionary<string, string?>
        {
            ["Kafdeck:NonBrowserRead:Enabled"] = "true", ["Kafdeck:NonBrowserRead:Issuer"] = "https://issuer.example/realms/test",
            ["Kafdeck:NonBrowserRead:ApiAudience"] = "api", ["Kafdeck:NonBrowserRead:Registrations:0:Id"] = "service",
            ["Kafdeck:NonBrowserRead:Registrations:0:OwnerId"] = "owner", ["Kafdeck:NonBrowserRead:Registrations:0:Subject"] = "subject",
            ["Kafdeck:NonBrowserRead:Registrations:0:ClientId"] = "client", ["Kafdeck:NonBrowserRead:Registrations:0:ReadActions:0"] = "SystemRead",
        };
        var configuration = new ConfigurationBuilder().AddInMemoryCollection(values).Build();
        var registry = new NonBrowserReadRegistry(configuration);
        Assert.True(registry.IsSigningKeyAllowed("key-A"));
        configuration["Kafdeck:NonBrowserRead:RevokedSigningKeyIds:0"] = "key-A";
        Assert.False(registry.IsSigningKeyAllowed("key-A"));
        Assert.True(registry.IsSigningKeyAllowed("key-B"));
        configuration["Kafdeck:NonBrowserRead:RevokedSigningKeyIds:1"] = "key-A";
        Assert.False(registry.IsSigningKeyAllowed("key-B"));
    }

    [Theory]
    [InlineData("records", "produce-preview", "cluster", "orders", "mutations/produce")]
    [InlineData("consumer-groups", "offsets-preview", "cluster", "group", "consumer-groups")]
    [InlineData("schemas", "register-preview", "cluster", "subject", "schemas")]
    [InlineData("connect", "create-preview", "cluster", "connector", "connect")]
    public void Named_cli_family_previews_require_closed_file_idempotency_and_nonbrowser_auth(string family, string command, string first, string second, string routeSegment)
    {
        string[] args = ["--service-profile", "service.json", family, command, first, second, "--request-file", "private-request.json", "--idempotency-key", "key"];
        var invocation = CliParser.Parse(args);
        Assert.Equal(CliCommand.FamilyMutation, invocation.Command);
        Assert.Contains("/" + routeSegment + "/", CliRouteBuilder.Build(invocation), StringComparison.Ordinal);
        Assert.True(invocation.Family!.Preview);
        Assert.Throws<CliUsageException>(() => CliParser.Parse(args.Take(args.Length - 2).ToArray()));
        Assert.Throws<CliUsageException>(() => CliParser.Parse([family, command, first, second, "--request-file", "private-request.json", "--idempotency-key", "key"]));
    }

    [Theory]
    [InlineData("{\"records\":[{\"value\":\"eA==\"}]}", true)]
    [InlineData("{\"records\":[{\"value\":\"eA==\",\"actor\":\"spoof\"}]}", false)]
    [InlineData("{\"records\":[],\"records\":[]}", false)]
    [InlineData("{\"records\":[],\"operationKind\":\"scramAlter\"}", false)]
    public void Client_payload_is_closed_recursively_before_token_acquisition(string json, bool expected)
    {
        using var document = JsonDocument.Parse(json);
        Assert.Equal(expected, CliFamilyCommands.ValidatePayload(document.RootElement, CliFamilyPayload.RecordsPreview));
    }

    [Fact]
    public void Recovery_commands_require_human_identity_and_preserve_approval_not_provider_success()
    {
        var status = CliParser.Parse(["--oidc-profile", "human.json", "notifications", "recovery", "status"]);
        Assert.Equal("/api/v1/automation/notifications/recovery/status", CliRouteBuilder.Build(status));
        Assert.True(status.Family!.Read);
        Assert.Throws<CliUsageException>(() => CliParser.Parse(["--service-profile", "service.json", "notifications", "recovery", "status"]));
        var preview = CliParser.Parse(["--oidc-profile", "human.json", "notifications", "recovery", "preview", "--request-file", "recovery.json"]);
        Assert.Null(preview.IdempotencyKey);
        Assert.Equal(CliApplication.PendingApproval, CliApplication.RecoveryOutcomeCategory("{\"state\":\"Previewed\"}"));
        Assert.Equal(CliApplication.Success, CliApplication.RecoveryOutcomeCategory("{\"state\":\"Approved\"}"));
        Assert.Equal(CliApplication.UnknownEffect, CliApplication.MutationOutcomeCategory("{\"state\":\"executionUnknown\"}"));
    }

    [Theory]
    [InlineData("Observing", false, 7)]
    [InlineData("Unknown", false, 7)]
    [InlineData("Completed", false, 0)]
    [InlineData("Stopped", false, 4)]
    [InlineData("Stopped", true, 0)]
    [InlineData("PausedAuthorization", false, 3)]
    public void Runtime_activation_never_claims_background_effect_completed(string phase, bool stop, int expected)
    {
        var json = JsonSerializer.Serialize(new { mutationState = "AppliedVerified", progressPhase = phase });
        Assert.Equal(expected, CliApplication.RuntimeProgressOutcomeCategory(json, stop));
        Assert.Equal(CliApplication.UnknownEffect, CliApplication.RuntimeProgressOutcomeCategory(
            "{\"mutationState\":\"AppliedVerified\",\"progressPhase\":\"Stopped\",\"pendingBatch\":{\"state\":\"DispatchStarted\"}}", true));
    }

    [Theory]
    [InlineData("data-jobs", "replay-preview")]
    [InlineData("data-jobs", "reprocess-preview")]
    [InlineData("generator", "preview")]
    public void Job_previews_are_named_closed_payloads_with_no_caller_handler(string family, string command)
    {
        var invocation = CliParser.Parse(["--service-profile", "service.json", family, command,
            "--request-file", "job.json", "--idempotency-key", "key"]);
        Assert.True(invocation.Family!.Preview);
        Assert.StartsWith("/api/v1/automation/data-jobs/", CliRouteBuilder.Build(invocation), StringComparison.Ordinal);
        Assert.Throws<CliUsageException>(() => CliParser.Parse(["--oidc-profile", "human.json", "data-jobs", "raw-handler", "--request-file", "job.json"]));
    }

    [Fact]
    public async Task Authenticated_slow_body_has_a_total_deadline_before_binding()
    {
        var context = new Microsoft.AspNetCore.Http.DefaultHttpContext();
        context.Request.Body = new StalledBody();
        var contract = BearerFamilyRoutes.Contracts.Single(c => c.OriginalName == "v05-record-production-preview");
        Assert.False(await BearerFamilyRoutes.ValidateBodyAsync(context.Request, contract, CancellationToken.None, TimeSpan.FromMilliseconds(50)));
    }

    private sealed class StalledBody : Stream
    {
        public override bool CanRead => true;
        public override bool CanSeek => false;
        public override bool CanWrite => false;
        public override long Length => throw new NotSupportedException();
        public override long Position { get => throw new NotSupportedException(); set => throw new NotSupportedException(); }
        public override void Flush() => throw new NotSupportedException();
        public override int Read(byte[] buffer, int offset, int count) => throw new NotSupportedException();
        public override async ValueTask<int> ReadAsync(Memory<byte> buffer, CancellationToken token = default)
        { await Task.Delay(Timeout.Infinite, token); return 0; }
        public override long Seek(long offset, SeekOrigin origin) => throw new NotSupportedException();
        public override void SetLength(long value) => throw new NotSupportedException();
        public override void Write(byte[] buffer, int offset, int count) => throw new NotSupportedException();
    }
}
