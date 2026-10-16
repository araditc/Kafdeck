using System.Net;
using System.Net.Http.Json;
using System.Security.Claims;
using System.Text.Json;
using Kafdeck.Api;
using Kafdeck.Core.Notifications;
using Kafdeck.Core.Security;
using Kafdeck.Infrastructure.Configuration;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Xunit;

namespace Kafdeck.Architecture.Tests;

/// <summary>Real HTTP endpoints with an explicit preverified-identity seam. Genuine issuer/token/CLI proof is separate.</summary>
public sealed class V08W66RecoveryHttpBoundaryTests
{
    [Theory]
    [InlineData("service")]
    [InlineData("expired")]
    [InlineData("unregistered")]
    [InlineData("scoped-resource")]
    [InlineData("scoped-cluster")]
    [InlineData("read-only")]
    public async Task Only_current_human_unconstrained_global_management_can_create_recovery_plans(string scenario)
    {
        await using var h = await HttpFixture.CreateAsync(scenario);
        using var result = await h.PostAsync("preview", h.Preview(NotificationRecoveryCommand.FenceBatch), scenario is "service" or "expired" or "unregistered" ? scenario : "requester");
        Assert.Equal(HttpStatusCode.Forbidden, result.StatusCode);
        Assert.Equal(0L, Convert.ToInt64(await h.Database.ScalarAsync("SELECT COUNT(*) FROM notification_recovery_plans")));
        Assert.Equal(h.Database.Initial, (await h.Database.Store.StatusAsync(default)).DatabaseGeneration);
    }

    [Theory]
    [InlineData("same-human", 409)]
    [InlineData("revoked-approver", 403)]
    [InlineData("revoked-requester", 403)]
    [InlineData("changed-requester-registration", 403)]
    [InlineData("stale-revision", 409)]
    [InlineData("changed-anchor", 409)]
    public async Task Exact_human_distinction_current_registration_and_preview_CAS_are_enforced_by_actual_HTTP_handlers(string scenario, int status)
    {
        await using var h = await HttpFixture.CreateAsync();
        var preview = await h.PlanAsync("preview", h.Preview(NotificationRecoveryCommand.FenceBatch));
        Assert.Single(preview.Items);
        Assert.Empty(preview.SourceEventIds);
        using var approval = await h.PostAsync($"{preview.OperationId:D}/approve", new NotificationRecoveryRevisionRequest(preview.Revision, preview.PreviewHash),
            scenario == "same-human" ? "same-human" : "approver");
        if (scenario == "same-human") Assert.Equal(status, (int)approval.StatusCode);
        else
        {
            Assert.Equal(HttpStatusCode.OK, approval.StatusCode);
            var approved = (await approval.Content.ReadFromJsonAsync<NotificationRecoveryData>())!;
            if (scenario == "revoked-approver") h.Configuration["Kafdeck:NonBrowserRead:HumanRegistrations:2:Enabled"] = "false";
            if (scenario == "revoked-requester") h.Configuration["Kafdeck:NonBrowserRead:HumanRegistrations:0:Enabled"] = "false";
            if (scenario == "changed-anchor") await File.WriteAllBytesAsync(h.Database.Anchor,
                JsonSerializer.SerializeToUtf8Bytes(new { version = 1, generation = h.Database.Target, state = "active" }));
            using var apply = await h.PostAsync($"{approved.OperationId:D}/apply",
                new NotificationRecoveryRevisionRequest(approved.Revision + (scenario == "stale-revision" ? 1 : 0), approved.PreviewHash),
                scenario == "changed-requester-registration" ? "same-human" : "requester");
            Assert.Equal(status, (int)apply.StatusCode);
        }
        Assert.Equal("Pending", await h.Database.ScalarAsync("SELECT state FROM kafdeck_notification_deliveries"));
        Assert.Equal(0L, Convert.ToInt64(await h.Database.ScalarAsync("SELECT COUNT(*) FROM notification_recovery_receipts")));
        Assert.Equal(h.Database.Initial, (await h.Database.Store.StatusAsync(default)).DatabaseGeneration);
    }

    [Fact]
    public async Task Real_HTTP_two_human_fence_then_epoch_advance_returns_reviewable_receipts_and_never_activates_outside_anchor()
    {
        await using var h = await HttpFixture.CreateAsync();
        var sourceId = Guid.NewGuid(); h.Database.Source.Ids.Add(sourceId);
        var initial = await h.PlanAsync("preview", h.Preview(NotificationRecoveryCommand.FenceBatch));
        Assert.Single(initial.Items);
        using (var unapproved = await h.PostAsync($"{initial.OperationId:D}/apply", new NotificationRecoveryRevisionRequest(initial.Revision, initial.PreviewHash)))
            Assert.Equal(HttpStatusCode.Conflict, unapproved.StatusCode);
        var approved = await h.PlanAsync($"{initial.OperationId:D}/approve", new NotificationRecoveryRevisionRequest(initial.Revision, initial.PreviewHash), "approver");
        var applied = await h.PlanAsync($"{initial.OperationId:D}/apply", new NotificationRecoveryRevisionRequest(approved.Revision, approved.PreviewHash));
        Assert.Equal("Applied", applied.State);
        Assert.Equal(0, applied.RemainingUnreconciled);
        Assert.False(applied.ReadyForOperatorActivation);
        var source = await h.PlanAsync("preview", h.Preview(NotificationRecoveryCommand.FenceSourceBatch));
        Assert.Equal(sourceId, Assert.Single(source.SourceEventIds));
        Assert.Empty(source.Items);
        var sourceApproval = await h.PlanAsync($"{source.OperationId:D}/approve", new NotificationRecoveryRevisionRequest(source.Revision, source.PreviewHash), "approver");
        await h.PlanAsync($"{source.OperationId:D}/apply", new NotificationRecoveryRevisionRequest(sourceApproval.Revision, sourceApproval.PreviewHash));
        var advance = await h.PlanAsync("preview", h.Preview(NotificationRecoveryCommand.AdvanceGeneration));
        var coapproved = await h.PlanAsync($"{advance.OperationId:D}/approve", new NotificationRecoveryRevisionRequest(advance.Revision, advance.PreviewHash), "approver");
        var complete = await h.PlanAsync($"{advance.OperationId:D}/apply", new NotificationRecoveryRevisionRequest(coapproved.Revision, coapproved.PreviewHash));
        Assert.True(complete.ReadyForOperatorActivation);
        Assert.NotEqual(complete.RequesterPrincipalId, complete.ApprovedByPrincipalId);
        using var detail = new HttpRequestMessage(HttpMethod.Get, $"{Prefix}/{complete.OperationId:D}"); detail.Headers.Add("X-Test-Actor", "requester");
        using var response = await h.Client.SendAsync(detail);
        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
        Assert.Equal("UnknownExternalEffect", await h.Database.ScalarAsync("SELECT state FROM kafdeck_notification_deliveries"));
        Assert.Equal(9L, Convert.ToInt64(await h.Database.ScalarAsync("SELECT COUNT(*) FROM notification_recovery_audit")));
        using var anchor = JsonDocument.Parse(await File.ReadAllBytesAsync(h.Database.Anchor));
        Assert.Equal("quarantined", anchor.RootElement.GetProperty("state").GetString());
    }

    [Theory]
    [InlineData("{\"command\":\"FenceBatch\",\"command\":\"AdvanceGeneration\"}")]
    [InlineData("{\"command\":\"FenceBatch\",\"force\":true}")]
    [InlineData("{\"expectedRevision\":2,\"expectedRevision\":3,\"previewHash\":\"a\"}")]
    public void Named_recovery_requests_reject_duplicate_or_undeclared_fields_before_binding(string body)
    {
        using var json = JsonDocument.Parse(body);
        Assert.False(BearerFamilyRoutes.IsClosed(json.RootElement, body.Contains("expectedRevision", StringComparison.Ordinal)
            ? typeof(NotificationRecoveryRevisionRequest) : typeof(NotificationRecoveryPreviewRequest)));
    }

    private const string Prefix = "/api/v1/automation/notifications/recovery";
    private const string Issuer = "https://issuer.example/realms/recovery";
    private sealed class HttpFixture(WebApplication app, V08W66GovernedRecoveryTests.Fixture database,
        HttpClient client, IConfigurationRoot configuration) : IAsyncDisposable
    {
        internal V08W66GovernedRecoveryTests.Fixture Database => database;
        internal HttpClient Client => client;
        internal IConfigurationRoot Configuration => configuration;
        internal NotificationRecoveryPreviewRequest Preview(NotificationRecoveryCommand command)
        {
            var r = database.Request(command);
            return new(r.Command.ToString(), r.Disposition?.ToString(), r.ProofReferenceSha256, r.FencedSendersEvidenceSha256,
                r.ExpectedDatabaseGeneration, r.TargetGeneration, r.ExpectedAnchorSha256, r.MaxItems);
        }
        internal async Task<HttpResponseMessage> PostAsync(string path, object body, string actor = "requester")
        {
            using var request = new HttpRequestMessage(HttpMethod.Post, Prefix + "/" + path) { Content = JsonContent.Create(body, body.GetType()) };
            request.Headers.Add("X-Test-Actor", actor);
            return await client.SendAsync(request);
        }
        internal async Task<NotificationRecoveryData> PlanAsync(string path, object body, string actor = "requester")
        {
            using var response = await PostAsync(path, body, actor);
            Assert.Equal(HttpStatusCode.OK, response.StatusCode);
            return (await response.Content.ReadFromJsonAsync<NotificationRecoveryData>())!;
        }
        internal static async Task<HttpFixture> CreateAsync(string? scenario = null)
        {
            var database = await V08W66GovernedRecoveryTests.Fixture.CreateAsync();
            await database.SeedAsync();
            var config = new Dictionary<string, string?>
            {
                ["Kafdeck:NonBrowserRead:Enabled"] = "true", ["Kafdeck:NonBrowserRead:Issuer"] = Issuer,
                ["Kafdeck:NonBrowserRead:ApiAudience"] = "api", ["Kafdeck:NonBrowserRead:GovernedMutationsEnabled"] = "true",
                ["Kafdeck:NonBrowserRead:GovernedApprovalsEnabled"] = "true",
            };
            foreach (var (index, id, subject, clientId) in new[] { (0, "requester", "alice", "cli-one"), (1, "same-human", "alice", "cli-two"), (2, "approver", "bob", "cli-one") })
            {
                var p = $"Kafdeck:NonBrowserRead:HumanRegistrations:{index}:";
                config[p + "Id"] = id; config[p + "OwnerId"] = "owner"; config[p + "Subject"] = subject; config[p + "ClientId"] = clientId;
                config[p + "Enabled"] = "true"; config[p + "ExpiresAtUtc"] = DateTimeOffset.UtcNow.AddMinutes(10).ToString("O");
                config[p + "ReadActions:0"] = "SystemRead";
                // The approver deliberately has approval-only registration.
                if (id != "approver") config[p + "MutationActions:0"] = "NotificationManage";
                config[p + "ApprovalActions:0"] = "NotificationManage";
            }
            var configuration = new ConfigurationBuilder().AddInMemoryCollection(config).Build();
            var permission = new AuthorizationPermissionDefinition(scenario == "read-only" ? AuthorizationAction.NotificationRead : AuthorizationAction.NotificationManage,
                scenario == "scoped-cluster" ? ["prod"] : [], scenario == "scoped-resource" ? ["*"] : []);
            var evaluator = new AuthorizationPolicyEvaluator(AuthorizationPolicyCompiler.Compile(new(
                [new("operator", [permission, new(AuthorizationAction.MutationApprove, [], [])])],
                [new(Issuer, "alice", ["operator"]), new(Issuer, "bob", ["operator"]), new(Issuer, "machine", ["operator"])], [])));
            var authorization = new KafdeckAuthorizationService(new(new DeploymentOptions("http://127.0.0.1:0", null, AccessMode.Oidc, null), []), evaluator, new(configuration));
            var b = WebApplication.CreateBuilder(); b.Logging.ClearProviders(); b.WebHost.UseUrls("http://127.0.0.1:0");
            b.Services.AddSingleton(authorization); b.Services.AddSingleton<INotificationRecoveryStore>(database.Store);
            b.Services.AddSingleton(new NotificationRecoveryOptions(database.Target, database.Anchor, database.Hash));
            var app = b.Build();
            app.Use((context, next) =>
            {
                var actor = context.Request.Headers["X-Test-Actor"].ToString();
                var kind = actor == "service" ? NonBrowserPrincipalKind.Service : NonBrowserPrincipalKind.Human;
                var subject = actor == "approver" ? "bob" : actor == "service" ? "machine" : "alice";
                var clientId = actor == "same-human" ? "cli-two" : "cli-one";
                var expiry = DateTimeOffset.UtcNow.AddMinutes(actor == "expired" ? -1 : 2);
                context.User = new ClaimsPrincipal(new NonBrowserReadIdentity(actor, new(Issuer, subject, clientId, "api", expiry), kind));
                return next(context);
            });
            app.MapKafdeckNotificationRecovery();
            await app.StartAsync();
            return new(app, database, new(new SocketsHttpHandler { UseProxy = false })
                { BaseAddress = new Uri(Assert.Single(app.Urls)), Timeout = TimeSpan.FromSeconds(20) }, configuration);
        }
        public async ValueTask DisposeAsync()
        { client.Dispose(); await app.StopAsync(); await app.DisposeAsync(); await database.DisposeAsync(); }
    }
}
