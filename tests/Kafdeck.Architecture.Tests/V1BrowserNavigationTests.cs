using System.Net;
using System.Net.Http.Json;
using System.Security.Claims;
using System.Text.Json;
using Kafdeck.Api;
using Kafdeck.Core.Security;
using Kafdeck.Infrastructure.Configuration;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.Extensions.DependencyInjection;
using Xunit;

namespace Kafdeck.Architecture.Tests;

public sealed class V1BrowserNavigationTests
{
    private static readonly string[] Keys = ["ownershipCatalog", "governancePolicy", "serviceRequests", "governanceAttribution"];

    [Theory]
    [InlineData("owner")]
    [InlineData("manager")]
    [InlineData("operator")]
    [InlineData("viewer")]
    public async Task Managed_roles_never_imply_governance_authority(string role)
    {
        await using var f = await NavigationLab.Start(managed: true, role: role);
        using var value = await f.Read("connection-a");
        Assert.All(Keys, key => Assert.False(value.RootElement.GetProperty(key).GetBoolean()));
        Assert.Equal(KafdeckAuthorizationOutcome.Forbidden,
            f.Auth.AuthorizeCollection(f.Actor, AuthorizationAction.CatalogRead, "connection-a"));
        Assert.Empty(f.Identity.Audit.Events);
    }

    [Theory]
    [InlineData(AuthorizationAction.CatalogRead, "ownershipCatalog")]
    [InlineData(AuthorizationAction.GovernancePolicyRead, "governancePolicy")]
    [InlineData(AuthorizationAction.RequestRead, "serviceRequests")]
    [InlineData(AuthorizationAction.AttributionRead, "governanceAttribution")]
    public async Task External_subject_grants_are_independent_and_cluster_scoped(AuthorizationAction action, string key)
    {
        await using var f = await NavigationLab.Start(action: action);
        using var allowed = await f.Read("connection-a");
        Assert.All(Keys, name => Assert.Equal(name == key, allowed.RootElement.GetProperty(name).GetBoolean()));
        using var other = await f.Read("connection-b");
        Assert.All(Keys, name => Assert.False(other.RootElement.GetProperty(name).GetBoolean()));
        Assert.Equal(KafdeckAuthorizationOutcome.Forbidden, f.Auth.Authorize(f.Actor, new(AuthorizationAction.CatalogManage, "connection-a", "catalog:team:a")));
    }

    [Fact]
    public async Task Unbound_external_user_has_no_navigation_grant()
    {
        await using var f = await NavigationLab.Start();
        using var value = await f.Read("connection-a");
        Assert.All(Keys, key => Assert.False(value.RootElement.GetProperty(key).GetBoolean()));
    }

    [Fact]
    public async Task Resource_restriction_does_not_enable_a_page_whose_active_read_is_denied()
    {
        await using var f = await NavigationLab.Start(action: AuthorizationAction.GovernancePolicyRead, pattern: "version-*");
        Assert.Equal(KafdeckAuthorizationOutcome.Allowed, f.Auth.AuthorizeCollection(f.Actor, AuthorizationAction.GovernancePolicyRead, "connection-a"));
        using var value = await f.Read("connection-a");
        Assert.False(value.RootElement.GetProperty("governancePolicy").GetBoolean());
    }

    [Fact]
    public async Task Current_managed_disable_is_denied_even_for_an_existing_browser_session()
    {
        await using var f = await NavigationLab.Start(managed: true, role: "manager");
        using var before = await f.Read("connection-a");
        var state = f.Identity.Store.Read();
        f.Identity.Store.Write(state with { Users = state.Users.Select(u => u.Id == f.ActorId ? u with { Enabled = false } : u).ToArray() });
        using var after = await f.Http.GetAsync("/api/v1/auth/navigation?clusterId=connection-a");
        Assert.Equal(HttpStatusCode.Forbidden, after.StatusCode);
        Assert.Equal("no-store", after.Headers.CacheControl!.ToString());
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task Anonymous_and_nonbrowser_identities_cannot_discover_browser_navigation(bool bearer)
    {
        await using var f = await NavigationLab.Start();
        f.Actor = bearer ? new(new NonBrowserReadIdentity("reader", new(ManagedProvisioningFixture.Issuer, "subject", "client", "api", DateTimeOffset.UtcNow.AddMinutes(1))))
            : new(new ClaimsIdentity());
        using var response = await f.Http.GetAsync("/api/v1/auth/navigation?clusterId=connection-a");
        Assert.Equal(HttpStatusCode.Unauthorized, response.StatusCode);
        Assert.Equal("no-store", response.Headers.CacheControl!.ToString());
    }

    [Theory]
    [InlineData("")]
    [InlineData("%0A")]
    [InlineData("oversized")]
    public async Task Invalid_cluster_input_is_rejected(string input)
    {
        await using var f = await NavigationLab.Start();
        using var response = await f.Http.GetAsync("/api/v1/auth/navigation?clusterId=" + (input == "oversized" ? new string('x', 257) : input));
        Assert.Equal(HttpStatusCode.BadRequest, response.StatusCode);
    }

    private sealed class NavigationLab : IAsyncDisposable
    {
        internal readonly ManagedProvisioningFixture Identity = new();
        internal ClaimsPrincipal Actor = null!;
        internal Guid ActorId;
        internal KafdeckAuthorizationService Auth = null!;
        internal WebApplication App = null!;
        internal HttpClient Http = null!;
        internal static async Task<NavigationLab> Start(bool managed = false, string role = "owner", AuthorizationAction? action = null, string pattern = "*")
        {
            var f = new NavigationLab();
            var state = f.Identity.Store.Read();
            f.ActorId = role == "owner" ? f.Identity.OwnerId : Guid.NewGuid();
            if (role != "owner")
                f.Identity.Store.Write(state with { Users = [..state.Users, new(f.ActorId, "ordinary@example.test", role, ["connection-a"], true, default)] });
            f.Actor = V1ManagedWorkflowLab.Browser(f.ActorId);
            var policy = AuthorizationPolicyCompiler.Compile(new AuthorizationPolicyDefinition(
                action is { } grant ? [new("navigation", [new(grant, ["connection-a"], [pattern])])] : [],
                action is not null ? [new(ManagedProvisioningFixture.Issuer, f.ActorId.ToString("D"), ["navigation"])] : [], []));
            f.Auth = new(new KafdeckOptions(new DeploymentOptions("http://127.0.0.1:0", null, AccessMode.Oidc, null), []), new AuthorizationPolicyEvaluator(policy))
                { ManagedIdentity = managed ? f.Identity.Store : null };
            var builder = WebApplication.CreateBuilder(new WebApplicationOptions { EnvironmentName = "Production" });
            builder.WebHost.UseUrls("http://127.0.0.1:0"); builder.Services.AddSingleton(f.Auth);
            builder.Services.AddSingleton<ISecurityAuditSink>(f.Identity.Audit);
            f.App = builder.Build(); f.App.Use((context, next) => { context.User = f.Actor; return next(context); }); f.App.MapKafdeckOidcSessionEndpoints();
            await f.App.StartAsync(); f.Http = new(new SocketsHttpHandler { UseProxy = false }) { BaseAddress = new(f.App.Urls.Single()) };
            return f;
        }
        internal async Task<JsonDocument> Read(string cluster)
        {
            using var response = await Http.GetAsync("/api/v1/auth/navigation?clusterId=" + Uri.EscapeDataString(cluster));
            Assert.Equal(HttpStatusCode.OK, response.StatusCode); Assert.Equal("no-store", response.Headers.CacheControl!.ToString());
            var value = JsonDocument.Parse(await response.Content.ReadAsStringAsync());
            Assert.Equal(cluster, value.RootElement.GetProperty("clusterId").GetString()); return value;
        }
        public async ValueTask DisposeAsync()
        { Http.Dispose(); await App.StopAsync(); await App.DisposeAsync(); Identity.Dispose(); }
    }
}
