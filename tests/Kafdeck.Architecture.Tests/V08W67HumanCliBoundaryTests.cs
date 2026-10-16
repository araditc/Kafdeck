using System.Security.Claims;
using System.Text.Json;
using Kafdeck.Api;
using Kafdeck.Cli;
using Kafdeck.Core.Security;
using Microsoft.Extensions.Configuration;
using Xunit;

namespace Kafdeck.Architecture.Tests;

public sealed class V08W67HumanCliBoundaryTests
{
    [Theory]
    [InlineData("http://sso.test/realms/kafdeck")]
    [InlineData("https://user:password@sso.test/realms/kafdeck")]
    [InlineData("https://sso.test/realms/kafdeck?token=sentinel")]
    [InlineData("https://sso.test/realms/kafdeck#fragment")]
    [InlineData("https://sso.test/realms/kafdeck/")]
    [InlineData("https://SSO.test/realms/kafdeck")]
    public void Human_cli_profile_requires_exact_https_realm_issuer(string issuer) =>
        Assert.Throws<CliUsageException>(() => new CliOidcProfile(issuer, "api", "public-cli").Validate());

    [Theory]
    [InlineData("api-*")]
    [InlineData(" api")]
    [InlineData("api\n")]
    [InlineData("")]
    public void Human_cli_profile_cannot_infer_or_wildcard_api_audience(string audience) =>
        Assert.Throws<CliUsageException>(() => new CliOidcProfile("https://sso.test/realms/kafdeck", audience, "public-cli").Validate());

    [Fact]
    public async Task Public_profile_rejects_secret_or_unknown_fields_without_echoing_values()
    {
        var file = Path.Combine(Path.GetTempPath(), "kafdeck-public-profile-" + Guid.NewGuid().ToString("N") + ".json");
        try
        {
            await File.WriteAllTextAsync(file, "{\"issuer\":\"https://sso.test/realms/kafdeck\",\"audience\":\"api\",\"clientId\":\"cli\",\"clientSecret\":\"sentinel-credential\"}");
            var exception = await Assert.ThrowsAsync<CliUsageException>(() => CliOidcProfile.LoadAsync(file, default));
            Assert.DoesNotContain("sentinel", exception.ToString(), StringComparison.Ordinal);
        }
        finally { File.Delete(file); }
    }

    [Theory]
    [InlineData("{}")]
    [InlineData("[]")]
    [InlineData("{\"issuer\":123,\"audience\":\"api\",\"clientId\":\"cli\"}")]
    [InlineData("{\"issuer\":\"https://sso.test/realms/kafdeck\",\"audience\":null,\"clientId\":\"cli\"}")]
    [InlineData("{\"issuer\":\"https://sso.test/realms/kafdeck\",\"audience\":\"api\",\"clientId\":[],\"trustedCaPemFile\":{}}")]
    public async Task Malformed_or_missing_public_profile_fields_produce_usage_error_not_unhandled_exception(string json)
    {
        var file = Path.Combine(Path.GetTempPath(), "kafdeck-profile-" + Guid.NewGuid().ToString("N") + ".json");
        try { await File.WriteAllTextAsync(file, json); await Assert.ThrowsAsync<CliUsageException>(() => CliOidcProfile.LoadAsync(file, default)); }
        finally { File.Delete(file); }
    }

    [Fact]
    public void Named_effect_commands_require_oidc_and_preserve_operation_outcome_categories()
    {
        const string operation = "00000000-0000-0000-0000-000000000001";
        Assert.Throws<CliUsageException>(() => CliParser.Parse(["operations", "execute", operation]));
        var invocation = CliParser.Parse(["--oidc-profile", "public.json", "operations", "confirm", operation, new string('a', 64)]);
        Assert.Equal(CliCommand.OperationConfirm, invocation.Command);
        Assert.Equal("/api/v1/automation/mutations/" + operation + "/confirm", CliRouteBuilder.Build(invocation));
        Assert.Equal(CliApplication.PendingApproval, CliApplication.MutationOutcomeCategory("{\"state\":\"awaitingApproval\"}"));
        Assert.Equal(CliApplication.UnknownEffect, CliApplication.MutationOutcomeCategory("{\"state\":\"executionUnknown\"}"));
        Assert.Equal(CliApplication.Conflict, CliApplication.MutationOutcomeCategory("{\"state\":\"stalePreview\"}"));
        Assert.Equal(CliApplication.PartialOrUnverified, CliApplication.MutationOutcomeCategory("{\"state\":\"appliedUnverified\"}"));
        Assert.Equal(CliApplication.Success, CliApplication.MutationOutcomeCategory("{\"state\":\"appliedVerified\"}"));
    }

    [Theory]
    [InlineData("previewed", CliApplication.Success)]
    [InlineData("awaitingConfirmation", CliApplication.Success)]
    [InlineData("ready", CliApplication.Success)]
    [InlineData("appliedVerified", CliApplication.Success)]
    [InlineData("awaitingApproval", CliApplication.PendingApproval)]
    [InlineData("executing", CliApplication.UnknownEffect)]
    [InlineData("executionUnknown", CliApplication.UnknownEffect)]
    [InlineData("stalePreview", CliApplication.Conflict)]
    [InlineData("expired", CliApplication.Expired)]
    [InlineData("appliedUnverified", CliApplication.PartialOrUnverified)]
    [InlineData("partiallyApplied", CliApplication.PartialOrUnverified)]
    [InlineData("failedBeforeDispatch", CliApplication.RemoteError)]
    [InlineData("failedDefinitive", CliApplication.RemoteError)]
    [InlineData("rejected", CliApplication.RemoteError)]
    [InlineData("cancelled", CliApplication.RemoteError)]
    [InlineData("futureUnknownState", CliApplication.RemoteError)]
    public void Every_exposed_durable_outcome_has_an_explicit_cli_exit_category(string state, int expected) =>
        Assert.Equal(expected, CliApplication.MutationOutcomeCategory(JsonSerializer.Serialize(new { state })));

    [Fact]
    public void Human_oidc_profile_is_a_per_command_public_configuration_and_never_a_token_argument()
    {
        var invocation = CliParser.Parse(["--oidc-profile", "public.json", "system", "info"]);
        Assert.Equal("public.json", invocation.OidcProfileFile);
        Assert.Equal(CliCommand.SystemInfo, invocation.Command);
        Assert.Null(invocation.TokenFile);
        Assert.Throws<CliUsageException>(() => CliParser.Parse(["--oidc-profile", "public.json", "--token-file", "secret.key", "system", "info"]));
        Assert.Throws<CliUsageException>(() => CliParser.Parse(["--oidc-profile", "public.json", "--token", "sentinel", "system", "info"]));
    }

    [Fact]
    public void Human_registry_is_distinct_from_service_and_does_not_create_cookie_session_or_group_grants()
    {
        var configuration = HumanConfiguration();
        var registry = new NonBrowserReadRegistry(configuration);
        Assert.NotNull(registry.Find("human-subject", "public-cli", kind: NonBrowserPrincipalKind.Human));
        Assert.Null(registry.Find("human-subject", "public-cli", kind: NonBrowserPrincipalKind.Service));
        var human = new NonBrowserReadIdentity("human-one", new("https://sso.test/realms/kafdeck", "human-subject", "public-cli", "api", DateTimeOffset.UtcNow.AddMinutes(2)), NonBrowserPrincipalKind.Human);
        Assert.False(OperatorSessionContextFactory.TryCreate(new ClaimsPrincipal(human), out _));
        Assert.StartsWith("oidc-cli-human:", human.AuditId, StringComparison.Ordinal);
        var evaluator = new AuthorizationPolicyEvaluator(AuthorizationPolicyCompiler.Compile(new(
            [new("viewer", [new(AuthorizationAction.TopicRead, ["permitted"], ["orders*"])])],
            [new("https://sso.test/realms/kafdeck", "human-subject", ["viewer"])], [])));
        Assert.True(registry.IsCollectionAllowed(human, AuthorizationAction.TopicRead, "permitted", evaluator));
        Assert.False(registry.IsCollectionAllowed(human, AuthorizationAction.TopicRead, "denied", evaluator));
        Assert.True(registry.IsAllowed(human, new(AuthorizationAction.TopicRead, "permitted", "orders-created"), evaluator));
        Assert.False(registry.IsAllowed(human, new(AuthorizationAction.TopicRead, "permitted", "secret-topic"), evaluator));
        configuration["Kafdeck:NonBrowserRead:HumanRegistrations:0:Enabled"] = "false";
        Assert.False(registry.IsAllowed(human, new(AuthorizationAction.TopicRead, "permitted", "orders-created"), evaluator));
    }

    [Fact]
    public void Same_subject_client_cannot_be_registered_as_both_machine_and_human()
    {
        var configuration = HumanConfiguration();
        foreach (var item in configuration.GetSection("Kafdeck:NonBrowserRead:HumanRegistrations:0").AsEnumerable().Where(p => p.Value is not null))
            configuration[item.Key.Replace(":HumanRegistrations:", ":Registrations:", StringComparison.Ordinal)] = item.Value;
        Assert.Throws<ArgumentException>(() => new NonBrowserReadRegistry(configuration));
    }

    private static IConfigurationRoot HumanConfiguration() => new ConfigurationBuilder().AddInMemoryCollection(new Dictionary<string, string?>
    {
        ["Kafdeck:NonBrowserRead:Enabled"] = "true", ["Kafdeck:NonBrowserRead:Issuer"] = "https://sso.test/realms/kafdeck",
        ["Kafdeck:NonBrowserRead:ApiAudience"] = "api", ["Kafdeck:NonBrowserRead:HumanRegistrations:0:Id"] = "human-one",
        ["Kafdeck:NonBrowserRead:HumanRegistrations:0:OwnerId"] = "operator-owner", ["Kafdeck:NonBrowserRead:HumanRegistrations:0:Subject"] = "human-subject",
        ["Kafdeck:NonBrowserRead:HumanRegistrations:0:ClientId"] = "public-cli", ["Kafdeck:NonBrowserRead:HumanRegistrations:0:Enabled"] = "true",
        ["Kafdeck:NonBrowserRead:HumanRegistrations:0:ExpiresAtUtc"] = DateTimeOffset.UtcNow.AddMinutes(10).ToString("O"),
        ["Kafdeck:NonBrowserRead:HumanRegistrations:0:ReadActions:0"] = "TopicRead",
    }).Build();
}
