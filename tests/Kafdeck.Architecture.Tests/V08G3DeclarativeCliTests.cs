using System.Text;
using System.Text.Json;
using Kafdeck.Cli;
using Xunit;

namespace Kafdeck.Architecture.Tests;

public sealed class V08G3DeclarativeCliTests
{
    private const string Hash = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    private const string Id = "00000000-0000-0000-0000-000000000001";

    [Theory]
    [InlineData("observe", "/api/v1/declarative/observe")]
    [InlineData("drift", "/api/v1/declarative/drift")]
    [InlineData("import", "/api/v1/declarative/import-preview")]
    [InlineData("import-preview", "/api/v1/declarative/import-preview")]
    [InlineData("plan", "/api/v1/declarative/plan")]
    [InlineData("reconcile", "/api/v1/declarative/plan")]
    public void Every_remote_command_has_one_closed_named_route_and_registered_profile(string verb, string route)
    {
        var args = Args(verb, "desired.yaml", preview: verb is "import" or "import-preview" or "plan" or "reconcile");
        Assert.Throws<CliUsageException>(() => CliParser.Parse(args));
        var invocation = CliParser.Parse(new[] { "--service-profile", "public.json" }.Concat(args).ToArray());
        Assert.Equal(CliCommand.Declarative, invocation.Command);
        Assert.Equal(route, CliRouteBuilder.Build(invocation));
        Assert.Equal("yaml", invocation.Declarative!.Format);
        Assert.Equal(0, invocation.Declarative.ExpectedOwnershipRevision);
    }

    [Fact]
    public void Apply_preserves_the_exact_server_stored_id_and_fingerprint_without_manifest_or_raw_route()
    {
        var invocation = CliParser.Parse(["--oidc-profile", "public.json", "declarative", "apply", Id, Hash]);
        Assert.Equal("/api/v1/declarative/plans/" + Id + "/apply", CliRouteBuilder.Build(invocation));
        Assert.Equal(Hash, invocation.Declarative!.PlanFingerprint);
        Assert.Equal("/api/v1/declarative/plans/" + Id + "/recover", CliRouteBuilder.Build(
            CliParser.Parse(["--oidc-profile", "public.json", "declarative", "recover", Id, Hash])));
        Assert.Throws<CliUsageException>(() => CliParser.Parse(["--oidc-profile", "public.json", "declarative", "apply", Id, Hash, "--manifest-file", "extra.json"]));
        Assert.Throws<CliUsageException>(() => CliParser.Parse(["--oidc-profile", "public.json", "declarative", "apply", Id, Hash.ToUpperInvariant()]));
        Assert.Throws<CliUsageException>(() => CliParser.Parse(["declarative", "raw", "https://provider.example"]));
    }

    [Fact]
    public async Task Source_revision_commands_have_closed_routes_generation_CAS_and_typed_canonical_payload()
    {
        var shown = CliParser.Parse(["--service-profile", "public.json", "declarative", "source-show", "registered-source", "--resource", "ops-lag"]);
        Assert.Equal("/api/v1/declarative/sources/registered-source?resourceId=ops-lag", CliRouteBuilder.Build(shown));
        var file = Path.Combine(Path.GetTempPath(), "g3-source-" + Guid.NewGuid().ToString("N") + ".yaml");
        try
        {
            await File.WriteAllTextAsync(file, V08G3DeclarativeManifestTests.Yaml);
            var args = new[] { "--service-profile", "public.json", "declarative", "source-advance", "registered-source", Guid.Empty.ToString("D"), "0", Hash, "--manifest-file", file };
            var invocation = CliParser.Parse(args);
            Assert.Equal("/api/v1/declarative/sources/registered-source/advance", CliRouteBuilder.Build(invocation));
            var body = JsonSerializer.SerializeToElement(await CliDeclarativeCommands.ReadRequestAsync(invocation.Declarative!, default));
            Assert.Equal(new[] { "manifest", "format", "expectedGeneration", "expectedRevision", "expectedSourceHash" }, body.EnumerateObject().Select(value => value.Name));
            Assert.Equal(Guid.Empty, body.GetProperty("expectedGeneration").GetGuid());
            Assert.Equal(0, body.GetProperty("expectedRevision").GetInt64());
            Assert.Equal(Hash, body.GetProperty("expectedSourceHash").GetString());
            Assert.Equal("json", body.GetProperty("format").GetString());
            Assert.Throws<CliUsageException>(() => CliParser.Parse(args.Select(value => value == "0" ? "-1" : value).ToArray()));
            Assert.Throws<CliUsageException>(() => CliParser.Parse(args.Concat(new[] { "--resource", "another" }).ToArray()));
            Assert.Throws<CliUsageException>(() => CliParser.Parse(args.Concat(new[] { "--idempotency-key", "not-a-source-cas-field" }).ToArray()));
        }
        finally { File.Delete(file); }
    }

    [Theory]
    [InlineData(".")] [InlineData("..")]
    public void Source_commands_reject_reserved_path_segments_before_route_construction(string segment)
    {
        Assert.Throws<CliUsageException>(() => CliParser.Parse(["--service-profile", "public.json", "declarative", "source-show", segment, "--resource", "ops-lag"]));
        Assert.Throws<CliUsageException>(() => CliParser.Parse(["--service-profile", "public.json", "declarative", "source-show", "registered-source", "--resource", segment]));
        Assert.Throws<CliUsageException>(() => CliParser.Parse(["--service-profile", "public.json", "declarative", "source-advance", segment,
            Guid.Empty.ToString("D"), "0", Hash, "--manifest-file", "desired.yaml"]));
    }

    [Fact]
    public void Plan_only_reads_and_capabilities_have_exact_GET_resources_without_manifest_or_apply_arguments()
    {
        Assert.Equal("/api/v1/declarative/plans/" + Id, CliRouteBuilder.Build(
            CliParser.Parse(["--service-profile", "public.json", "declarative", "show", Id])));
        Assert.Equal("/api/v1/declarative/capabilities", CliRouteBuilder.Build(
            CliParser.Parse(["--service-profile", "public.json", "declarative", "capabilities"])));
        Assert.Throws<CliUsageException>(() => CliParser.Parse(["--service-profile", "public.json", "declarative", "show", Id, Hash]));
        Assert.Throws<CliUsageException>(() => CliParser.Parse(["--service-profile", "public.json", "declarative", "capabilities", "--manifest-file", "raw.json"]));
    }

    [Fact]
    public void Source_revision_cas_consent_idempotency_and_payload_options_are_command_specific()
    {
        var args = Args("validate", "desired.json");
        Assert.Equal("validate", CliParser.Parse(args).Declarative!.Command);
        Assert.Throws<CliUsageException>(() => CliParser.Parse(args.Concat(new[] { "--source", "another" }).ToArray()));
        Assert.Throws<CliUsageException>(() => CliParser.Parse(args.Concat(new[] { "--yes" }).ToArray()));
        Assert.Throws<CliUsageException>(() => CliParser.Parse(args.Concat(new[] { "--request-file", "raw.json" }).ToArray()));
        Assert.Throws<CliUsageException>(() => CliParser.Parse(args.Select(value => value == Hash ? "unverified-ref" : value).ToArray()));
        Assert.Throws<CliUsageException>(() => CliParser.Parse(args.Select(value => value == "0" ? "-1" : value).ToArray()));
        Assert.Throws<CliUsageException>(() => CliParser.Parse(["system", "info", "--manifest-file", "desired.json"]));
        var yes = CliParser.Parse(new[] { "--oidc-profile", "public.json" }.Concat(Args("reconcile", "desired.json", true)).Append("--yes").ToArray());
        Assert.True(yes.Declarative!.Yes);
    }

    [Theory]
    [InlineData(false)] [InlineData(true)]
    public async Task Payload_is_closed_canonical_and_format_neutral_before_any_authentication(bool yaml)
    {
        var file = Path.Combine(Path.GetTempPath(), "g3-manifest-" + Guid.NewGuid().ToString("N") + (yaml ? ".yaml" : ".json"));
        try
        {
            await File.WriteAllTextAsync(file, yaml ? V08G3DeclarativeManifestTests.Yaml : V08G3DeclarativeManifestTests.Json);
            var invocation = CliParser.Parse(Args("validate", file));
            var body = JsonSerializer.SerializeToElement(await CliDeclarativeCommands.ReadRequestAsync(invocation.Declarative!, default));
            Assert.Equal(new[] { "manifest", "format", "sourceRef", "sourceRevision", "expectedOwnershipRevision" }, body.EnumerateObject().Select(value => value.Name));
            Assert.Equal("json", body.GetProperty("format").GetString());
            Assert.Equal(Hash, body.GetProperty("sourceRevision").GetString());
            Assert.Equal("registered-source", body.GetProperty("sourceRef").GetString());
            using var output = new StringWriter(); using var error = new StringWriter();
            Assert.Equal(CliApplication.Success, await CliApplication.RunAsync(Args("validate", file), output, error, default));
            using var result = JsonDocument.Parse(output.ToString());
            Assert.True(result.RootElement.GetProperty("valid").GetBoolean());
            Assert.Equal(64, result.RootElement.GetProperty("manifestFingerprint").GetString()!.Length);
        }
        finally { File.Delete(file); }
    }

    [Fact]
    public async Task Invalid_secret_or_graph_manifest_is_rejected_before_opening_a_missing_credential_file_or_socket()
    {
        var file = Path.Combine(Path.GetTempPath(), "g3-rejected-" + Guid.NewGuid().ToString("N") + ".yaml");
        try
        {
            await File.WriteAllTextAsync(file, "metadata: &recursive [*recursive]\nspec:\n  password: sentinel-secret");
            using var output = new StringWriter(); using var error = new StringWriter();
            var args = new[] { "--oidc-profile", "absent-profile-must-never-open.json" }.Concat(Args("plan", file, true)).ToArray();
            Assert.Equal(CliApplication.UsageError, await CliApplication.RunAsync(args, output, error, default));
            Assert.DoesNotContain("sentinel", output + error.ToString(), StringComparison.Ordinal);
            Assert.Contains("closed manifest", error.ToString(), StringComparison.Ordinal);
        }
        finally { File.Delete(file); }
    }

    [Theory]
    [InlineData("InSync", "Managed", CliApplication.Success)]
    [InlineData("InSync", "ObservedOnly", CliApplication.Conflict)]
    [InlineData("OutOfSync", "ObservedOnly", CliApplication.Conflict)]
    [InlineData("Missing", "Managed", CliApplication.Conflict)]
    [InlineData("Unknown", "Managed", CliApplication.UnknownEffect)]
    [InlineData("Inaccessible", "ObservedOnly", CliApplication.AuthenticationOrAuthorizationError)]
    public void Finite_reconcile_noops_only_owned_in_sync_and_never_adopts_or_creates_from_uncertainty(string drift, string ownership, int expected) =>
        Assert.Equal(expected, CliDeclarativeCommands.ReconcileObservation(JsonSerializer.Serialize(new { drift, ownership, sourceInSync = true })));

    [Theory]
    [InlineData(true)] [InlineData(false)]
    public void Noop_requires_current_source_generation_and_pinned_revision_binding(bool sourceInSync) =>
        Assert.Equal(sourceInSync ? CliApplication.Success : (int?)null,
            CliDeclarativeCommands.ReconcileObservation(JsonSerializer.Serialize(new { drift = "InSync", ownership = "Managed", sourceInSync })));

    [Theory]
    [InlineData("{\"drift\":\"InSync\",\"ownership\":\"Managed\"}")]
    [InlineData("{\"drift\":\"InSync\",\"ownership\":\"Managed\",\"sourceInSync\":null}")]
    [InlineData("{\"drift\":\"InSync\",\"ownership\":\"Managed\",\"sourceInSync\":\"true\"}")]
    public void Missing_or_untyped_source_binding_proof_never_skips_governed_planning(string json) =>
        Assert.Equal(CliApplication.RemoteError, CliDeclarativeCommands.ReconcileObservation(json));

    [Theory]
    [InlineData("{\"drift\":\"InSync\"}", CliApplication.RemoteError)]
    [InlineData("{\"drift\":\"Unknown\"}", CliApplication.UnknownEffect)]
    [InlineData("[]", CliApplication.RemoteError)]
    public void Reconcile_missing_ownership_proof_never_claims_owned_in_sync(string json, int expected) =>
        Assert.Equal(expected, CliDeclarativeCommands.ReconcileObservation(json));

    [Theory]
    [InlineData("Missing", "ObservedOnly")]
    [InlineData("OutOfSync", "Managed")]
    public void Finite_reconcile_requires_a_new_governed_plan_for_authoritative_permitted_changes(string drift, string ownership) =>
        Assert.Null(CliDeclarativeCommands.ReconcileObservation(JsonSerializer.Serialize(new { drift, ownership })));

    [Theory]
    [InlineData("InSync", CliApplication.Success)]
    [InlineData("OutOfSync", CliApplication.Conflict)]
    [InlineData("Missing", CliApplication.Conflict)]
    [InlineData("Conflict", CliApplication.Conflict)]
    [InlineData("Inaccessible", CliApplication.AuthenticationOrAuthorizationError)]
    [InlineData("Unknown", CliApplication.UnknownEffect)]
    [InlineData("Unsupported", CliApplication.RemoteError)]
    [InlineData("new-unadmitted-truth", CliApplication.RemoteError)]
    public void Drift_never_reports_uncertainty_or_absence_as_success(string state, int exit) =>
        Assert.Equal(exit, CliDeclarativeCommands.DriftExit(JsonSerializer.Serialize(new { drift = state })));

    private static string[] Args(string verb, string file, bool preview = false) =>
        new[] { "declarative", verb, "--manifest-file", file, "--source", "registered-source", "--source-revision", Hash, "--ownership-revision", "0" }
            .Concat(preview ? new[] { "--idempotency-key", "stable-g3-request" } : Array.Empty<string>()).ToArray();
}
