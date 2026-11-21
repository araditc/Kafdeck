using Kafdeck.Core.Security;
using Microsoft.AspNetCore.Mvc;

namespace Kafdeck.Api;

internal sealed record FleetCredentialMetadata(string Id, string Authentication, string Purpose, bool Revoked, long Revision, bool Stored);

internal static class FleetSettingsEndpoints
{
    internal static void MapFleetSettings(this WebApplication app, ManagedIdentityStore identity,
        FleetSettingsStore store, BrokerAgentRecoveryStore? recovery = null)
    {
        var group = app.MapGroup("/api/v1/fleet/settings");
        group.AddEndpointFilter(async (context, next) =>
        {
            var http = context.HttpContext;
            http.Response.Headers.CacheControl = "no-store";
            if (Owner(http, identity) is null) return Failure("host_access_denied", 403);
            try { return await next(context); }
            catch (BrokerHostFailure failure) { return Failure(failure.Code, failure.Code.EndsWith("invalid", StringComparison.Ordinal) ? 400 : 409); }
        });

        group.MapGet("/", () => Results.Ok(View(store)))
            .WithManagedBrowserContract("managed-fleet-settings-read");
        group.MapGet("/public-certificate", (IConfiguration configuration) =>
        {
            var path = configuration["Kestrel:Certificates:Default:Path"];
            if (string.IsNullOrEmpty(path)) return Results.Ok(new { pem = (string?)null });
            try
            {
                using var certificate = System.Security.Cryptography.X509Certificates.X509CertificateLoader.LoadPkcs12FromFile(
                    path, configuration["Kestrel:Certificates:Default:Password"],
                    System.Security.Cryptography.X509Certificates.X509KeyStorageFlags.EphemeralKeySet);
                return Results.Ok(new { pem = certificate.ExportCertificatePem() });
            }
            catch (Exception failure) when (failure is IOException or System.Security.Cryptography.CryptographicException)
            { return Failure("bootstrap_public_certificate_unavailable"); }
        }).WithManagedBrowserContract("managed-fleet-public-certificate");
        group.MapPost("/configuration", async (FleetConfigurationInput input, HttpContext http,
            ISecurityAuditSink audit, CancellationToken cancellation) =>
        {
            var configuration = input.Configuration;
            if (configuration is null || configuration.AllowedNetworks is null || configuration.LogReaderUserIds is null ||
                configuration.LogSources is null || configuration.AgentInstallerUserIds is null ||
                configuration.AgentTargets is null || configuration.AgentPackages is null)
                return Failure("fleet_configuration_invalid", 400);
            using var lease = recovery?.ConfigurationLease();
            var previous = store.Read().Configuration;
            if (recovery is not null && previous.AgentTargets.Any(target => recovery.Find(target.ClusterId) is not null &&
                (!configuration.AgentTargets.Contains(target) || !previous.AgentPackages.All(configuration.AgentPackages.Contains))))
                return Failure("bootstrap_agent_operation_pending");
            store.SaveConfiguration(input, Audit(http, identity, "fleet_configuration_changed", "fleet-settings"));
            await store.DrainAuditAsync(audit, cancellation);
            return Results.Ok(View(store));
        }).WithManagedBrowserContract("managed-fleet-settings-write", typeof(FleetConfigurationInput))
            .WithMetadata(new RequestSizeLimitAttribute(65536)).RequireKafdeckAntiforgery();
        group.MapPost("/credentials", async (FleetCredentialInput input, HttpContext http,
            ISecurityAuditSink audit, CancellationToken cancellation) =>
        {
            store.SaveCredential(input, Audit(http, identity, input.Revoke ? "fleet_credential_revoked" : "fleet_credential_saved", input.Id));
            await store.DrainAuditAsync(audit, cancellation);
            return Results.Ok(View(store));
        }).WithManagedBrowserContract("managed-fleet-credential-write", typeof(FleetCredentialInput))
            .WithMetadata(new RequestSizeLimitAttribute(131072)).RequireKafdeckAntiforgery();
    }

    internal static object View(FleetSettingsStore store)
    {
        var state = store.Read();
        return new
        {
            revision = state.SettingsRevision, configuration = state.Configuration,
            credentials = state.Credentials.Select(item => new FleetCredentialMetadata(
                item.Id, item.Authentication, item.Purpose, item.Revoked, item.Revision, !item.Revoked))
                .Concat(store.LegacyCredentials.Where(item => !state.Credentials.Any(saved => saved.Id == item.Id))
                    .Select(item => new FleetCredentialMetadata(item.Id, item.Authentication,
                        state.Configuration.AgentTargets.Any(target => target.CredentialId == item.Id) ? "agent" : "probe", false, 0, false))),
        };
    }

    internal static OperatorSessionContext? Owner(HttpContext http, ManagedIdentityStore identity) =>
        http.User.Identity is not NonBrowserReadIdentity && OperatorSessionContextFactory.TryCreate(http.User, out var session) &&
        session is not null && ManagedIdentityEndpoints.IsOwner(identity, session) ? session : null;

    internal static SecurityAuditEvent Audit(HttpContext http, ManagedIdentityStore identity, string code, string? resource = null, string? cluster = null)
    {
        var actor = Owner(http, identity) ?? throw new BrokerHostFailure("host_access_denied");
        return new(DateTimeOffset.UtcNow, SecurityAuditEventType.BrokerHostManagement,
            SecurityAuditPrincipal.FromOperator(actor.Identity), Guid.NewGuid().ToString("N"), cluster, resource,
            SecurityAuditOutcome.Succeeded, code);
    }

    internal static IResult Failure(string code, int status = 409) => Results.Problem(statusCode: status,
        title: "Fleet setup", detail: code switch
        {
            "host_access_denied" => "Only the installation administrator can manage Fleet setup.",
            "bootstrap_sudo_password_required" => "This account requires its sudo password for the fixed setup helper.",
            "bootstrap_cluster_identity_unavailable" => "No running broker on this host could be verified against the selected Kafka cluster.",
            "bootstrap_service_changed" => "A selected service changed since inspection. Inspect the host again before preparing it.",
            "bootstrap_agent_operation_pending" => "Finish or reconcile the current agent operation before changing its target or packages.",
            "bootstrap_existing_agent_requires_adoption" => "An existing agent uses a different launch configuration. Register its existing restricted access and use the agent target selector to upgrade it safely.",
            "bootstrap_preparation_unverified" => "Host preparation is incomplete or its installed files changed. Resume the same operation with temporary administrative access.",
            "host_key_mismatch" => "The SSH host key differs from the confirmed fingerprint. Verify the host before retrying.",
            "fleet_revision_conflict" => "Fleet settings changed. Reload the settings and retry.",
            "fleet_credential_invalid" => "Check the credential name, authentication type and secret.",
            "fleet_private_key_invalid" => "The private key or its passphrase could not be read.",
            "fleet_configuration_invalid" => "Check the network ranges, operator permissions and agent targets.",
            "fleet_credential_revoked" => "This SSH credential has been revoked. Select an active credential.",
            "fleet_keys_or_storage_unavailable" => "Fleet could not decrypt its stored settings. Restore the matching key backup.",
            _ => "Fleet setup could not be verified. Check the selected settings and operation status.",
        }, extensions: new Dictionary<string, object?> { ["code"] = code });
}
