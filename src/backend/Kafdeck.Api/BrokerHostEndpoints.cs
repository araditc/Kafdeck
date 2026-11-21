using Kafdeck.Core.Kafka;
using Kafdeck.Core.Security;
using Kafdeck.Modules.Clusters;
using Microsoft.AspNetCore.Mvc;

namespace Kafdeck.Api;

internal static class BrokerHostEndpoints
{
    internal static void MapBrokerHosts(this WebApplication app, ManagedIdentityStore identity,
        BrokerHostOptions options, BrokerHostStore store, IBrokerHostProbe probe)
    {
        var group = app.MapGroup("/api/v1/fleet");
        group.AddEndpointFilter(async (context, next) =>
        {
            var http = context.HttpContext;
            http.Response.Headers.CacheControl = "no-store";
            if (Owner(http, identity) is null) return Problem("host_access_denied", 403);
            if (!options.Current.Enabled && !HttpMethods.IsGet(http.Request.Method)) return Problem("host_management_disabled", 409);
            try
            {
                await store.DrainAuditAsync(http.RequestServices.GetRequiredService<ISecurityAuditSink>(), http.RequestAborted);
                return await next(context);
            }
            catch (BrokerHostFailure failure)
            {
                var owner = Owner(http, identity);
                if (owner is not null)
                    await http.RequestServices.GetRequiredService<ISecurityAuditSink>().WriteAsync(
                        Audit(owner, null, null, SecurityAuditOutcome.Failed, failure.Code), http.RequestAborted);
                return Problem(failure.Code, failure.Code == "host_probe_busy" ? 429 : failure.Code == "host_access_denied" ? 403 : 409);
            }
        });

        group.MapGet("/hosts", () =>
        {
            var state = store.Read();
            return Results.Ok(new { enabled = options.Current.Enabled, state.Revision, state.Hosts, state.Bindings,
                agentCredentials = store.AgentCredentials(), credentials = options.Current.Credentials.Select(value => new { value.Id, value.Authentication }) });
        }).WithManagedBrowserContract("managed-hosts-list");

        group.MapPost("/hosts", async (BrokerHostWrite input, HttpContext http, ISecurityAuditSink audit, CancellationToken cancellation) =>
        {
            if (!BrokerHostValidation.Draft(input.Host, options)) return Problem("host_configuration_invalid", 400);
            var observed = await probe.ObserveAsync(input.Host, cancellation);
            var owner = Owner(http, identity) ?? throw new BrokerHostFailure("host_access_denied");
            var host = new BrokerHost(Guid.NewGuid(), input.Host, DateTimeOffset.UtcNow);
            var state = store.Commit(input.ExpectedRevision, current =>
            {
                if (!options.Current.Enabled || !BrokerHostValidation.Draft(input.Host, options))
                    throw new BrokerHostFailure("host_configuration_invalid");
                if (current.Hosts.Length >= 64 || current.Hosts.Any(item => item.Connection.Address == input.Host.Address &&
                        item.Connection.Port == input.Host.Port && item.Connection.User == input.Host.User))
                    throw new BrokerHostFailure("host_already_registered_or_limit");
                return current with { Hosts = [.. current.Hosts, host] };
            }, Audit(owner, null, host.Id.ToString("D"), SecurityAuditOutcome.Succeeded, "host_registered"));
            await store.DrainAuditAsync(audit, cancellation);
            return Results.Json(new { state.Revision, host, observed.Targets }, statusCode: 201);
        }).WithManagedBrowserContract("managed-hosts-register", typeof(BrokerHostWrite))
            .WithMetadata(new RequestSizeLimitAttribute(8192)).RequireKafdeckAntiforgery();

        group.MapPost("/hosts/{hostId:guid}/probe", async (Guid hostId, HttpContext http, ISecurityAuditSink audit, CancellationToken cancellation) =>
        {
            var host = store.Read().Hosts.SingleOrDefault(value => value.Id == hostId);
            if (host is null) return Problem("host_missing", 404);
            var observation = await probe.ObserveAsync(host.Connection, cancellation);
            var owner = Owner(http, identity) ?? throw new BrokerHostFailure("host_access_denied");
            await audit.WriteAsync(Audit(owner, null, hostId.ToString("D"), SecurityAuditOutcome.Succeeded, "host_observed"), cancellation);
            return Results.Ok(observation);
        }).WithManagedBrowserContract("managed-hosts-probe")
            .WithMetadata(new RequestSizeLimitAttribute(1024)).RequireKafdeckAntiforgery();

        group.MapPost("/hosts/bindings", async (BrokerHostBind input, HttpContext http, ISecurityAuditSink audit,
            ClusterExplorerService clusters, CancellationToken cancellation) =>
        {
            if (input.ClusterId is not { Length: > 0 and <= 128 } || input.ClusterId.Any(char.IsControl) ||
                input.BrokerId < 0 || !BrokerHostValidation.Name(input.TargetId)) return Problem("host_binding_invalid", 400);
            var before = store.Read();
            if (before.Revision != input.ExpectedRevision) return Problem("host_revision_conflict", 409);
            var host = before.Hosts.SingleOrDefault(value => value.Id == input.HostId);
            if (host is null) return Problem("host_missing", 404);
            var observation = await probe.ObserveAsync(host.Connection, cancellation);
            var target = observation.Targets.SingleOrDefault(value => value.Id == input.TargetId);
            using var deadline = CancellationTokenSource.CreateLinkedTokenSource(cancellation);
            deadline.CancelAfter(TimeSpan.FromSeconds(8));
            var cluster = await clusters.GetClusterAsync(input.ClusterId, deadline.Token);
            ValidateBinding(target, cluster, input.BrokerId);
            var owner = Owner(http, identity) ?? throw new BrokerHostFailure("host_access_denied");
            var binding = new BrokerHostBinding(input.ClusterId, input.BrokerId, input.HostId, input.TargetId,
                target!.KafkaClusterId!, target.Instance!, DateTimeOffset.UtcNow);
            var result = store.Commit(input.ExpectedRevision, current =>
            {
                if (!options.Current.Enabled || !BrokerHostValidation.Draft(host.Connection, options))
                    throw new BrokerHostFailure("host_configuration_invalid");
                if (current.Bindings.Length >= 1024 && !current.Bindings.Any(item => item.ClusterId == input.ClusterId && item.BrokerId == input.BrokerId) || current.Bindings.Any(item =>
                        item.HostId == input.HostId && item.TargetId == input.TargetId &&
                        (item.ClusterId != input.ClusterId || item.BrokerId != input.BrokerId)))
                    throw new BrokerHostFailure("host_service_already_bound_or_limit");
                return current with { Bindings = [.. current.Bindings.Where(item => item.ClusterId != input.ClusterId || item.BrokerId != input.BrokerId), binding] };
            }, Audit(owner, input.ClusterId, input.BrokerId.ToString(System.Globalization.CultureInfo.InvariantCulture),
                SecurityAuditOutcome.Succeeded, "host_broker_binding_verified"));
            await store.DrainAuditAsync(audit, cancellation);
            return Results.Ok(new { result.Revision, binding });
        }).WithManagedBrowserContract("managed-hosts-bind", typeof(BrokerHostBind))
            .WithMetadata(new RequestSizeLimitAttribute(4096)).RequireKafdeckAntiforgery();
    }

    internal static void ValidateBinding(BrokerServiceTarget? target, ClusterProjection cluster, int brokerId)
    {
        if (target?.Status != "verified" || cluster.Failure is not null || !cluster.Observation.IsFreshAt(DateTimeOffset.UtcNow) || cluster.Observation.Source == ObservationSource.StaleSnapshot ||
            string.IsNullOrEmpty(cluster.KafkaClusterId) || !cluster.Brokers.Any(broker => broker.BrokerId == brokerId))
            throw new BrokerHostFailure("host_identity_unavailable");
        if (target.BrokerId != brokerId || target.KafkaClusterId != cluster.KafkaClusterId)
            throw new BrokerHostFailure("host_broker_identity_mismatch");
    }

    private static OperatorSessionContext? Owner(HttpContext http, ManagedIdentityStore identity) =>
        http.User.Identity is not NonBrowserReadIdentity && OperatorSessionContextFactory.TryCreate(http.User, out var session) &&
        session is not null && ManagedIdentityEndpoints.IsOwner(identity, session) ? session : null;

    private static SecurityAuditEvent Audit(OperatorSessionContext owner, string? cluster, string? resource,
        SecurityAuditOutcome outcome, string code) => new(DateTimeOffset.UtcNow, SecurityAuditEventType.BrokerHostManagement,
            SecurityAuditPrincipal.FromOperator(owner.Identity), Guid.NewGuid().ToString("N"), cluster, resource, outcome, code);

    private static IResult Problem(string code, int status) => ApiResults.Problem(new ApiProblemDefinition(status,
        "urn:kafdeck:problem:" + code, "Broker host management", code switch
        {
            "host_key_mismatch" => "The SSH host key differs from the trusted fingerprint. Verify the host through your administrator.",
            "host_broker_identity_mismatch" => "The running service belongs to a different cluster or broker.",
            "host_revision_conflict" => "Host settings changed. Reload Fleet and retry.",
            "host_management_disabled" => "Open Fleet settings to enable host management, approve networks and add encrypted SSH credentials.",
            "host_access_denied" => "Only the installation administrator can register hosts and broker bindings.",
            _ => "The host operation could not be verified. Check the dedicated account, trusted fingerprint and approved service catalog.",
        }, code));
}
