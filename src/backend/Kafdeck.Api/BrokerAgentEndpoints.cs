using System.Globalization;
using System.Security.Cryptography;
using System.Text.Json;
using Kafdeck.Core.Kafka;
using Kafdeck.Core.Security;
using Microsoft.AspNetCore.DataProtection;
using Microsoft.AspNetCore.Mvc;

namespace Kafdeck.Api;

internal static class BrokerAgentEndpoints
{
    internal static void MapBrokerAgents(this WebApplication app, ManagedIdentityStore identity,
        BrokerHostOptions options, BrokerHostStore registry, IBrokerAgentLifecycle host, BrokerAgentRecoveryStore recovery)
    {
        var group = app.MapGroup("/api/v1/fleet/hosts/agent");
        var dispatch = new SemaphoreSlim(1);
        group.AddEndpointFilter(async (context, next) =>
        {
            context.HttpContext.Response.Headers.CacheControl = "no-store";
            try { return await next(context); }
            catch (BrokerHostFailure failure) { return Problem(409, failure.Code); }
            catch (OperationCanceledException) when (!context.HttpContext.RequestAborted.IsCancellationRequested)
            { return Problem(504, "agent_observation_timeout"); }
        });

        group.MapGet("/targets", (string clusterId, HttpContext http) =>
        {
            var allowed = Installer(http, identity, options, clusterId);
            var target = options.AgentTargets.SingleOrDefault(item => item.ClusterId == clusterId);
            if (allowed is null || target is null || !registry.Read().Bindings.Any(row =>
                    row.ClusterId == clusterId && row.BrokerId == target.BrokerId))
                return Results.Ok(new { brokerId = (int?)null, packages = Array.Empty<BrokerAgentPackage>(), pendingPlan = (string?)null });
            var pending = recovery.Find(clusterId);
            return Results.Ok(new { brokerId = (int?)target.BrokerId, packages = options.AgentPackages,
                pendingPlan = pending is { } item && item.Principal == allowed ? item.Token : null });
        }).WithManagedBrowserContract("managed-broker-agent-targets");

        group.MapPost("/preview", async (BrokerAgentPreviewInput input, HttpContext http, IKafkaAdministrationPort kafka,
            MonitoringStore metrics, CancellationToken cancellation) =>
        {
            var principal = Installer(http, identity, options, input.ClusterId);
            if (principal is null) return Problem(403, "agent_install_denied");
            var target = options.AgentTargets.SingleOrDefault(item => item.ClusterId == input.ClusterId && item.BrokerId == input.BrokerId);
            var package = options.AgentPackages.SingleOrDefault(item => item.Version == input.Version);
            if (target is null || package is null || !HasSource(metrics, target)) return Problem(409, "agent_target_unconfigured");
            var selected = GetTarget(registry, target);
            if (selected is null) return Problem(404, "agent_target_unbound");
            var (binding, connection) = selected.Value;
            var command = new BrokerAgentCommand("preview", Guid.NewGuid().ToString("N"), binding.TargetId, binding.BrokerId,
                binding.KafkaClusterId, package.Version, package.Digest);
            var pending = recovery.Find(input.ClusterId);
            if (pending is not null)
            {
                var previous = pending.Principal == principal ? ReadPlan(http, pending.Token) : null;
                if (previous is null || previous.Principal != principal || previous.Command.OperationId != pending.OperationId ||
                    previous.Target != target || previous.Package != package || previous.Binding != binding ||
                    previous.Connection != connection || previous.Command.ExpectRestart != true ||
                    previous.Command.ApplyUntil is not { } deadline || Timestamp() < deadline)
                    return Problem(409, "agent_previous_operation_pending");
                var stage = await host.CallAsync(connection, previous.Command with { Action = "status" }, cancellation);
                CheckStatus(stage);
                if (stage.Stage != "publishing") return Problem(409, "agent_previous_operation_pending");
                command = previous.Command with { Action = "preview" };
            }
            var before = await host.CallAsync(connection, command, cancellation);
            CheckPreview(before);
            if (pending is not null && (!before.RestartRequired || before.Instance != command.Instance ||
                before.CurrentDigest != command.CurrentDigest || before.LaunchMode != command.LaunchMode))
                return Problem(409, "agent_preview_stale");
            var health = await BrokerAgentHealthCheck.CheckAsync(kafka, binding, target.ExpectedBrokers, before.RestartRequired, cancellation);
            var now = Timestamp();
            if (Installer(http, identity, options, input.ClusterId) != principal || GetTarget(registry, target) != selected ||
                recovery.Find(input.ClusterId) != pending)
                return Problem(403, "agent_install_denied");
            var applyUntil = before.RestartRequired ? Math.Min(before.MaintenanceTo!.Value, now + 300) : now + 300;
            var ready = health.Healthy && (!before.RestartRequired || before.MaintenanceFrom <= now && applyUntil - now >= 15);
            var plan = new BrokerAgentPlan(principal, input.ClusterId, target, package, binding, connection,
                command with { Instance = before.Instance, LaunchMode = before.LaunchMode, CurrentDigest = before.CurrentDigest,
                    ApplyUntil = applyUntil, ExpectRestart = before.RestartRequired }, now);
            return Results.Ok(new
            {
                plan = ready ? Protect(http, plan) : null,
                operationId = command.OperationId, brokerId = binding.BrokerId, package, before.Adapter, before.CurrentVersion,
                before.CurrentDigest, before.LaunchMode, before.RestartRequired, before.MaintenanceFrom, before.MaintenanceTo, health,
                requiresWindow = before.RestartRequired && (before.MaintenanceFrom > now || applyUntil - now < 15),
            });
        }).WithManagedBrowserContract("managed-broker-agent-preview", typeof(BrokerAgentPreviewInput))
            .WithMetadata(new RequestSizeLimitAttribute(2048)).RequireKafdeckAntiforgery();

        async Task<IResult> Progress(BrokerAgentPlanInput input, HttpContext http, IKafkaAdministrationPort kafka,
            MonitoringStore metrics, ISecurityAuditSink audit, bool apply, CancellationToken cancellation)
        {
            var plan = ReadPlan(http, input.Plan);
            if (plan is null || plan.Command.ExpectRestart is not { } expectRestart || plan.Command.ApplyUntil is not { } deadline)
                return Problem(400, "agent_preview_invalid");
            bool Authorized() => Installer(http, identity, options, plan.ClusterId) == plan.Principal &&
                options.AgentTargets.Contains(plan.Target) && options.AgentPackages.Contains(plan.Package) &&
                GetTarget(registry, plan.Target) == (plan.Binding, plan.Connection);
            if (!Authorized() || !HasSource(metrics, plan.Target)) return Problem(403, "agent_install_denied");
            if (!await dispatch.WaitAsync(0, cancellation)) return Problem(429, "agent_lifecycle_busy");
            try
            {
                var operationId = plan.Command.OperationId;
                BrokerAgentRecovery? Pending()
                {
                    var row = recovery.Find(plan.ClusterId);
                    return row?.OperationId == operationId && row.Principal == plan.Principal ? row : null;
                }
                string? Checkpoint() => Pending()?.Checkpoint.ToString();
                IResult Reconcile() => Results.Ok(new { stage = "reconcile", operationId, lastConfirmed = Checkpoint() });
                void Advance(BrokerAgentCheckpoint checkpoint) => recovery.Advance(plan.ClusterId, operationId, checkpoint);

                BrokerAgentHostResult status;
                try { status = await host.CallAsync(plan.Connection, plan.Command with { Action = "status" }, cancellation); }
                catch (BrokerHostFailure e) when (UncertainResponse(e) && Pending() is not null)
                { return Authorized() ? Reconcile() : Problem(403, "agent_install_denied"); }
                CheckStatus(status);
                if (!Authorized()) return Problem(403, "agent_install_denied");
                // A lost host journal cannot reset an observed restart or release the cluster's pending operation.
                var checkpoint = Pending()?.Checkpoint;
                if ((checkpoint > BrokerAgentCheckpoint.DispatchAuthorized && status.Stage == "unknown") ||
                    (checkpoint >= BrokerAgentCheckpoint.RestartIntent && status.Stage == "publishing"))
                    return Reconcile();
                if (apply && status.Stage is ("unknown" or "publishing"))
                {
                    if (Timestamp() >= deadline) return Problem(409, "agent_preview_expired");
                    if (status.Stage == "publishing" && !expectRestart) return Problem(409, "agent_preview_stale");
                    var observed = await BrokerAgentHealthCheck.CheckAsync(kafka, plan.Binding, plan.Target.ExpectedBrokers, expectRestart, cancellation);
                    if (!observed.Healthy) return Results.Conflict(new { stage = "blocked", code = "agent_restart_not_safe", health = observed });
                    if (!Authorized()) return Problem(403, "agent_install_denied");
                    if (Timestamp() >= deadline) return Problem(409, "agent_preview_expired");
                    recovery.Begin(new(plan.ClusterId, plan.Command.OperationId, plan.Principal, input.Plan));
                    await Audit(audit, plan, SecurityAuditOutcome.Succeeded, "agent_dispatch_authorized", cancellation);
                    try { status = await host.CallAsync(plan.Connection, plan.Command with { Action = "apply" }, cancellation); }
                    catch (BrokerHostFailure e) when (UncertainResponse(e))
                    {
                        await Audit(audit, plan, SecurityAuditOutcome.Failed, "agent_dispatch_response_uncertain", cancellation);
                        return Authorized() ? Reconcile() : Problem(403, "agent_install_denied");
                    }
                    CheckStatus(status);
                }
                if (!Authorized()) return Problem(403, "agent_install_denied");
                if (status.Stage == "unknown" && Timestamp() >= deadline)
                {
                    recovery.Finish(plan.ClusterId, plan.Command.OperationId);
                    return Results.Ok(new { stage = "expired", operationId = plan.Command.OperationId });
                }
                if (status.Stage == "alreadyInstalled")
                {
                    if (expectRestart) return Problem(409, "agent_restart_evidence_invalid");
                    recovery.Finish(plan.ClusterId, plan.Command.OperationId);
                }
                if (status.Stage == "publishing")
                    Advance(BrokerAgentCheckpoint.PackagePublishing);
                if (status.Stage == "awaitingRestart" && status.DispatchedAt is > 0)
                {
                    Advance(BrokerAgentCheckpoint.RestartIntent);
                    var health = await BrokerAgentHealthCheck.CheckAsync(kafka, plan.Binding, plan.Target.ExpectedBrokers, false, cancellation);
                    if (!Authorized()) return Problem(403, "agent_install_denied");
                    return Results.Ok(new { stage = health.Healthy ? "awaitingRestart" : "awaitingHealth", operationId,
                        status.DispatchedAt, status.Version, lastConfirmed = Checkpoint(), health });
                }
                if (status.Stage is not ("restartObserved" or "complete"))
                    return Results.Ok(new { status.Stage, operationId, status.DispatchedAt, status.Version,
                        lastConfirmed = Checkpoint(), requiresRenewal = status.Stage == "publishing" && Timestamp() >= deadline });
                if (status.ProcessStartedAt is not { } started || status.DispatchedAt is not { } dispatched ||
                    started < dispatched - 2 || started > Timestamp() + 2 || status.Digest != plan.Package.Digest ||
                    status.Version != plan.Package.Version)
                    return Problem(409, "agent_restart_evidence_invalid");
                Advance(BrokerAgentCheckpoint.RestartObserved);
                var recovered = await BrokerAgentHealthCheck.CheckAsync(kafka, plan.Binding, plan.Target.ExpectedBrokers, false, cancellation);
                if (!Authorized()) return Problem(403, "agent_install_denied");
                if (!recovered.Healthy)
                    return Results.Ok(new { stage = "awaitingHealth", operationId, processStartedAt = started,
                        dispatchedAt = dispatched, lastConfirmed = Checkpoint(), health = recovered });
                Advance(BrokerAgentCheckpoint.KafkaHealthy);
                var seen = metrics.AgentMetricAfter(plan.ClusterId, plan.Target.MonitoringInstance, started, plan.CreatedAt);
                if (seen is null)
                    return Results.Ok(new { stage = "awaitingMetrics", operationId, processStartedAt = started,
                        dispatchedAt = dispatched, lastConfirmed = Checkpoint(),
                        monitoringInstance = plan.Target.MonitoringInstance, health = recovered });
                if (status.Stage == "restartObserved")
                {
                    await Audit(audit, plan, SecurityAuditOutcome.Succeeded, "agent_recovery_observed", cancellation);
                    var completed = await host.CallAsync(plan.Connection, plan.Command with { Action = "complete" }, cancellation);
                    if (completed.Stage != "complete" || completed.Instance != status.Instance || completed.Digest != status.Digest)
                        return Problem(409, "agent_restart_evidence_changed");
                }
                recovery.Finish(plan.ClusterId, plan.Command.OperationId);
                return Results.Ok(new { stage = "complete", operationId = plan.Command.OperationId, status.Version,
                    status.Digest, dispatchedAt = dispatched, processStartedAt = started, metricSeenAt = seen,
                    monitoringInstance = plan.Target.MonitoringInstance, health = recovered });
            }
            finally { dispatch.Release(); }
        }

        group.MapPost("/apply", (BrokerAgentPlanInput input, HttpContext http, IKafkaAdministrationPort kafka, MonitoringStore metrics,
            ISecurityAuditSink audit, CancellationToken cancellation) => Progress(input, http, kafka, metrics, audit, true, cancellation))
            .WithManagedBrowserContract("managed-broker-agent-apply", typeof(BrokerAgentPlanInput))
            .WithMetadata(new RequestSizeLimitAttribute(8192)).RequireKafdeckAntiforgery();
        group.MapPost("/status", (BrokerAgentPlanInput input, HttpContext http, IKafkaAdministrationPort kafka, MonitoringStore metrics,
            ISecurityAuditSink audit, CancellationToken cancellation) => Progress(input, http, kafka, metrics, audit, false, cancellation))
            .WithManagedBrowserContract("managed-broker-agent-status", typeof(BrokerAgentPlanInput))
            .WithMetadata(new RequestSizeLimitAttribute(8192)).RequireKafdeckAntiforgery();
    }

    private static bool UncertainResponse(BrokerHostFailure failure) => failure.Code is
        "host_probe_unavailable" or "host_helper_unavailable" or "agent_response_invalid";

    private static long Timestamp() => DateTimeOffset.UtcNow.ToUnixTimeSeconds();

    private static string? Installer(HttpContext http, ManagedIdentityStore identity, BrokerHostOptions options, string cluster) =>
        options.Enabled && BrokerHostValidation.ClusterId(cluster) && http.User.Identity is not NonBrowserReadIdentity &&
        OperatorSessionContextFactory.TryCreate(http.User, out var session) && session is not null &&
        identity.Find(session) is { } user && options.AgentInstallerUserIds.Contains(user.Id) &&
        ManagedIdentityStore.SessionAllowed(user, session) && (user.Role == "owner" || user.ClusterIds.Contains(cluster, StringComparer.Ordinal))
            ? SecurityAuditPrincipal.FromOperator(session.Identity) : null;

    private static bool HasSource(MonitoringStore metrics, BrokerAgentTarget target) =>
        metrics.Sources().Any(source => source.ClusterId == target.ClusterId && source.Instance == target.MonitoringInstance &&
            source.Credential.Length != 0);

    private static (BrokerHostBinding Binding, BrokerHostDraft Connection)? GetTarget(BrokerHostStore registry, BrokerAgentTarget target)
    {
        var state = registry.Read();
        var binding = state.Bindings.SingleOrDefault(item => item.ClusterId == target.ClusterId && item.BrokerId == target.BrokerId);
        if (binding is null) return null;
        var host = state.Hosts.Single(item => item.Id == binding.HostId);
        if (host.Connection.CredentialId == target.CredentialId) return null;
        return (binding, host.Connection with { User = "kafdeck-agent", CredentialId = target.CredentialId });
    }

    private static IDataProtector Protector(HttpContext context) =>
        context.RequestServices.GetRequiredService<IDataProtectionProvider>().CreateProtector("kafdeck-broker-agent-plan-v1");
    private static string Protect(HttpContext context, BrokerAgentPlan plan) => Protector(context).Protect(JsonSerializer.Serialize(plan));
    private static BrokerAgentPlan? ReadPlan(HttpContext context, string? value)
    {
        if (value is not { Length: > 0 and <= 7000 }) return null;
        try { return JsonSerializer.Deserialize<BrokerAgentPlan>(Protector(context).Unprotect(value)); }
        catch (Exception error) when (error is CryptographicException or JsonException) { return null; }
    }

    private static void CheckPreview(BrokerAgentHostResult result)
    {
        CheckStatus(result);
        if (result.Stage != "preview" || result.Adapter is not ("systemd" or "compose") ||
            result.Instance is not { Length: > 0 and <= 160 } || result.Instance.Any(char.IsControl) ||
            result.CurrentDigest is not null && !BrokerAgentModels.ValidDigest(result.CurrentDigest) ||
            result.CurrentVersion is not null && !BrokerAgentModels.ValidVersion(result.CurrentVersion) ||
            result.LaunchMode is not ("argfile" or "existingJvmArgument") || result.MaintenanceFrom is not > 0 ||
            result.MaintenanceTo is not > 0 || result.MaintenanceTo <= result.MaintenanceFrom ||
            result.MaintenanceTo - result.MaintenanceFrom > 8 * 3600)
            throw new BrokerHostFailure("agent_response_invalid");
    }

    private static void CheckStatus(BrokerAgentHostResult result)
    {
        if (result.Stage == "blocked") throw new BrokerHostFailure(result.Code is { Length: <= 80 } && BrokerHostValidation.Name(result.Code)
            ? result.Code : "agent_host_unavailable");
        if (result.Stage is not ("preview" or "unknown" or "publishing" or "awaitingRestart" or "restartObserved" or "complete" or "alreadyInstalled"))
            throw new BrokerHostFailure("agent_response_invalid");
    }

    private static ValueTask Audit(ISecurityAuditSink sink, BrokerAgentPlan plan, SecurityAuditOutcome outcome, string code,
        CancellationToken cancellation) => sink.WriteAsync(new(DateTimeOffset.UtcNow, SecurityAuditEventType.BrokerHostManagement,
            plan.Principal, plan.Command.OperationId, plan.ClusterId, plan.Binding.BrokerId.ToString(CultureInfo.InvariantCulture), outcome, code),
            cancellation);

    private static IResult Problem(int status, string code) => ApiResults.Problem(new ApiProblemDefinition(status,
        "urn:kafdeck:problem:" + code, "Broker agent lifecycle",
        "The agent operation could not advance. Verify the approved package, binding, maintenance window and latest operation status.", code));
}
