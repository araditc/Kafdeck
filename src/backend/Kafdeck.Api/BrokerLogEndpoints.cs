using System.Globalization;
using System.Text.Json;
using System.Text.RegularExpressions;
using Kafdeck.Core.Kafka;
using Kafdeck.Core.Security;
using Kafdeck.Modules.Clusters;
using Microsoft.AspNetCore.Mvc;

namespace Kafdeck.Api;

internal static partial class BrokerLogEndpoints
{
    internal const int MaxLines = 200;
    private const int MaxEncodedLines = 48 * 1024;
    private static readonly JsonSerializerOptions Wire = new(JsonSerializerDefaults.Web);

    internal static void MapBrokerLogs(this WebApplication app, ManagedIdentityStore identity,
        BrokerHostOptions options, BrokerHostStore store, IBrokerLogReader reader)
    {
        var group = app.MapGroup("/api/v1/fleet");
        group.AddEndpointFilter(async (context, next) =>
        {
            context.HttpContext.Response.Headers.CacheControl = "no-store";
            return await next(context);
        });

        group.MapGet("/hosts/log-targets", (string clusterId, HttpContext http) =>
        {
            var session = BrowserSession(http);
            if (!MayRead(identity, options, session, clusterId))
                return Results.Ok(new { allowed = false, targets = Array.Empty<object>() });
            var targets = store.Read().Bindings.Where(binding => binding.ClusterId == clusterId).Select(binding => new
            {
                binding.BrokerId,
                monitoringInstance = options.LogSources.SingleOrDefault(source =>
                    source.ClusterId == clusterId && source.BrokerId == binding.BrokerId)?.MonitoringInstance,
            }).OrderBy(target => target.BrokerId);
            return Results.Ok(new { allowed = true, targets });
        }).WithManagedBrowserContract("managed-broker-log-targets");

        group.MapPost("/hosts/logs", async (BrokerLogRead input, HttpContext http, ISecurityAuditSink audit,
            ClusterExplorerService clusters, CancellationToken cancellation) =>
        {
            var now = DateTimeOffset.UtcNow.ToUnixTimeSeconds();
            if (!BrokerHostValidation.ClusterId(input.ClusterId) || input.BrokerId < 0 || !ValidWindow(input, now))
                return Problem("broker_log_window_invalid", 400);
            var session = BrowserSession(http);
            var correlation = Guid.NewGuid().ToString("N");

            Task Record(SecurityAuditOutcome outcome, string code) => session is null ? Task.CompletedTask :
                audit.WriteAsync(new SecurityAuditEvent(DateTimeOffset.UtcNow, SecurityAuditEventType.SensitiveRead,
                    SecurityAuditPrincipal.FromOperator(session.Identity), correlation, input.ClusterId,
                    input.BrokerId.ToString(CultureInfo.InvariantCulture), outcome, code), cancellation).AsTask();

            async Task<IResult> Denied()
            {
                await Record(SecurityAuditOutcome.Denied, "broker_log_read_denied");
                return Problem("broker_log_read_denied", 403);
            }

            if (!MayRead(identity, options, session, input.ClusterId)) return await Denied();
            var state = store.Read();
            var binding = state.Bindings.SingleOrDefault(candidate => candidate.ClusterId == input.ClusterId && candidate.BrokerId == input.BrokerId);
            if (binding is null)
            {
                await Record(SecurityAuditOutcome.Failed, "broker_log_target_unbound");
                return Problem("broker_log_target_unbound", 404);
            }
            var host = state.Hosts.Single(candidate => candidate.Id == binding.HostId);
            try
            {
                using var deadline = CancellationTokenSource.CreateLinkedTokenSource(cancellation);
                deadline.CancelAfter(TimeSpan.FromSeconds(8));
                var cluster = await clusters.GetClusterAsync(input.ClusterId, deadline.Token);
                if (cluster.Failure is not null || !cluster.Observation.IsFreshAt(DateTimeOffset.UtcNow) ||
                    cluster.Observation.Source == ObservationSource.StaleSnapshot || cluster.KafkaClusterId != binding.KafkaClusterId ||
                    !cluster.Brokers.Any(broker => broker.BrokerId == binding.BrokerId))
                    throw new BrokerHostFailure("broker_log_identity_unverified");
                var observation = await reader.ReadAsync(host.Connection, new(binding.TargetId, binding.BrokerId,
                    binding.KafkaClusterId, input.From, input.To), cancellation);
                var response = Prepare(input, observation);
                if (!MayRead(identity, options, session, input.ClusterId)) return await Denied();
                if (!store.Read().Bindings.Contains(binding)) throw new BrokerHostFailure("broker_log_binding_changed");
                await Record(SecurityAuditOutcome.Succeeded, "broker_log_read");
                return Results.Ok(response);
            }
            catch (OperationCanceledException) when (!cancellation.IsCancellationRequested)
            {
                await Record(SecurityAuditOutcome.Failed, "broker_log_timeout");
                return Problem("broker_log_timeout", 504);
            }
            catch (BrokerHostFailure failure)
            {
                await Record(SecurityAuditOutcome.Failed, failure.Code);
                return Problem(failure.Code, failure.Code == "host_probe_busy" ? 429 : 409);
            }
        }).WithManagedBrowserContract("managed-broker-logs-read", typeof(BrokerLogRead))
            .WithMetadata(new RequestSizeLimitAttribute(2048)).RequireKafdeckAntiforgery();
    }

    private static OperatorSessionContext? BrowserSession(HttpContext http) =>
        http.User.Identity is not NonBrowserReadIdentity && OperatorSessionContextFactory.TryCreate(http.User, out var session)
            ? session : null;

    internal static bool MayRead(ManagedIdentityStore identity, BrokerHostOptions options,
        OperatorSessionContext? session, string clusterId) =>
        options.Enabled && BrokerHostValidation.ClusterId(clusterId) && session is not null &&
        identity.Find(session) is { } user && options.LogReaderUserIds.Contains(user.Id) &&
        ManagedIdentityStore.SessionAllowed(user, session) &&
        (user.Role == "owner" || user.ClusterIds.Contains(clusterId, StringComparer.Ordinal));

    internal static bool ValidWindow(BrokerLogRead input, long now) =>
        input.From >= now - 48 * 3600 && input.To <= now + 120 &&
        input.To > input.From && input.To - input.From <= 3600;

    internal static BrokerLogPage Prepare(BrokerLogRead input, BrokerLogObservation observation)
    {
        if (observation.Version != 1 || observation.Adapter is not ("systemd" or "compose") ||
            observation.Lines is null || observation.Lines.Length > MaxLines)
            throw new BrokerHostFailure("broker_log_response_invalid");
        var safe = new List<BrokerLogLine>();
        var bytes = 0;
        var truncated = observation.Truncated;
        foreach (var line in observation.Lines.OrderByDescending(line => line?.TimeMs))
        {
            if (line?.Text is null || line.TimeMs < input.From * 1000 || line.TimeMs >= input.To * 1000)
                throw new BrokerHostFailure("broker_log_response_invalid");
            var sanitized = line with { Text = Redact(line.Text) };
            bytes += JsonSerializer.SerializeToUtf8Bytes(sanitized, Wire).Length + 1;
            if (bytes > MaxEncodedLines)
            {
                truncated = true;
                break;
            }
            safe.Add(sanitized);
        }
        safe.Reverse();
        return new(input.BrokerId, input.From, input.To, observation.Adapter, safe, truncated);
    }

    private static string Redact(string text) =>
        text.Length > 2048 || text.Any(character => char.IsControl(character) || char.GetUnicodeCategory(character) == UnicodeCategory.Format) ||
        SensitiveText().IsMatch(text) ? "[REDACTED]" : text;

    // Suppress the full entry for sensitive markers so multiline/structured
    // values and unquoted values cannot leave an uncensored suffix.
    [GeneratedRegex("""pass(?:word|phrase|wd)|credential|secret|token|authorization|auth[_ -]?header|cookie|sasl[._ -]|jaas|oauth|api[_ -]?key|private[_ -]?key|(?:bearer|basic)\s+[A-Za-z0-9+/=_-]{4,}|eyJ[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+|://[^/\s]+@|-----BEGIN """,
        RegexOptions.IgnoreCase | RegexOptions.CultureInvariant | RegexOptions.NonBacktracking)]
    private static partial Regex SensitiveText();

    private static IResult Problem(string code, int status) => ApiResults.Problem(new ApiProblemDefinition(status,
        "urn:kafdeck:problem:" + code, "Broker service logs", code switch
        {
            "broker_log_window_invalid" => "Choose a window of up to one hour within the last 48 hours.",
            "broker_log_read_denied" => "This account does not have permission to read service logs for this cluster.",
            "broker_log_target_unbound" => "This broker does not have a verified service binding.",
            _ => "The registered service logs could not be read safely. Verify the broker binding and the host log helper.",
        }, code));
}

internal sealed record BrokerLogPage(int BrokerId, long From, long To, string Adapter,
    IReadOnlyList<BrokerLogLine> Lines, bool Truncated);
