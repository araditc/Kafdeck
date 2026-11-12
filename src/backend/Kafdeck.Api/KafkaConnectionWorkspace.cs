using System.Net;
using System.Security.Cryptography;
using System.Text.Json;
using Confluent.Kafka;
using Kafdeck.Core.Security;
using Kafdeck.Infrastructure.Configuration;
using BrokerProtocol = Kafdeck.Infrastructure.Configuration.KafkaSecurityProtocol;

namespace Kafdeck.Api;

internal static class KafkaConnectionWorkspace
{
    private static readonly TimeSpan ProbeTimeout = TimeSpan.FromSeconds(5);
    private static readonly SemaphoreSlim Probes = new(4);

    internal static string RecentScope(OperatorIdentityKey key) =>
        Convert.ToHexStringLower(SHA256.HashData(JsonSerializer.SerializeToUtf8Bytes(key)));

    internal static void MapKafkaConnectionWorkspace(this WebApplication app, KafdeckOptions options, ManagedIdentityStore? store,
        Func<AdminClientConfig, CancellationToken, Task<Metadata>>? metadataProbe = null)
    {
        var networks = RegisteredKafkaRuntime.ReadNetworks(app.Configuration);
        var runtime = app.Services.GetService<RegisteredKafkaRuntime>();
        var configured = options.Clusters.Select(profile => new WorkspaceTarget(
            profile.Id, profile.Id, profile.BootstrapServers, ProtocolName(profile.SecurityProtocol), null, false)).ToArray();

        app.MapGet("/api/v1/connections/targets", (HttpContext context, KafdeckAuthorizationService authorization) =>
        {
            context.Response.Headers.CacheControl = "no-store";
            if (!IsOperator(context)) return Results.Unauthorized();

            var targets = configured.Concat(ReadRegistered(store).Select(WorkspaceTarget.FromRegistered))
                .Where(profile => Allowed(context, authorization, AuthorizationAction.ClusterRead, profile.Id));
            return Results.Ok(targets.ToArray());
        });

        async Task<IResult> Probe(
            HttpContext context, ConnectionDraft draft, KafdeckAuthorizationService authorization,
            CancellationToken cancellation, bool register, RegistrationAttempt? attempt = null)
        {
            IResult Reject(int status, string code, string title)
            {
                if (attempt is not null && !attempt.Created)
                {
                    attempt.Outcome = status is 401 or 403 ? SecurityAuditOutcome.Denied : SecurityAuditOutcome.Failed;
                    attempt.Category = code;
                }
                return Failure(status, code, title);
            }
            context.Response.Headers.CacheControl = "no-store";
            if (!IsOperator(context)) return Reject(401, "connection-authentication", "Operator authentication is required.");
            var owner = IsOwner(context, store);
            if (register && !owner) return Reject(403, "connection-target", "Only the installation administrator can register Kafka targets.");

            if (draft.BootstrapServers is not { Length: > 0 and <= 16 } ||
                draft.BootstrapServers.Any(server => string.IsNullOrWhiteSpace(server) || server.Length > 300) ||
                draft.BootstrapServers.Distinct(StringComparer.OrdinalIgnoreCase).Count() != draft.BootstrapServers.Length ||
                ParseProtocol(draft.Protocol) is not { } protocol)
                return Reject(400, "connection-input", "Invalid Kafka connection settings.");

            var candidates = configured.Concat(ReadRegistered(store).Select(WorkspaceTarget.FromRegistered));
            var profile = candidates.FirstOrDefault(candidate =>
                Matches(candidate, draft) && Allowed(context, authorization, AuthorizationAction.ClusterRead, candidate.Id));
            var pending = profile is null;
            if (pending && !owner)
                return Reject(403, "connection-target", "This Kafka target and protocol are not available to your account.");
            if ((pending || profile!.Registered) && !IsRegistrationNetwork(draft.BootstrapServers, networks))
                return Reject(403, "connection-network", "Enter broker IP addresses within the networks permitted by this installation.");
            var name = draft.Name?.Trim();
            if (pending && (!ValidName(name) ||
                new[] { draft.Username, draft.Password, draft.ClientKeyPassword }.Any(value => !string.IsNullOrEmpty(value) && name!.Contains(value, StringComparison.Ordinal))))
                return Reject(400, "connection-input", "Enter a valid connection name that does not contain credentials.");
            if (pending && register && configured.Any(candidate => string.Equals(candidate.Name, name, StringComparison.OrdinalIgnoreCase)))
                return Reject(409, "connection-name", "This Kafka connection name is already in use.");

            var config = CreateClientConfig(profile?.BootstrapServers ?? draft.BootstrapServers, protocol, draft);
            if (config is null) return Reject(400, "connection-input", "Invalid Kafka connection settings.");

            if (!Probes.Wait(0)) return Reject(429, "connection-busy", "Too many Kafka connection tests are running.");

            try
            {
                var metadata = metadataProbe is not null ? await metadataProbe(config, cancellation) : await Task.Run(() =>
                {
                    using var client = new AdminClientBuilder(config).SetLogHandler((_, _) => { }).Build();
                    return client.GetMetadata(ProbeTimeout);
                }, cancellation).ConfigureAwait(false);
                if (metadata.Brokers.Count == 0)
                    return Reject(502, "connection-failed", "Kafka did not return broker metadata.");

                if ((pending || profile!.Registered) &&
                    !IsRegistrationNetwork(metadata.Brokers.Select(broker =>
                        IPAddress.TryParse(broker.Host, out var address) && broker.Port is > 0 and <= 65535
                            ? new IPEndPoint(address, broker.Port).ToString() : "").ToArray(), networks))
                    return Reject(400, "connection-network", "Kafka advertised a broker outside the allowed registration networks.");

                if (pending ? !IsOwner(context, store) :
                    !Allowed(context, authorization, AuthorizationAction.ClusterRead, profile!.Id))
                    return Reject(403, "connection-target", "This Kafka target and protocol are not available to your account.");

                if (pending && register)
                {
                    using var lease = store!.TryLock();
                    if (lease is null) return Reject(409, "connection-save-busy", "Try saving the connection again.");
                    var state = store.Read();
                    var registered = state.Connections ?? [];
                    var existing = registered.Select(WorkspaceTarget.FromRegistered).FirstOrDefault(candidate => Matches(candidate, draft));
                    if (existing is not null)
                    {
                        profile = existing;
                    }
                    else
                    {
                        if (registered.Any(candidate => string.Equals(candidate.Name, name, StringComparison.OrdinalIgnoreCase)))
                            return Reject(409, "connection-name", "This Kafka connection name is already in use.");
                        if (registered.Length >= 256)
                            return Reject(409, "connection-limit", "This installation has reached its saved connection limit.");

                        var saved = new RegisteredKafkaConnection(
                            "connection-" + Guid.NewGuid().ToString("N"), name!, draft.BootstrapServers, draft.Protocol!, draft.Mechanism);
                        store.Write(state with { Connections = [.. registered, saved] });
                        profile = WorkspaceTarget.FromRegistered(saved);
                        if (attempt is not null) attempt.Created = true;
                    }
                }

                if (register && attempt is not null)
                {
                    attempt.TargetId = profile?.Id;
                    attempt.Outcome = SecurityAuditOutcome.Succeeded;
                    attempt.Category = attempt.Created ? "kafka_target_registered" : "kafka_target_registration_reused";
                }
                if (profile is { Registered: true }) runtime?.Remember(context, profile.Id, config);
                var brokers = pending || Allowed(context, authorization, AuthorizationAction.BrokerRead, profile!.Id)
                    ? metadata.Brokers.OrderBy(broker => broker.BrokerId).Take(100)
                        .Select(broker => new { id = broker.BrokerId, host = broker.Host, port = broker.Port }).ToArray()
                    : [];
                var topics = pending || Allowed(context, authorization, AuthorizationAction.TopicList, profile!.Id)
                    ? metadata.Topics.Where(topic => !topic.Error.IsError).OrderBy(topic => topic.Topic, StringComparer.Ordinal)
                        .Select(topic => new { name = topic.Topic, partitions = topic.Partitions.Count }).Take(251).ToArray()
                    : [];
                return Results.Ok(new
                {
                    clusterId = profile?.Id, brokers, topics = topics.Take(250),
                    topicsTruncated = topics.Length > 250, observedAt = DateTimeOffset.UtcNow,
                    requiresRegistration = profile is null,
                    savedTarget = register && profile?.Registered == true ? profile : null,
                });
            }
            catch (Exception exception) when (exception is KafkaException or ArgumentException or InvalidOperationException)
            {
                return Reject(502, "connection-failed", "Kafka did not accept the connection. Check the broker, credentials, and TLS settings.");
            }
            catch (Exception exception) when (exception is IOException or UnauthorizedAccessException)
            {
                return Reject(503, "connection-save-failed", "The Kafka connection could not be saved.");
            }
            finally { Probes.Release(); }
        }

        app.MapPost("/api/v1/connections/test", (
            HttpContext context, ConnectionDraft draft, KafdeckAuthorizationService authorization, CancellationToken cancellation) =>
            Probe(context, draft, authorization, cancellation, register: false))
        .WithMetadata(new Microsoft.AspNetCore.Mvc.RequestSizeLimitAttribute(384 * 1024))
        .RequireKafdeckAntiforgery();
        app.MapPost("/api/v1/connections/targets", async (
            HttpContext context, ConnectionDraft draft, KafdeckAuthorizationService authorization,
            ISecurityAuditSink audit, CancellationToken cancellation) =>
        {
            var attempt = new RegistrationAttempt();
            try { return await Probe(context, draft, authorization, cancellation, register: true, attempt); }
            finally
            {
                var session = OperatorSessionContextFactory.TryCreate(context.User, out var current) ? current : null;
                await audit.WriteAsync(new SecurityAuditEvent(DateTimeOffset.UtcNow, SecurityAuditEventType.KafkaTargetRegistration,
                    session is null ? SecurityAuditPrincipal.Anonymous : SecurityAuditPrincipal.FromOperator(session.Identity),
                    session?.SessionId.Value.ToString("N"), attempt.TargetId, null, attempt.Outcome, attempt.Category));
            }
        })
        .WithMetadata(new Microsoft.AspNetCore.Mvc.RequestSizeLimitAttribute(384 * 1024))
        .RequireKafdeckAntiforgery();
    }

    private sealed class RegistrationAttempt
    {
        internal bool Created { get; set; }
        internal string? TargetId { get; set; }
        internal SecurityAuditOutcome Outcome { get; set; } = SecurityAuditOutcome.Failed;
        internal string Category { get; set; } = "kafka_target_registration_failed";
    }

    internal static RegisteredKafkaConnection[] ReadRegistered(ManagedIdentityStore? store) =>
        store?.Read().Connections ?? [];

    private static bool IsOwner(HttpContext context, ManagedIdentityStore? store) =>
        store is not null && OperatorSessionContextFactory.TryCreate(context.User, out var session) &&
        session is not null && ManagedIdentityEndpoints.IsOwner(store, session);

    private static bool Matches(WorkspaceTarget target, ConnectionDraft draft) =>
        target.Protocol == draft.Protocol &&
        new HashSet<string>(target.BootstrapServers, StringComparer.OrdinalIgnoreCase).SetEquals(draft.BootstrapServers!);

    internal static bool IsRegistrationNetwork(string[] servers, IPNetwork[] networks) =>
        servers.All(server =>
            IPEndPoint.TryParse(server, out var endpoint) && endpoint.Port > 0 &&
            string.Equals(endpoint.ToString(), server, StringComparison.OrdinalIgnoreCase) &&
            !endpoint.Address.IsIPv4MappedToIPv6 && !endpoint.Address.IsIPv6Multicast &&
            networks.Any(network => network.Contains(endpoint.Address)));

    private static bool ValidName(string? name) => name is { Length: > 0 and <= 60 } &&
        !name.Any(character => char.IsControl(character) || character is >= '\u202a' and <= '\u202e' or >= '\u2066' and <= '\u2069');

    private static bool IsOperator(HttpContext context) => context.User.Identity is not NonBrowserReadIdentity &&
        OperatorSessionContextFactory.TryCreate(context.User, out _);

    private static bool Allowed(HttpContext context, KafdeckAuthorizationService authorization, AuthorizationAction action, string clusterId) =>
        authorization.Authorize(context.User, new AuthorizationRequest(action, clusterId)) == KafdeckAuthorizationOutcome.Allowed;

    private static IResult Failure(int status, string code, string title) =>
        Results.Problem(statusCode: status, type: $"urn:kafdeck:problem:{code}", title: title);

    private static string ProtocolName(BrokerProtocol protocol) => protocol switch
    {
        BrokerProtocol.Plaintext => "PLAINTEXT",
        BrokerProtocol.Ssl => "SSL",
        BrokerProtocol.SaslPlaintext => "SASL_PLAINTEXT",
        BrokerProtocol.SaslSsl => "SASL_SSL",
        _ => throw new ArgumentOutOfRangeException(nameof(protocol)),
    };

    internal static BrokerProtocol? ParseProtocol(string? protocol) => protocol switch
    {
        "PLAINTEXT" => BrokerProtocol.Plaintext,
        "SSL" => BrokerProtocol.Ssl,
        "SASL_PLAINTEXT" => BrokerProtocol.SaslPlaintext,
        "SASL_SSL" => BrokerProtocol.SaslSsl,
        _ => null,
    };

    private static AdminClientConfig? CreateClientConfig(IReadOnlyList<string> servers, BrokerProtocol protocol, ConnectionDraft draft)
    {
        var tls = protocol is BrokerProtocol.Ssl or BrokerProtocol.SaslSsl;
        var sasl = protocol is BrokerProtocol.SaslPlaintext or BrokerProtocol.SaslSsl;
        var mechanism = draft.Mechanism switch
        {
            "PLAIN" => Confluent.Kafka.SaslMechanism.Plain,
            "SCRAM-SHA-256" => Confluent.Kafka.SaslMechanism.ScramSha256,
            "SCRAM-SHA-512" => Confluent.Kafka.SaslMechanism.ScramSha512,
            _ => (Confluent.Kafka.SaslMechanism?)null,
        };
        if (sasl && (mechanism is null || !ValidCredential(draft.Username, 512) || !ValidCredential(draft.Password, 4096)))
            return null;
        if (!sasl && (draft.Mechanism is not null || draft.Username is not null || draft.Password is not null))
            return null;
        if ((!tls && (draft.CaCertificate is not null || draft.ClientCertificate is not null || draft.ClientKey is not null || draft.ClientKeyPassword is not null)) ||
            (draft.ClientCertificate is null) != (draft.ClientKey is null) ||
            (draft.ClientKeyPassword is not null && (draft.ClientKey is null || !ValidCredential(draft.ClientKeyPassword, 4096))) ||
            !ValidPem(draft.CaCertificate, "CERTIFICATE") || !ValidPem(draft.ClientCertificate, "CERTIFICATE") ||
            !ValidPem(draft.ClientKey, "PRIVATE KEY", "ENCRYPTED PRIVATE KEY", "RSA PRIVATE KEY", "EC PRIVATE KEY"))
            return null;

        var config = new AdminClientConfig
        {
            BootstrapServers = string.Join(",", servers),
            ClientId = "kafdeck-connection-workspace",
            SecurityProtocol = protocol switch
            {
                BrokerProtocol.Ssl => SecurityProtocol.Ssl,
                BrokerProtocol.SaslSsl => SecurityProtocol.SaslSsl,
                BrokerProtocol.SaslPlaintext => SecurityProtocol.SaslPlaintext,
                _ => SecurityProtocol.Plaintext,
            },
            SocketTimeoutMs = (int)ProbeTimeout.TotalMilliseconds,
        };
        config.Set("allow.auto.create.topics", "false");
        if (tls)
        {
            config.EnableSslCertificateVerification = true;
            config.SslEndpointIdentificationAlgorithm = SslEndpointIdentificationAlgorithm.Https;
            if (draft.CaCertificate is not null) config.SslCaPem = draft.CaCertificate;
            if (draft.ClientCertificate is not null) config.SslCertificatePem = draft.ClientCertificate;
            if (draft.ClientKey is not null) config.SslKeyPem = draft.ClientKey;
            if (draft.ClientKeyPassword is not null) config.SslKeyPassword = draft.ClientKeyPassword;
        }
        if (sasl)
        {
            config.SaslMechanism = mechanism;
            config.SaslUsername = draft.Username;
            config.SaslPassword = draft.Password;
        }
        return config;
    }

    private static bool ValidCredential(string? value, int maxLength) =>
        !string.IsNullOrWhiteSpace(value) && value.Length <= maxLength && !value.Any(char.IsControl);

    private static bool ValidPem(string? value, params string[] labels) => value is null ||
        (value.Length <= 64 * 1024 && !value.Contains('\0') &&
            PemEncoding.TryFind(value, out var fields) && labels.Contains(value[fields.Label]));

    internal sealed record ConnectionDraft(
        string[]? BootstrapServers, string? Protocol, string? Mechanism, string? Username, string? Password,
        string? CaCertificate, string? ClientCertificate, string? ClientKey, string? ClientKeyPassword, string? Name = null);

    private sealed record WorkspaceTarget(
        string Id, string Name, IReadOnlyList<string> BootstrapServers, string Protocol, string? Mechanism, bool Registered)
    {
        internal static WorkspaceTarget FromRegistered(RegisteredKafkaConnection connection) =>
            new(connection.Id, connection.Name, connection.BootstrapServers, connection.Protocol, connection.Mechanism, true);
    }
}
