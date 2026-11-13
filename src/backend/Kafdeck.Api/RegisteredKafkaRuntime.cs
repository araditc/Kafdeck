using System.Collections;
using System.Net;
using Confluent.Kafka;
using Kafdeck.Core.Security;
using Kafdeck.Infrastructure.Configuration;
using Microsoft.Extensions.Caching.Memory;

namespace Kafdeck.Api;

internal sealed class RegisteredKafkaRuntime : IDisposable
{
    private readonly IHttpContextAccessor _requests;
    private readonly ManagedIdentityStore _store;
    private readonly IPNetwork[] _networks;
    private readonly MemoryCache _credentials;
    private readonly object _credentialAdmission = new();
    private readonly object _requestClientsKey = new();

    // An injected cache is owned by this runtime; production retains the fixed bounded default.
    internal RegisteredKafkaRuntime(IHttpContextAccessor requests, ManagedIdentityStore store, IConfiguration settings, MemoryCache? credentials = null)
    {
        _requests = requests;
        _store = store;
        _networks = ReadNetworks(settings);
        _credentials = credentials ?? new MemoryCache(new MemoryCacheOptions { SizeLimit = 128 });
    }

    internal static IPNetwork[] ReadNetworks(IConfiguration settings) =>
        (settings.GetSection("Kafdeck:ConnectionWorkspace:RegistrationNetworks").Get<string[]>() ?? [])
        .Select(value => IPNetwork.TryParse(value, out var network) && network.PrefixLength > 0
            ? network : throw new KafdeckConfigurationException("Kafka registration networks must be bounded CIDR prefixes."))
        .ToArray();

    internal IReadOnlyList<ClusterProfile> IncludeRegistered(IReadOnlyList<ClusterProfile> configured) =>
        new ClusterProfiles(configured, _store);

    internal bool Remember(HttpContext context, string id, AdminClientConfig config)
    {
        if (config.SecurityProtocol == SecurityProtocol.Plaintext) return true;
        if (!OperatorSessionContextFactory.TryCreate(context.User, out var session) || session is null) return false;
        lock (_credentialAdmission)
        {
            var key = (session.SessionId, id);
            var lease = new CredentialLease(Guid.NewGuid(), config);
            // Never evict another admitted session merely to test a new target.
            // Absolute expiry still applies, and capacity remains fixed at 128.
            _credentials.Set(key, lease, new MemoryCacheEntryOptions
            {
                Size = 1, Priority = CacheItemPriority.NeverRemove,
                AbsoluteExpirationRelativeToNow = TimeSpan.FromMinutes(30)
            });
            return _credentials.TryGetValue(key, out CredentialLease? retained) && ReferenceEquals(lease, retained);
        }
    }

    // Snapshot keys include the exact credential lease for a secured target.
    // A different session, expired lease, or re-test cannot see those snapshots.
    internal string? SnapshotScope(string id)
    {
        var target = Find(id);
        if (target is null || target.Protocol == "PLAINTEXT") return null;
        return Lease(_requests.HttpContext, id)?.Version.ToString("N") ?? "locked";
    }

    internal IAdminClient? GetClient(string id)
    {
        var context = _requests.HttpContext;
        if (context is null) return null;
        var config = GetConfig(context, id);
        if (config is null) return null;
        lock (context.Items)
        {
            if (context.Items[_requestClientsKey] is not Dictionary<string, IAdminClient> clients)
            {
                clients = new Dictionary<string, IAdminClient>(StringComparer.Ordinal);
                context.Items[_requestClientsKey] = clients;
            }
            if (clients.TryGetValue(id, out var existing)) return existing;
            var client = new AdminClientBuilder(config).SetLogHandler((_, _) => { }).Build();
            context.Response.RegisterForDispose(client);
            clients.Add(id, client);
            return client;
        }
    }

    internal ConsumerConfig? GetConsumerConfig(string id)
    {
        var config = GetConfig(_requests.HttpContext, id);
        return config is null ? null : new ConsumerConfig(config.ToDictionary(entry => entry.Key, entry => entry.Value))
        {
            GroupId = $"kafdeck-record-read-{id}",
            EnableAutoCommit = false,
            EnableAutoOffsetStore = false,
            EnablePartitionEof = true,
            AllowAutoCreateTopics = false,
            AutoOffsetReset = AutoOffsetReset.Error,
            FetchMaxBytes = checked((int)Kafdeck.Core.Records.RecordOperationBudget.HardMaxRawBytes),
            MaxPartitionFetchBytes = checked((int)Kafdeck.Core.Records.RecordOperationBudget.HardMaxRawBytes),
        };
    }

    internal ProducerConfig? GetProducerConfig(string id)
    {
        var config = GetConfig(_requests.HttpContext, id);
        return config is null ? null : new ProducerConfig(config.ToDictionary(entry => entry.Key, entry => entry.Value))
        {
            EnableIdempotence = true,
            Acks = Acks.All,
            AllowAutoCreateTopics = false,
            MessageTimeoutMs = 15_000,
            RequestTimeoutMs = 10_000,
        };
    }

    private AdminClientConfig? GetConfig(HttpContext? context, string id)
    {
        var target = Find(id);
        if (context is null || target is null ||
            !OperatorSessionContextFactory.TryCreate(context.User, out var session) || session is null ||
            !ManagedIdentityEndpoints.Allows(_store, session, AuthorizationAction.ClusterRead, id) ||
            !KafkaConnectionWorkspace.IsRegistrationNetwork(target.BootstrapServers, _networks))
            return null;

        if (target.Protocol == "PLAINTEXT")
        {
            var config = new AdminClientConfig { BootstrapServers = string.Join(",", target.BootstrapServers), SecurityProtocol = SecurityProtocol.Plaintext };
            config.Set("allow.auto.create.topics", "false");
            return config;
        }
        return Lease(context, id)?.Config;
    }

    private RegisteredKafkaConnection? Find(string id) => id.StartsWith("connection-", StringComparison.Ordinal)
        ? KafkaConnectionWorkspace.ReadRegistered(_store).SingleOrDefault(target => target.Id == id)
        : null;

    private CredentialLease? Lease(HttpContext? context, string id) =>
        context is not null && OperatorSessionContextFactory.TryCreate(context.User, out var session) && session is not null &&
        _credentials.TryGetValue((session.SessionId, id), out CredentialLease? lease) ? lease : null;

    public void Dispose() => _credentials.Dispose();

    private sealed record CredentialLease(Guid Version, AdminClientConfig Config);

    private sealed class ClusterProfiles(IReadOnlyList<ClusterProfile> configured, ManagedIdentityStore store) : IReadOnlyList<ClusterProfile>
    {
        public int Count => configured.Count + KafkaConnectionWorkspace.ReadRegistered(store).Length;
        public ClusterProfile this[int index] => AllProfiles().ElementAt(index);
        public IEnumerator<ClusterProfile> GetEnumerator() => AllProfiles().GetEnumerator();
        private IEnumerable<ClusterProfile> AllProfiles() => configured.Concat(KafkaConnectionWorkspace.ReadRegistered(store).Select(target =>
            new ClusterProfile(target.Id, target.BootstrapServers, KafkaConnectionWorkspace.ParseProtocol(target.Protocol)!.Value,
                null, null, SchemaRegistry: target.SchemaRegistry is { } registry
                    ? new SchemaRegistryProfile(registry.Url, null, null)
                    : null, DisplayName: target.Name)));
        IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();
    }
}
