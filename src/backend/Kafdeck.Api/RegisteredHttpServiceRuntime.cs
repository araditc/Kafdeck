using System.Net;
using System.Net.Http.Headers;
using System.Net.Sockets;
using System.Security.Cryptography;
using System.Text;
using Microsoft.AspNetCore.DataProtection;

namespace Kafdeck.Api;

internal sealed class RegisteredHttpServiceRuntime(
    ManagedIdentityStore store,
    IHttpContextAccessor requests,
    IDataProtectionProvider protection,
    IConfiguration configuration)
{
    private readonly object _requestKey = new();
    private readonly IPNetwork[] _networks = RegisteredKafkaRuntime.ReadNetworks(configuration);

    internal string? RegistryRevision(string clusterId) =>
        Snapshot(clusterId, RegisteredHttpService.SchemaRegistry)?.Revision.ToString("N");

    internal string? ConnectRevision(string clusterId) =>
        Snapshot(clusterId, RegisteredHttpService.KafkaConnect)?.Revision.ToString("N");

    internal HttpClient? OpenRegistry(string clusterId) => Open(clusterId, RegisteredHttpService.SchemaRegistry);

    internal HttpClient? OpenConnect(string clusterId) => Open(clusterId, RegisteredHttpService.KafkaConnect);

    internal HttpClient? OpenKsql(string clusterId) => Open(clusterId, RegisteredHttpService.KsqlDb);

    private HttpClient? Open(string clusterId, RegisteredHttpService service)
    {
        var settings = Snapshot(clusterId, service);
        if (settings is null) return null;
        try { return Open(clusterId, service, settings); }
        catch (CryptographicException) { return null; }
    }

    private RegisteredHttpServiceSettings? Snapshot(string clusterId, RegisteredHttpService service)
    {
        if (!clusterId.StartsWith("connection-", StringComparison.Ordinal) || requests.HttpContext is not { } request)
            return null;
        lock (request.Items)
        {
            if (request.Items[_requestKey] is not Dictionary<(string, RegisteredHttpService), RegisteredHttpServiceSettings?> snapshots)
                request.Items[_requestKey] = snapshots = new();
            var key = (clusterId, service);
            if (!snapshots.TryGetValue(key, out var settings))
            {
                settings = KafkaConnectionWorkspace.ReadRegistered(store).SingleOrDefault(item => item.Id == clusterId)?.GetService(service);
                snapshots.Add(key, settings);
            }
            return settings;
        }
    }

    internal RegisteredHttpServiceSettings? Prepare(
        string clusterId, RegisteredHttpService service, SettingsInput input, RegisteredHttpServiceSettings? current)
    {
        var rawUrl = input.Url?.Trim();
        if (rawUrl is not { Length: > 0 and <= 2048 } || rawUrl.Any(char.IsControl) || rawUrl.Contains('\\') ||
            !Uri.TryCreate(rawUrl, UriKind.Absolute, out var uri) || uri.Scheme is not ("http" or "https") ||
            uri.UserInfo.Length != 0 || uri.Query.Length != 0 || uri.Fragment.Length != 0 ||
            uri.HostNameType is not (UriHostNameType.Dns or UriHostNameType.IPv4 or UriHostNameType.IPv6))
            return null;

        if (input.Authentication is not ("none" or "basic" or "bearer")) return null;
        if (input.Authentication != "none" && uri.Scheme != "https" && !uri.IsLoopback) return null;
        var url = uri.AbsoluteUri.EndsWith('/') ? uri.AbsoluteUri : uri.AbsoluteUri + "/";

        if (input.Authentication == "none")
            return new(url, "none", null, Guid.NewGuid());

        if (string.IsNullOrEmpty(input.Username) && string.IsNullOrEmpty(input.Secret) &&
            current?.Url == url && current.Authentication == input.Authentication &&
            current.ProtectedCredential is not null)
            return current with { Revision = Guid.NewGuid() };

        if (input.Secret is not { Length: > 0 and <= 4096 } || input.Secret.Any(char.IsControl))
            return null;

        string credential;
        if (input.Authentication == "basic")
        {
            if (input.Username is not { Length: > 0 and <= 512 } ||
                input.Username.Any(character => char.IsControl(character) || character == ':'))
                return null;
            credential = Convert.ToBase64String(Encoding.UTF8.GetBytes($"{input.Username}:{input.Secret}"));
        }
        else
        {
            if (input.Secret.Any(character => char.IsWhiteSpace(character) || character > 0x7e)) return null;
            credential = input.Secret;
        }

        return new(url, input.Authentication, Protector(clusterId, service).Protect(credential), Guid.NewGuid());
    }

    internal HttpClient Open(string clusterId, RegisteredHttpService service, RegisteredHttpServiceSettings settings)
    {
        var uri = new Uri(settings.Url);
        AuthenticationHeaderValue? auth = null;
        if (settings.ProtectedCredential is { } protectedCredential)
        {
            var credential = Protector(clusterId, service).Unprotect(protectedCredential);
            auth = new AuthenticationHeaderValue(settings.Authentication == "basic" ? "Basic" : "Bearer", credential);
        }
        var handler = new SocketsHttpHandler
        {
            AllowAutoRedirect = false,
            UseProxy = false,
            UseCookies = false,
            ConnectTimeout = TimeSpan.FromSeconds(5),
            ConnectCallback = (context, token) => Connect(uri, auth is not null && uri.Scheme == "http", context.DnsEndPoint, token),
        };
        var client = new HttpClient(handler) { BaseAddress = uri, Timeout = Timeout.InfiniteTimeSpan };
        client.DefaultRequestHeaders.Authorization = auth;
        return client;
    }

    private IDataProtector Protector(string clusterId, RegisteredHttpService service) =>
        protection.CreateProtector(service switch
        {
            RegisteredHttpService.SchemaRegistry => "Kafdeck.registered-schema-registry.v1",
            RegisteredHttpService.KafkaConnect => "Kafdeck.registered-kafka-connect.v1",
            RegisteredHttpService.KsqlDb => "Kafdeck.registered-ksqldb.v1",
            _ => throw new ArgumentOutOfRangeException(nameof(service)),
        }, clusterId);

    private async ValueTask<Stream> Connect(Uri uri, bool requireLoopback, DnsEndPoint endpoint, CancellationToken token)
    {
        if (endpoint.Port != uri.Port ||
            !string.Equals(endpoint.Host.Trim('[', ']'), uri.IdnHost.Trim('[', ']'), StringComparison.OrdinalIgnoreCase))
            throw new HttpRequestException("The service authority changed.");

        var addresses = IPAddress.TryParse(endpoint.Host.Trim('[', ']'), out var literal)
            ? new[] { literal }
            : await Dns.GetHostAddressesAsync(endpoint.Host, token);

        foreach (var address in addresses.Where(address => AllowedAddress(address) && (!requireLoopback || IPAddress.IsLoopback(address))))
        {
            var socket = new Socket(address.AddressFamily, SocketType.Stream, ProtocolType.Tcp);
            try
            {
                await socket.ConnectAsync(new IPEndPoint(address, endpoint.Port), token);
                return new NetworkStream(socket, ownsSocket: true);
            }
            catch (SocketException) { socket.Dispose(); }
            catch { socket.Dispose(); throw; }
        }
        throw new HttpRequestException("The service could not be reached on an allowed network.");
    }

    private bool AllowedAddress(IPAddress address)
    {
        if (address.IsIPv4MappedToIPv6 || address.IsIPv6LinkLocal || address.IsIPv6Multicast ||
            address.Equals(IPAddress.Any) || address.Equals(IPAddress.IPv6Any) || address.Equals(IPAddress.Broadcast))
            return false;
        if (address.AddressFamily == AddressFamily.InterNetwork)
        {
            var bytes = address.GetAddressBytes();
            if (bytes[0] >= 224 || (bytes[0] == 169 && bytes[1] == 254)) return false;
        }
        return _networks.Any(network => network.Contains(address));
    }

    internal sealed record SettingsInput(string? Url, string? Authentication, string? Username, string? Secret, Guid? Revision);
}
