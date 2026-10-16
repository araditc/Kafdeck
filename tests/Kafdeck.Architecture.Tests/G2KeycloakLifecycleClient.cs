using System.Net;
using System.Net.Security;
using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;
using System.Text;
using System.Text.Json;
using System.Text.Json.Nodes;

namespace Kafdeck.Architecture.Tests;

/// <summary>Explicit disposable realm control; never an application authentication shortcut.</summary>
internal sealed class G2KeycloakLifecycleClient : IDisposable
{
    private readonly JsonDocument _manifest;
    private readonly X509Certificate2 _ca;
    private readonly HttpClient _http;
    private readonly RSA _controlKey;
    internal string Issuer => Field("issuer");
    internal string Audience => Field("audience");
    internal string ClientId => Field("clientId");
    internal string Subject => Field("subject");
    internal string CaPath => Field("caPemFile");
    internal string ClientKeyPath => Field("clientPrivateKeyFile");
    private string AdminRoot => Issuer[..Issuer.IndexOf("/realms/", StringComparison.Ordinal)] + "/admin/realms/kafdeck-g2";

    internal G2KeycloakLifecycleClient(string path)
    {
        _manifest = JsonDocument.Parse(File.ReadAllBytes(path));
        _ca = X509Certificate2.CreateFromPem(File.ReadAllText(CaPath));
        _controlKey = RSA.Create();
        _controlKey.ImportFromPem(File.ReadAllText(Field("controlPrivateKeyFile")));
        _http = new HttpClient(new SocketsHttpHandler
        {
            AllowAutoRedirect = false, UseProxy = false, UseCookies = false,
            ConnectTimeout = TimeSpan.FromSeconds(3),
            SslOptions = new SslClientAuthenticationOptions
            {
                CertificateChainPolicy = new X509ChainPolicy
                {
                    TrustMode = X509ChainTrustMode.CustomRootTrust, RevocationMode = X509RevocationMode.NoCheck,
                    CustomTrustStore = { _ca },
                },
            },
        }) { Timeout = TimeSpan.FromSeconds(10) };
    }
    private string Field(string name) => _manifest.RootElement.GetProperty(name).GetString()!;

    internal async Task<(HttpStatusCode Status, string? Token)> ExchangeAsync(RSA key, CancellationToken token,
        string? assertionClient = null, string? fixedAssertion = null)
    {
        var client = assertionClient ?? ClientId;
        var endpoint = Issuer + "/protocol/openid-connect/token";
        var now = DateTimeOffset.UtcNow.ToUnixTimeSeconds();
        var header = Encode(JsonSerializer.SerializeToUtf8Bytes(new { alg = "RS256", typ = "JWT" }));
        var payload = Encode(JsonSerializer.SerializeToUtf8Bytes(new
        {
            iss = client, sub = client, aud = endpoint, iat = now, nbf = now, exp = now + 45, jti = Guid.NewGuid().ToString("N"),
        }));
        var input = header + "." + payload;
        var assertion = fixedAssertion ?? input + "." + Encode(key.SignData(Encoding.ASCII.GetBytes(input), HashAlgorithmName.SHA256, RSASignaturePadding.Pkcs1));
        using var response = await _http.PostAsync(endpoint, new FormUrlEncodedContent(new Dictionary<string, string>
        {
            ["grant_type"] = "client_credentials", ["client_id"] = client,
            ["client_assertion_type"] = "urn:ietf:params:oauth:client-assertion-type:jwt-bearer", ["client_assertion"] = assertion,
        }), token);
        if (response.StatusCode != HttpStatusCode.OK) return (response.StatusCode, null);
        using var body = await ReadDocumentAsync(response, token);
        return (response.StatusCode, body.RootElement.GetProperty("access_token").GetString());
    }

    internal async Task<string> RotateSigningKeyAsync(CancellationToken token)
    {
        using var realm = await AdminAsync(HttpMethod.Get, "", null, token);
        var realmId = realm!.RootElement.GetProperty("id").GetString()!;
        using var providers = await AdminAsync(HttpMethod.Get, "/components?type=org.keycloak.keys.KeyProvider", null, token);
        var priority = providers!.RootElement.EnumerateArray().Select(component =>
            component.TryGetProperty("config", out var config) && config.TryGetProperty("priority", out var value) &&
            value.GetArrayLength() > 0 && int.TryParse(value[0].GetString(), out var parsed) ? parsed : 0).DefaultIfEmpty(0).Max();
        var nextPriority = checked(priority + 10).ToString(System.Globalization.CultureInfo.InvariantCulture);
        await AdminAsync(HttpMethod.Post, "/components", new
        {
            name = "g2-lifecycle-" + Guid.NewGuid().ToString("N"), parentId = realmId,
            providerId = "rsa-generated", providerType = "org.keycloak.keys.KeyProvider",
            config = new { priority = new[] { nextPriority }, enabled = new[] { "true" }, active = new[] { "true" },
                algorithm = new[] { "RS256" }, keySize = new[] { "2048" } },
        }, token);
        using var keys = await AdminAsync(HttpMethod.Get, "/keys", null, token);
        return keys!.RootElement.GetProperty("active").GetProperty("RS256").GetString()!;
    }

    internal async Task ReplaceServiceCertificateAsync(string certificate, CancellationToken token)
    {
        var client = await GetClientAsync(token);
        client["attributes"]!["jwt.credential.certificate"] = certificate;
        using var ignored = await AdminAsync(HttpMethod.Put, "/clients/" + client["id"]!.GetValue<string>(), client, token);
    }
    internal async Task SetServiceEnabledAsync(bool enabled, CancellationToken token)
    {
        var client = await GetClientAsync(token);
        client["enabled"] = enabled;
        using var ignored = await AdminAsync(HttpMethod.Put, "/clients/" + client["id"]!.GetValue<string>(), client, token);
    }
    private async Task<JsonObject> GetClientAsync(CancellationToken token)
    {
        using var clients = await AdminAsync(HttpMethod.Get, "/clients?clientId=" + Uri.EscapeDataString(ClientId), null, token);
        if (clients!.RootElement.GetArrayLength() != 1) throw new InvalidOperationException("Disposable lifecycle client is not unique.");
        return JsonNode.Parse(clients.RootElement[0].GetRawText())!.AsObject();
    }
    private async Task<JsonDocument?> AdminAsync(HttpMethod method, string suffix, object? value, CancellationToken token)
    {
        var exchange = await ExchangeAsync(_controlKey, token, Field("controlClientId"));
        if (exchange.Status != HttpStatusCode.OK || exchange.Token is null)
            throw new InvalidOperationException("Disposable realm control assertion was refused; token response withheld.");
        using var request = new HttpRequestMessage(method, AdminRoot + suffix);
        request.Headers.Authorization = new("Bearer", exchange.Token);
        if (value is not null) request.Content = new ByteArrayContent(JsonSerializer.SerializeToUtf8Bytes(value));
        if (request.Content is not null) request.Content.Headers.ContentType = new("application/json");
        using var response = await _http.SendAsync(request, HttpCompletionOption.ResponseHeadersRead, token);
        if (!response.IsSuccessStatusCode) throw new InvalidOperationException("Disposable realm lifecycle operation was refused (status " + (int)response.StatusCode + ").");
        return method == HttpMethod.Get ? await ReadDocumentAsync(response, token) : null;
    }
    private static async Task<JsonDocument> ReadDocumentAsync(HttpResponseMessage response, CancellationToken token)
    {
        if (response.Content.Headers.ContentLength > 262144) throw new InvalidOperationException("Lifecycle document exceeded its finite budget.");
        await using var stream = await response.Content.ReadAsStreamAsync(token);
        using var buffer = new MemoryStream();
        var chunk = new byte[4096];
        while (true)
        {
            var count = await stream.ReadAsync(chunk, token);
            if (count == 0) break;
            if (buffer.Length + count > 262144) throw new InvalidOperationException("Lifecycle document exceeded its finite budget.");
            buffer.Write(chunk, 0, count);
        }
        return JsonDocument.Parse(buffer.ToArray(), new JsonDocumentOptions { MaxDepth = 24 });
    }
    internal static string KeyId(string jwt)
    {
        var part = jwt.Split('.')[0].Replace('-', '+').Replace('_', '/');
        using var header = JsonDocument.Parse(Convert.FromBase64String(part.PadRight((part.Length + 3) / 4 * 4, '=')));
        return header.RootElement.GetProperty("kid").GetString()!;
    }
    private static string Encode(byte[] bytes) => Convert.ToBase64String(bytes).TrimEnd('=').Replace('+', '-').Replace('/', '_');
    public void Dispose() { _controlKey.Dispose(); _http.Dispose(); _ca.Dispose(); _manifest.Dispose(); }
}
