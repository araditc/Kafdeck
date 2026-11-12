using System.Net;
using System.Net.Http.Headers;
using System.Text.Json;
using Kafdeck.Infrastructure.Configuration;

namespace Kafdeck.Api;

internal sealed class ManagedKeycloakUsers : IDisposable
{
    internal const string ProvisioningAttribute = "kafdeckProvisioningId";
    private readonly HttpClient _http;
    private readonly string _clientId;
    private readonly string _secret;
    private readonly Uri _token;
    private readonly string _users;

    internal ManagedKeycloakUsers(OidcProfile oidc, ManagedIdentityOptions settings)
        : this(CreateHttpClient(oidc), oidc.Issuer, settings.ServiceClientId!,
            new SecretResolver().Resolve(SecretReference.Parse("file:" + settings.ServiceSecretFile)).Reveal()) { }

    internal ManagedKeycloakUsers(HttpClient http, string issuer, string clientId, string secret)
    {
        _clientId = clientId;
        _secret = secret;
        issuer = issuer.TrimEnd('/');
        var separator = issuer.LastIndexOf("/realms/", StringComparison.Ordinal);
        _users = issuer[..separator] + "/admin" + issuer[separator..] + "/users";
        _token = new Uri(issuer + "/protocol/openid-connect/token");
        _http = http;
    }

    private static HttpClient CreateHttpClient(OidcProfile oidc) =>
        new(oidc.TrustedCaPemFile is { } ca ? OidcTrustedCaHandler.Create(ca) :
            new SocketsHttpHandler { AllowAutoRedirect = false, UseProxy = false, ConnectTimeout = TimeSpan.FromSeconds(5) })
        { Timeout = TimeSpan.FromSeconds(10), MaxResponseContentBufferSize = 1024 * 1024 };

    internal async Task<bool> ProvisioningProfileReady()
    {
        using var response = await Send(HttpMethod.Get, _users + "/profile");
        response.EnsureSuccessStatusCode();
        using var body = JsonDocument.Parse(await response.Content.ReadAsStreamAsync());
        if (!body.RootElement.TryGetProperty("attributes", out var attributes) || attributes.ValueKind != JsonValueKind.Array)
            return false;
        var matches = attributes.EnumerateArray().Where(attribute => attribute.TryGetProperty("name", out var name) &&
            name.GetString() == ProvisioningAttribute).ToArray();
        return matches.Length == 1 && matches[0].TryGetProperty("permissions", out var permissions) &&
            AdminOnly(permissions, "view") && AdminOnly(permissions, "edit") &&
            (!matches[0].TryGetProperty("multivalued", out var multiple) || multiple.ValueKind == JsonValueKind.False);
    }

    private static bool AdminOnly(JsonElement permissions, string name) =>
        permissions.TryGetProperty(name, out var roles) && roles.ValueKind == JsonValueKind.Array &&
        roles.GetArrayLength() == 1 && roles[0].GetString() == "admin";

    internal async Task<(Guid? Id, HttpStatusCode Status)> CreateProvisioned(string email, string password, string proof)
    {
        using var response = await Send(HttpMethod.Post, _users, new
        {
            username = email, email, emailVerified = false, enabled = true,
            firstName = email[..email.IndexOf('@')], lastName = "Kafdeck",
            attributes = new Dictionary<string, string[]> { [ProvisioningAttribute] = [proof] },
            credentials = new[] { new { type = "password", value = password, temporary = true } },
        });
        if (response.StatusCode != HttpStatusCode.Created) return (null, response.StatusCode);
        var location = response.Headers.Location?.OriginalString;
        if (location is null || !Guid.TryParse(location[(location.LastIndexOf('/') + 1)..], out var id) || id == Guid.Empty)
            throw new InvalidDataException("Keycloak did not return a usable account identifier.");
        return (id, response.StatusCode);
    }

    internal async Task<Guid?> FindProvisioned(string email, string proof, Guid? knownId)
    {
        using var response = await Send(HttpMethod.Get, knownId is { } id ? _users + "/" + id.ToString("D") :
            _users + "?username=" + Uri.EscapeDataString(email) + "&exact=true&max=2&briefRepresentation=false");
        if (response.StatusCode == HttpStatusCode.NotFound) return null;
        response.EnsureSuccessStatusCode();
        using var body = JsonDocument.Parse(await response.Content.ReadAsStreamAsync());
        var user = body.RootElement;
        if (knownId is null)
        {
            if (user.ValueKind != JsonValueKind.Array || user.GetArrayLength() != 1) return null;
            user = user[0];
        }
        if (!user.TryGetProperty("id", out var userId) || !Guid.TryParse(userId.GetString(), out var found) || found == Guid.Empty ||
            (knownId is { } expected && expected != found) ||
            !user.TryGetProperty("username", out var username) || !string.Equals(username.GetString(), email, StringComparison.OrdinalIgnoreCase) ||
            !user.TryGetProperty("email", out var address) || !string.Equals(address.GetString(), email, StringComparison.OrdinalIgnoreCase) ||
            !user.TryGetProperty("enabled", out var enabled) || enabled.ValueKind != JsonValueKind.True ||
            !user.TryGetProperty("attributes", out var attributes) ||
            !attributes.TryGetProperty(ProvisioningAttribute, out var value) || value.ValueKind != JsonValueKind.Array ||
            value.GetArrayLength() != 1 || value[0].GetString() != proof) return null;
        return found;
    }

    internal async Task<bool> HasExistingUsers()
    {
        using var response = await Send(HttpMethod.Get, _users + "?max=2");
        response.EnsureSuccessStatusCode();
        using var body = JsonDocument.Parse(await response.Content.ReadAsStreamAsync());
        return body.RootElement.EnumerateArray().Any(user =>
            user.GetProperty("username").GetString() != "service-account-" + _clientId);
    }

    internal async Task<(Guid? Id, HttpStatusCode Status)> Create(string email, string password, bool temporary)
    {
        using var response = await Send(HttpMethod.Post, _users, new
        {
            username = email, email, emailVerified = false, enabled = true,
            firstName = email[..email.IndexOf('@')], lastName = "Kafdeck",
            credentials = new[] { new { type = "password", value = password, temporary } },
        });
        if (response.StatusCode != HttpStatusCode.Created) return (null, response.StatusCode);
        var location = response.Headers.Location?.OriginalString;
        if (location is null || !Guid.TryParse(location[(location.LastIndexOf('/') + 1)..], out var id) || id == Guid.Empty)
            throw new InvalidDataException("Keycloak did not return a usable account identifier.");
        return (id, response.StatusCode);
    }

    internal async Task SetEnabled(Guid id, bool enabled)
    {
        using var response = await Send(HttpMethod.Put, _users + "/" + id, new { enabled });
        response.EnsureSuccessStatusCode();
        if (!enabled)
        {
            using var logout = await Send(HttpMethod.Post, _users + "/" + id + "/logout");
            logout.EnsureSuccessStatusCode();
        }
    }

    private async Task<HttpResponseMessage> Send(HttpMethod method, string target, object? body = null)
    {
        using var credentials = new FormUrlEncodedContent(new Dictionary<string, string>
        {
            ["grant_type"] = "client_credentials", ["client_id"] = _clientId, ["client_secret"] = _secret,
        });
        using var token = await _http.PostAsync(_token, credentials);
        token.EnsureSuccessStatusCode();
        using var tokenBody = JsonDocument.Parse(await token.Content.ReadAsStreamAsync());
        var accessToken = tokenBody.RootElement.GetProperty("access_token").GetString();
        if (string.IsNullOrEmpty(accessToken)) throw new InvalidDataException("Keycloak service authentication failed.");
        using var request = new HttpRequestMessage(method, target);
        request.Headers.Authorization = new AuthenticationHeaderValue("Bearer", accessToken);
        if (body is not null) request.Content = JsonContent.Create(body);
        return await _http.SendAsync(request);
    }

    public void Dispose() => _http.Dispose();
}
