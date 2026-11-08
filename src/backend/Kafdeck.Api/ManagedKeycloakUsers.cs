using System.Net;
using System.Net.Http.Headers;
using System.Text.Json;
using Kafdeck.Infrastructure.Configuration;

namespace Kafdeck.Api;

internal sealed class ManagedKeycloakUsers : IDisposable
{
    private readonly HttpClient _http;
    private readonly string _clientId;
    private readonly string _secret;
    private readonly Uri _token;
    private readonly string _users;

    internal ManagedKeycloakUsers(OidcProfile oidc, ManagedIdentityOptions settings)
    {
        _clientId = settings.ServiceClientId!;
        _secret = new SecretResolver().Resolve(SecretReference.Parse("file:" + settings.ServiceSecretFile)).Reveal();
        var issuer = oidc.Issuer.TrimEnd('/');
        var separator = issuer.LastIndexOf("/realms/", StringComparison.Ordinal);
        _users = issuer[..separator] + "/admin" + issuer[separator..] + "/users";
        _token = new Uri(issuer + "/protocol/openid-connect/token");
        _http = new HttpClient(oidc.TrustedCaPemFile is { } ca ? OidcTrustedCaHandler.Create(ca) :
            new SocketsHttpHandler { AllowAutoRedirect = false, UseProxy = false, ConnectTimeout = TimeSpan.FromSeconds(5) })
        { Timeout = TimeSpan.FromSeconds(10), MaxResponseContentBufferSize = 1024 * 1024 };
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
