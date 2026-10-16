using System.Net.Security;
using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;
using System.Text;
using System.Text.Json;
using static Kafdeck.Cli.CliOidcPkceAuthenticator;

namespace Kafdeck.Cli;

/// <summary>Public exact identity and file locators; never inline credentials.</summary>
public sealed record CliServiceOidcProfile(string Issuer, string Audience, string ClientId,
    string ExpectedSubject, string PrivateKeyFile, string? TrustedCaPemFile = null)
{
    internal CliOidcProfile Trust => new(Issuer, Audience, ClientId, TrustedCaPemFile);

    public void Validate()
    {
        Trust.Validate();
        if (string.IsNullOrWhiteSpace(ExpectedSubject) || ExpectedSubject.Length > 256 ||
            ExpectedSubject.Trim() != ExpectedSubject || ExpectedSubject.Any(c => char.IsControl(c) || c is '*' or '?') ||
            string.IsNullOrWhiteSpace(PrivateKeyFile) || PrivateKeyFile.Length > 2048 || PrivateKeyFile.Any(char.IsControl))
            throw new CliUsageException("The service profile requires an exact expectedSubject and finite privateKeyFile locator.");
    }

    public static async Task<CliServiceOidcProfile> LoadAsync(string path, CancellationToken cancellationToken)
    {
        try
        {
            await using var stream = File.OpenRead(path);
            using var json = JsonDocument.Parse(await ReadBoundedAsync(stream, 16 * 1024, cancellationToken));
            var root = json.RootElement;
            RequireUnique(root);
            if (root.EnumerateObject().Any(p => p.Name is not ("issuer" or "audience" or "clientId" or "expectedSubject" or "privateKeyFile" or "trustedCaPemFile")) ||
                new[] { "issuer", "audience", "clientId", "expectedSubject", "privateKeyFile" }.Any(name =>
                    !root.TryGetProperty(name, out var field) || field.ValueKind != JsonValueKind.String) ||
                root.TryGetProperty("trustedCaPemFile", out var ca) && ca.ValueKind is not (JsonValueKind.String or JsonValueKind.Null))
                throw new CliUsageException("The service profile accepts only issuer, audience, clientId, expectedSubject, privateKeyFile and optional trustedCaPemFile.");
            var profile = new CliServiceOidcProfile(Text(root, "issuer"), Text(root, "audience"), Text(root, "clientId"),
                Text(root, "expectedSubject"), Text(root, "privateKeyFile"), root.TryGetProperty("trustedCaPemFile", out ca) ? ca.GetString() : null);
            profile.Validate();
            return profile;
        }
        catch (Exception exception) when (exception is JsonException or CliOidcAuthenticationException)
        { throw new CliUsageException("The public service profile must be a closed JSON object within the 16 KiB budget."); }
    }
}

/// <summary>Single fresh private_key_jwt exchange; no browser, secret fallback or token persistence.</summary>
public static class CliServiceOidcAuthenticator
{
    public static readonly TimeSpan FlowTimeout = TimeSpan.FromSeconds(45);

    public static async Task<CliOidcSession> AcquireAsync(CliServiceOidcProfile profile, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(profile);
        profile.Validate();
        using var deadline = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        deadline.CancelAfter(FlowTimeout);
        var stage = CliOidcFailureStage.Discovery;
        try
        {
            using var key = await LoadPrivateKeyAsync(profile.PrivateKeyFile, deadline.Token);
            using var ca = await profile.Trust.LoadTrustedCaAsync(deadline.Token);
            using var handler = new SocketsHttpHandler { AllowAutoRedirect = false, UseCookies = false, UseProxy = false,
                ConnectTimeout = TimeSpan.FromSeconds(10) };
            if (ca is not null) handler.SslOptions = new SslClientAuthenticationOptions
            {
                CertificateChainPolicy = new X509ChainPolicy { TrustMode = X509ChainTrustMode.CustomRootTrust,
                    RevocationMode = X509RevocationMode.NoCheck, CustomTrustStore = { ca } },
            };
            using var http = new HttpClient(handler) { Timeout = TimeSpan.FromSeconds(15) };
            using var discovery = await GetDocumentAsync(http, profile.Issuer + "/.well-known/openid-configuration", deadline.Token);
            var metadata = discovery.RootElement;
            RequireUnique(metadata);
            var endpoint = profile.Issuer + "/protocol/openid-connect/token";
            if (Text(metadata, "issuer") != profile.Issuer || Text(metadata, "token_endpoint") != endpoint ||
                Text(metadata, "jwks_uri") != profile.Issuer + "/protocol/openid-connect/certs" ||
                !metadata.GetProperty("token_endpoint_auth_methods_supported").EnumerateArray().Any(v => v.GetString() == "private_key_jwt"))
                throw new CliOidcAuthenticationException();
            stage = CliOidcFailureStage.CodeExchange;
            var assertion = CreateAssertion(key, profile.ClientId, endpoint, DateTimeOffset.UtcNow);
            using var exchange = await http.PostAsync(endpoint, new FormUrlEncodedContent(new Dictionary<string, string>
            {
                ["grant_type"] = "client_credentials", ["client_id"] = profile.ClientId,
                ["client_assertion_type"] = "urn:ietf:params:oauth:client-assertion-type:jwt-bearer", ["client_assertion"] = assertion,
            }), deadline.Token);
            if (!exchange.IsSuccessStatusCode) throw new CliOidcAuthenticationException();
            stage = CliOidcFailureStage.TokenResponse;
            using var tokens = JsonDocument.Parse(await ReadResponseAsync(exchange, deadline.Token));
            RequireUnique(tokens.RootElement);
            if (!string.Equals(Text(tokens.RootElement, "token_type"), "Bearer", StringComparison.OrdinalIgnoreCase))
                throw new CliOidcAuthenticationException();
            var accessToken = Text(tokens.RootElement, "access_token");
            stage = CliOidcFailureStage.SigningKeys;
            using var jwks = await GetDocumentAsync(http, Text(metadata, "jwks_uri"), deadline.Token);
            var keys = ReadKeys(jwks.RootElement);
            stage = CliOidcFailureStage.AccessToken;
            using var claims = VerifyToken(accessToken, keys, profile.Trust, profile.Audience, "Bearer");
            stage = CliOidcFailureStage.BoundIdentity;
            RequireExpectedIdentity(claims.RootElement, profile);
            return new(accessToken, profile.Issuer, profile.ExpectedSubject, profile.ClientId,
                DateTimeOffset.FromUnixTimeSeconds(claims.RootElement.GetProperty("exp").GetInt64()));
        }
        catch (CliOidcAuthenticationException exception)
        { throw new CliOidcAuthenticationException(exception.Stage == CliOidcFailureStage.Unknown ? stage : exception.Stage); }
        catch (Exception exception) when (exception is JsonException or InvalidOperationException or KeyNotFoundException or FormatException or
            CryptographicException or HttpRequestException or ArgumentException)
        { throw new CliOidcAuthenticationException(stage); }
        catch (OperationCanceledException) when (!cancellationToken.IsCancellationRequested)
        { throw new CliOidcAuthenticationException(stage); }
    }

    internal static void RequireExpectedIdentity(JsonElement claims, CliServiceOidcProfile profile)
    {
        if (Text(claims, "sub") != profile.ExpectedSubject || Text(claims, "azp") != profile.ClientId)
            throw new CliOidcAuthenticationException(CliOidcFailureStage.BoundIdentity);
    }

    internal static async Task<RSA> LoadPrivateKeyAsync(string path, CancellationToken token)
    {
        await using var stream = File.OpenRead(path);
        var bytes = await ReadBoundedAsync(stream, 16 * 1024, token);
        var rsa = RSA.Create();
        try
        {
            rsa.ImportFromPem(Encoding.UTF8.GetString(bytes));
            var parameters = rsa.ExportParameters(true);
            try
            {
                if (rsa.KeySize is < 2048 or > 4096 || parameters.D is null ||
                    parameters.Exponent is null || !parameters.Exponent.AsSpan().SequenceEqual(new byte[] { 1, 0, 1 }))
                    throw new CliOidcAuthenticationException();
            }
            finally
            {
                foreach (var component in new[] { parameters.D, parameters.P, parameters.Q, parameters.DP, parameters.DQ, parameters.InverseQ })
                    if (component is not null) CryptographicOperations.ZeroMemory(component);
            }
            return rsa;
        }
        catch { rsa.Dispose(); throw new CliOidcAuthenticationException(); }
        finally { CryptographicOperations.ZeroMemory(bytes); }
    }

    internal static string CreateAssertion(RSA key, string clientId, string tokenEndpoint, DateTimeOffset now)
    {
        var issuedAt = now.ToUnixTimeSeconds();
        var header = Encode(JsonSerializer.SerializeToUtf8Bytes(new { alg = "RS256", typ = "JWT" }));
        var payload = Encode(JsonSerializer.SerializeToUtf8Bytes(new { iss = clientId, sub = clientId, aud = tokenEndpoint,
            iat = issuedAt, nbf = issuedAt, exp = issuedAt + 45, jti = Encode(RandomNumberGenerator.GetBytes(32)) }));
        var signingInput = header + "." + payload;
        return signingInput + "." + Encode(key.SignData(Encoding.ASCII.GetBytes(signingInput), HashAlgorithmName.SHA256, RSASignaturePadding.Pkcs1));
    }
}
