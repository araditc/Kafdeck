using System.Diagnostics;
using System.Net;
using System.Net.Security;
using System.Net.Sockets;
using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;
using System.Text;
using System.Text.Json;

namespace Kafdeck.Cli;

public sealed record CliOidcProfile(string Issuer, string Audience, string ClientId, string? TrustedCaPemFile = null)
{
    public static async Task<CliOidcProfile> LoadAsync(string path, CancellationToken cancellationToken)
    {
        try
        {
            await using var stream = File.OpenRead(path);
            using var json = JsonDocument.Parse(await CliOidcPkceAuthenticator.ReadBoundedAsync(stream, 16 * 1024, cancellationToken));
            var root = json.RootElement;
            if (root.ValueKind != JsonValueKind.Object || root.EnumerateObject().Select(p => p.Name).Distinct(StringComparer.Ordinal).Count() != root.EnumerateObject().Count() ||
                root.EnumerateObject().Any(p => p.Name is not ("issuer" or "audience" or "clientId" or "trustedCaPemFile")) ||
                new[] { "issuer", "audience", "clientId" }.Any(name => !root.TryGetProperty(name, out var field) || field.ValueKind != JsonValueKind.String) ||
                root.TryGetProperty("trustedCaPemFile", out var ca) && ca.ValueKind is not (JsonValueKind.String or JsonValueKind.Null))
                throw new CliUsageException("The public OIDC profile requires string issuer, audience and clientId, plus optional trustedCaPemFile only.");
            var profile = new CliOidcProfile(root.GetProperty("issuer").GetString()!, root.GetProperty("audience").GetString()!,
                root.GetProperty("clientId").GetString()!, root.TryGetProperty("trustedCaPemFile", out ca) ? ca.GetString() : null);
            profile.Validate();
            return profile;
        }
        catch (Exception ex) when (ex is JsonException or CliOidcAuthenticationException)
        { throw new CliUsageException("The public OIDC profile must be valid JSON within the 16 KiB budget."); }
    }

    public void Validate()
    {
        if (string.IsNullOrWhiteSpace(Issuer) || Issuer.Length > 2048 || !Uri.TryCreate(Issuer, UriKind.Absolute, out var issuer) ||
            issuer.Scheme != "https" || issuer.AbsoluteUri != Issuer || !string.IsNullOrEmpty(issuer.UserInfo) ||
            !string.IsNullOrEmpty(issuer.Query) || !string.IsNullOrEmpty(issuer.Fragment) || Issuer.EndsWith('/'))
            throw new CliUsageException("The OIDC profile requires an exact canonical HTTPS realm issuer.");
        foreach (var value in new[] { Audience, ClientId })
            if (string.IsNullOrWhiteSpace(value) || value.Length > 256 || value.Trim() != value ||
                value.Any(c => char.IsControl(c) || c is '*' or '?'))
                throw new CliUsageException("The OIDC audience and public client ID must be finite exact identifiers.");
        if (TrustedCaPemFile is not null && (string.IsNullOrWhiteSpace(TrustedCaPemFile) || TrustedCaPemFile.Length > 2048 || TrustedCaPemFile.Any(char.IsControl)))
            throw new CliUsageException("The OIDC CA path must be finite and nonempty.");
    }

    internal async Task<X509Certificate2?> LoadTrustedCaAsync(CancellationToken token)
    {
        if (TrustedCaPemFile is null) return null;
        try
        {
            await using var stream = File.OpenRead(TrustedCaPemFile);
            var bytes = await CliOidcPkceAuthenticator.ReadBoundedAsync(stream, 16 * 1024, token);
            return X509Certificate2.CreateFromPem(Encoding.UTF8.GetString(bytes));
        }
        catch (Exception ex) when (ex is CryptographicException or CliOidcAuthenticationException)
        { throw new CliUsageException("The configured OIDC CA must be valid PEM within the 16 KiB budget."); }
    }
}

public enum CliOidcFailureStage { Unknown, Discovery, Callback, CodeExchange, TokenResponse, SigningKeys, IdToken, AccessToken, BoundIdentity }

public sealed class CliOidcAuthenticationException : Exception
{
    public CliOidcAuthenticationException(CliOidcFailureStage stage = CliOidcFailureStage.Unknown)
        : base("OIDC sign-in was rejected. No credentials or provider response were retained.") { Stage = stage; }
    public CliOidcFailureStage Stage { get; }
}

public sealed class CliOidcSession : IDisposable
{
    private string? _accessToken;
    internal CliOidcSession(string token, string issuer, string subject, string clientId, DateTimeOffset expires)
    { _accessToken = token; Issuer = issuer; Subject = subject; ClientId = clientId; ExpiresAtUtc = expires; }
    internal string AccessToken => _accessToken ?? throw new ObjectDisposedException(nameof(CliOidcSession));
    public string Issuer { get; }
    public string Subject { get; }
    public string ClientId { get; }
    public DateTimeOffset ExpiresAtUtc { get; }
    public void Dispose() => _accessToken = null;
    public override string ToString() => "[memory-only OIDC session]";
}

/// <summary>Real authorization-code S256 flow. Listener exists before browser navigation; tokens never leave memory.</summary>
public static class CliOidcPkceAuthenticator
{
    public static readonly TimeSpan FlowTimeout = TimeSpan.FromMinutes(3);

    public static async Task<CliOidcSession> AcquireAsync(CliOidcProfile profile,
        Func<Uri, CancellationToken, Task>? openBrowser = null, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(profile);
        profile.Validate();
        using var deadline = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        deadline.CancelAfter(FlowTimeout);
        using var ca = await profile.LoadTrustedCaAsync(deadline.Token);
        using var handler = new SocketsHttpHandler { AllowAutoRedirect = false, UseCookies = false, UseProxy = false, ConnectTimeout = TimeSpan.FromSeconds(10) };
        if (ca is not null) handler.SslOptions = new SslClientAuthenticationOptions
        {
            CertificateChainPolicy = new X509ChainPolicy { TrustMode = X509ChainTrustMode.CustomRootTrust,
                RevocationMode = X509RevocationMode.NoCheck, CustomTrustStore = { ca } },
        };
        using var http = new HttpClient(handler) { Timeout = TimeSpan.FromSeconds(15) };
        var listener = new TcpListener(IPAddress.Loopback, 0);
        var stage = CliOidcFailureStage.Discovery;
        try
        {
            using var discovery = await GetDocumentAsync(http, profile.Issuer + "/.well-known/openid-configuration", deadline.Token);
            var metadata = discovery.RootElement;
            RequireUnique(metadata);
            if (Text(metadata, "issuer") != profile.Issuer ||
                Text(metadata, "authorization_endpoint") != profile.Issuer + "/protocol/openid-connect/auth" ||
                Text(metadata, "token_endpoint") != profile.Issuer + "/protocol/openid-connect/token" ||
                Text(metadata, "jwks_uri") != profile.Issuer + "/protocol/openid-connect/certs" ||
                !metadata.GetProperty("code_challenge_methods_supported").EnumerateArray().Any(v => v.GetString() == "S256"))
                throw new CliOidcAuthenticationException();

            stage = CliOidcFailureStage.Callback;
            listener.Start(8);
            var address = (IPEndPoint)listener.LocalEndpoint;
            var redirect = new Uri($"http://127.0.0.1:{address.Port}{CliOidcLoopbackPolicy.CallbackPath}");
            CliOidcLoopbackPolicy.RequireExactListenerAddress(redirect, address.Address);
            var state = Encode(RandomNumberGenerator.GetBytes(32));
            var nonce = Encode(RandomNumberGenerator.GetBytes(32));
            var verifier = Encode(RandomNumberGenerator.GetBytes(48));
            var challenge = Encode(SHA256.HashData(Encoding.ASCII.GetBytes(verifier)));
            var authorization = new Uri(Text(metadata, "authorization_endpoint") + "?" + Query(new Dictionary<string, string>
            {
                ["client_id"] = profile.ClientId, ["response_type"] = "code", ["scope"] = "openid",
                ["redirect_uri"] = redirect.AbsoluteUri, ["code_challenge_method"] = "S256", ["code_challenge"] = challenge,
                ["state"] = state, ["nonce"] = nonce,
            }));
            var callback = ReceiveCodeAsync(listener, redirect, profile.Issuer, state, deadline.Token);
            string code;
            try
            {
                await (openBrowser ?? OpenSystemBrowserAsync)(authorization, deadline.Token);
                code = await callback;
            }
            catch
            {
                deadline.Cancel();
                listener.Stop();
                try { await callback; } catch (Exception) { }
                throw;
            }
            listener.Stop();
            stage = CliOidcFailureStage.CodeExchange;
            using var exchange = await http.PostAsync(Text(metadata, "token_endpoint"), new FormUrlEncodedContent(new Dictionary<string, string>
            {
                ["grant_type"] = "authorization_code", ["client_id"] = profile.ClientId,
                ["redirect_uri"] = redirect.AbsoluteUri, ["code"] = code, ["code_verifier"] = verifier,
            }), deadline.Token);
            if (!exchange.IsSuccessStatusCode) throw new CliOidcAuthenticationException();
            stage = CliOidcFailureStage.TokenResponse;
            using var tokens = JsonDocument.Parse(await ReadResponseAsync(exchange, deadline.Token));
            RequireUnique(tokens.RootElement);
            if (!string.Equals(Text(tokens.RootElement, "token_type"), "Bearer", StringComparison.OrdinalIgnoreCase))
                throw new CliOidcAuthenticationException();
            var accessToken = Text(tokens.RootElement, "access_token");
            var idToken = Text(tokens.RootElement, "id_token");
            stage = CliOidcFailureStage.SigningKeys;
            using var jwks = await GetDocumentAsync(http, Text(metadata, "jwks_uri"), deadline.Token);
            var keys = ReadKeys(jwks.RootElement);
            stage = CliOidcFailureStage.IdToken;
            using var idClaims = VerifyToken(idToken, keys, profile, profile.ClientId, "ID");
            if (!FixedEquals(Text(idClaims.RootElement, "nonce"), nonce)) throw new CliOidcAuthenticationException();
            stage = CliOidcFailureStage.AccessToken;
            using var accessClaims = VerifyToken(accessToken, keys, profile, profile.Audience, "Bearer");
            stage = CliOidcFailureStage.BoundIdentity;
            var subject = Text(accessClaims.RootElement, "sub");
            if (Text(idClaims.RootElement, "sub") != subject || Text(accessClaims.RootElement, "azp") != profile.ClientId)
                throw new CliOidcAuthenticationException();
            if (idClaims.RootElement.TryGetProperty("at_hash", out var hash) &&
                !FixedEquals(hash.GetString()!, Encode(SHA256.HashData(Encoding.ASCII.GetBytes(accessToken))[..16])))
                throw new CliOidcAuthenticationException();
            return new CliOidcSession(accessToken, profile.Issuer, subject, profile.ClientId,
                DateTimeOffset.FromUnixTimeSeconds(accessClaims.RootElement.GetProperty("exp").GetInt64()));
        }
        catch (CliOidcAuthenticationException exception)
        { throw new CliOidcAuthenticationException(exception.Stage == CliOidcFailureStage.Unknown ? stage : exception.Stage); }
        catch (Exception ex) when (ex is JsonException or InvalidOperationException or KeyNotFoundException or FormatException or
            CryptographicException or HttpRequestException or SocketException or ArgumentException)
        { throw new CliOidcAuthenticationException(stage); }
        catch (OperationCanceledException) when (!cancellationToken.IsCancellationRequested)
        { throw new CliOidcAuthenticationException(stage); }
        finally { listener.Stop(); }
    }

    private static Task OpenSystemBrowserAsync(Uri authorization, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        Process.Start(new ProcessStartInfo(authorization.AbsoluteUri) { UseShellExecute = true });
        return Task.CompletedTask;
    }

    private static async Task<string> ReceiveCodeAsync(TcpListener listener, Uri redirect, string issuer, string state, CancellationToken token)
    {
        for (var attempt = 0; attempt < 8; attempt++)
        {
            using var client = await listener.AcceptTcpClientAsync(token);
            using var requestDeadline = CancellationTokenSource.CreateLinkedTokenSource(token);
            requestDeadline.CancelAfter(TimeSpan.FromSeconds(5));
            await using var stream = client.GetStream();
            var bytes = new byte[8192];
            var count = 0;
            while (count < bytes.Length)
            {
                var read = await stream.ReadAsync(bytes.AsMemory(count), requestDeadline.Token);
                if (read == 0) break;
                count += read;
                if (Encoding.ASCII.GetString(bytes, 0, count).Contains("\r\n\r\n", StringComparison.Ordinal)) break;
            }
            string? code = null;
            try
            {
                var request = Encoding.ASCII.GetString(bytes, 0, count);
                if (!request.EndsWith("\r\n\r\n", StringComparison.Ordinal)) throw new CliOidcAuthenticationException();
                var lines = request.Split("\r\n", StringSplitOptions.None);
                var first = lines[0].Split(' ');
                var hosts = lines.Where(l => l.StartsWith("Host:", StringComparison.OrdinalIgnoreCase)).ToArray();
                if (first.Length != 3 || first[0] != "GET" || first[2] != "HTTP/1.1" || !first[1].StartsWith('/') ||
                    hosts.Length != 1 || hosts[0][5..].Trim() != redirect.Authority) throw new CliOidcAuthenticationException();
                var target = new Uri(redirect.GetLeftPart(UriPartial.Authority) + first[1]);
                if (target.AbsolutePath != redirect.AbsolutePath || !string.IsNullOrEmpty(target.Fragment)) throw new CliOidcAuthenticationException();
                var parameters = new Dictionary<string, string>(StringComparer.Ordinal);
                foreach (var pair in target.Query.TrimStart('?').Split('&', StringSplitOptions.RemoveEmptyEntries))
                {
                    var split = pair.Split('=', 2);
                    if (split.Length != 2 || !parameters.TryAdd(Uri.UnescapeDataString(split[0]), Uri.UnescapeDataString(split[1])))
                        throw new CliOidcAuthenticationException();
                }
                if (!parameters.TryGetValue("state", out var receivedState) || !FixedEquals(receivedState, state) ||
                    parameters.TryGetValue("error", out _) || parameters.TryGetValue("iss", out var receivedIssuer) && receivedIssuer != issuer ||
                    !parameters.TryGetValue("code", out code) || string.IsNullOrWhiteSpace(code) || code.Length > 2048 || code.Any(char.IsControl))
                    throw new CliOidcAuthenticationException();
            }
            catch (Exception ex) when (ex is CliOidcAuthenticationException or UriFormatException) { code = null; }
            var body = code is null ? "Sign-in callback rejected." : "Sign-in completed. Return to Kafdeck CLI.";
            var response = Encoding.ASCII.GetBytes($"HTTP/1.1 {(code is null ? "400 Bad Request" : "200 OK")}\r\nContent-Type: text/plain\r\nContent-Length: {body.Length}\r\nCache-Control: no-store\r\nReferrer-Policy: no-referrer\r\nContent-Security-Policy: default-src 'none'\r\nConnection: close\r\n\r\n{body}");
            await stream.WriteAsync(response, requestDeadline.Token);
            if (code is not null) return code;
        }
        throw new CliOidcAuthenticationException();
    }

    internal static Dictionary<string, string> ReadKeys(JsonElement root)
    {
        RequireUnique(root);
        var list = root.GetProperty("keys");
        if (list.GetArrayLength() is < 1 or > 16) throw new CliOidcAuthenticationException();
        var keys = new Dictionary<string, string>(StringComparer.Ordinal);
        foreach (var key in list.EnumerateArray())
        {
            RequireUnique(key);
            if (Text(key, "kty") != "RSA" || Text(key, "use") != "sig" || Text(key, "alg") != "RS256") continue;
            var modulus = Decode(Text(key, "n"), 512);
            var exponent = Decode(Text(key, "e"), 3);
            if (modulus.Length is < 256 or > 512 || modulus[0] == 0 || modulus.Length == 256 && modulus[0] < 128 ||
                !exponent.AsSpan().SequenceEqual(new byte[] { 1, 0, 1 })) throw new CliOidcAuthenticationException();
            using var rsa = RSA.Create();
            rsa.ImportParameters(new RSAParameters { Modulus = modulus, Exponent = exponent });
            if (!keys.TryAdd(Text(key, "kid"), rsa.ExportSubjectPublicKeyInfoPem())) throw new CliOidcAuthenticationException();
        }
        if (keys.Count is < 1 or > 8) throw new CliOidcAuthenticationException();
        return keys;
    }

    internal static JsonDocument VerifyToken(string token, Dictionary<string, string> keys, CliOidcProfile profile, string audience, string purpose)
    {
        if (token.Length > 16 * 1024) throw new CliOidcAuthenticationException();
        var parts = token.Split('.');
        if (parts.Length != 3) throw new CliOidcAuthenticationException();
        using var header = JsonDocument.Parse(Decode(parts[0], 1024));
        RequireUnique(header.RootElement);
        if (Text(header.RootElement, "alg") != "RS256" || Text(header.RootElement, "typ") is not ("JWT" or "at+jwt") ||
            header.RootElement.EnumerateObject().Any(p => p.Name is "jku" or "x5u" or "jwk" or "crit") ||
            !keys.TryGetValue(Text(header.RootElement, "kid"), out var pem)) throw new CliOidcAuthenticationException();
        using var rsa = RSA.Create(); rsa.ImportFromPem(pem);
        if (!rsa.VerifyData(Encoding.ASCII.GetBytes(parts[0] + "." + parts[1]), Decode(parts[2], 512), HashAlgorithmName.SHA256, RSASignaturePadding.Pkcs1))
            throw new CliOidcAuthenticationException();
        var claims = JsonDocument.Parse(Decode(parts[1], 8 * 1024));
        try
        {
            var root = claims.RootElement; RequireUnique(root);
            var aud = root.GetProperty("aud");
            var exactAudience = aud.ValueKind == JsonValueKind.String ? aud.GetString() == audience :
                aud.ValueKind == JsonValueKind.Array && aud.GetArrayLength() == 1 && aud[0].GetString() == audience;
            var iat = root.GetProperty("iat").GetInt64(); var exp = root.GetProperty("exp").GetInt64();
            var nbf = root.TryGetProperty("nbf", out var notBefore) ? notBefore.GetInt64() : iat;
            var now = DateTimeOffset.UtcNow.ToUnixTimeSeconds();
            if (Text(root, "iss") != profile.Issuer || Text(root, "typ") != purpose || !exactAudience ||
                string.IsNullOrWhiteSpace(Text(root, "sub")) || iat < 0 || exp <= iat || exp - iat > 900 || exp <= now ||
                iat > now + 30 || nbf < iat - 30 || nbf >= exp || nbf > now + 30 ||
                root.TryGetProperty("azp", out var azp) && azp.GetString() != profile.ClientId)
                throw new CliOidcAuthenticationException();
            if (root.TryGetProperty("client_id", out var client) && client.GetString() != profile.ClientId)
                throw new CliOidcAuthenticationException();
            return claims;
        }
        catch { claims.Dispose(); throw; }
    }

    internal static async Task<JsonDocument> GetDocumentAsync(HttpClient http, string url, CancellationToken token)
    {
        using var response = await http.GetAsync(url, HttpCompletionOption.ResponseHeadersRead, token);
        if (!response.IsSuccessStatusCode) throw new CliOidcAuthenticationException();
        return JsonDocument.Parse(await ReadResponseAsync(response, token));
    }
    internal static async Task<byte[]> ReadResponseAsync(HttpResponseMessage response, CancellationToken token)
    {
        using var deadline = CancellationTokenSource.CreateLinkedTokenSource(token); deadline.CancelAfter(TimeSpan.FromSeconds(15));
        if (response.Content.Headers.ContentLength > 64 * 1024) throw new CliOidcAuthenticationException();
        await using var stream = await response.Content.ReadAsStreamAsync(deadline.Token);
        return await ReadBoundedAsync(stream, 64 * 1024, deadline.Token);
    }
    internal static async Task<byte[]> ReadBoundedAsync(Stream stream, int limit, CancellationToken token)
    {
        using var output = new MemoryStream(); var chunk = new byte[4096]; int count;
        while ((count = await stream.ReadAsync(chunk, token)) > 0)
        { if (output.Length + count > limit) throw new CliOidcAuthenticationException(); output.Write(chunk, 0, count); }
        return output.ToArray();
    }
    private static byte[] Decode(string text, int max)
    {
        if (text.Length is < 1 || text.Length > (max + 2) / 3 * 4 || text.Any(c => !char.IsAsciiLetterOrDigit(c) && c is not '-' and not '_'))
            throw new CliOidcAuthenticationException();
        var normalized = text.Replace('-', '+').Replace('_', '/');
        var bytes = Convert.FromBase64String(normalized.PadRight((normalized.Length + 3) / 4 * 4, '='));
        if (bytes.Length > max) throw new CliOidcAuthenticationException(); return bytes;
    }
    internal static string Text(JsonElement root, string property) => root.GetProperty(property).GetString() ?? throw new CliOidcAuthenticationException();
    internal static void RequireUnique(JsonElement root)
    { if (root.ValueKind != JsonValueKind.Object || root.EnumerateObject().Select(p => p.Name).Distinct(StringComparer.Ordinal).Count() != root.EnumerateObject().Count()) throw new CliOidcAuthenticationException(); }
    private static bool FixedEquals(string actual, string expected) => actual.Length == expected.Length &&
        CryptographicOperations.FixedTimeEquals(Encoding.UTF8.GetBytes(actual), Encoding.UTF8.GetBytes(expected));
    internal static string Encode(byte[] bytes) => Convert.ToBase64String(bytes).TrimEnd('=').Replace('+', '-').Replace('/', '_');
    private static string Query(Dictionary<string, string> values) => string.Join('&', values.Select(p => Uri.EscapeDataString(p.Key) + "=" + Uri.EscapeDataString(p.Value)));
}
