using System.Net.Http.Headers;
using System.Net.Http.Json;
using System.Net;
using System.Net.Security;
using System.Security.Cryptography.X509Certificates;

namespace Kafdeck.Cli;

public sealed class KafdeckCliClient : IDisposable
{
    public const string DeploymentTokenHeader =
        "X-Kafdeck-Access-Token";
    public const int MaxTokenBytes = 8 * 1024;
    public static TimeSpan RequestTimeout { get; } =
        TimeSpan.FromSeconds(30);

    private readonly HttpClient _httpClient;
    private CliOidcSession? _oidcSession;
    private X509Certificate2? _trustedCa;

    private KafdeckCliClient(HttpClient httpClient)
    {
        _httpClient = httpClient;
    }

    public static async Task<KafdeckCliClient> CreateAsync(
        CliInvocation invocation,
        CancellationToken cancellationToken,
        Func<Uri, CancellationToken, Task>? openBrowser = null)
    {
        ArgumentNullException.ThrowIfNull(invocation);

        CliOidcSession? session = null;
        CliOidcProfile? profile = null;
        string? token = null;
        if (invocation.OidcProfileFile is not null || invocation.ServiceProfileFile is not null)
        {
            if (invocation.OidcProfileFile is not null && invocation.ServiceProfileFile is not null)
                throw new CliUsageException("Choose one human or service OIDC profile.");
            if (invocation.TokenFile is not null || !string.IsNullOrEmpty(Environment.GetEnvironmentVariable("KAFDECK_ACCESS_TOKEN")))
                throw new CliUsageException("OIDC sign-in cannot be combined with deployment access-token credentials.");
            if (invocation.BaseUri.Scheme != "https" && !(IPAddress.TryParse(invocation.BaseUri.Host, out var ip) && IPAddress.IsLoopback(ip)))
                throw new CliUsageException("OIDC bearer API requests require HTTPS, except numeric loopback laboratory origins.");
            if (invocation.ServiceProfileFile is not null)
            {
                var serviceProfile = await CliServiceOidcProfile.LoadAsync(invocation.ServiceProfileFile, cancellationToken);
                profile = serviceProfile.Trust;
                session = await CliServiceOidcAuthenticator.AcquireAsync(serviceProfile, cancellationToken);
            }
            else
            {
                profile = await CliOidcProfile.LoadAsync(invocation.OidcProfileFile!, cancellationToken);
                session = await CliOidcPkceAuthenticator.AcquireAsync(profile, openBrowser, cancellationToken);
            }
        }
        else token = await ResolveTokenAsync(invocation.TokenFile, cancellationToken);

        var handler = new SocketsHttpHandler
        {
            AllowAutoRedirect = false,
            UseCookies = false,
            UseProxy = profile is null,
            ConnectTimeout = TimeSpan.FromSeconds(10),
        };
        X509Certificate2? trustedCa;
        try { trustedCa = profile is not null ? await profile.LoadTrustedCaAsync(cancellationToken) : null; }
        catch { handler.Dispose(); session?.Dispose(); throw; }
        if (trustedCa is not null) handler.SslOptions = new SslClientAuthenticationOptions
        {
            CertificateChainPolicy = new X509ChainPolicy { TrustMode = X509ChainTrustMode.CustomRootTrust,
                RevocationMode = X509RevocationMode.NoCheck, CustomTrustStore = { trustedCa } },
        };

        var client = new HttpClient(
            handler,
            disposeHandler: true)
        {
            BaseAddress = invocation.BaseUri,
            Timeout = Timeout.InfiniteTimeSpan,
        };

        client.DefaultRequestHeaders.Accept.Add(
            new MediaTypeWithQualityHeaderValue(
                "application/json"));
        client.DefaultRequestHeaders.UserAgent.ParseAdd(
            "Kafdeck.Cli/0.7");

        if (token is not null)
        {
            client.DefaultRequestHeaders.TryAddWithoutValidation(
                DeploymentTokenHeader,
                token);
        }
        if (session is not null) client.DefaultRequestHeaders.Authorization = new AuthenticationHeaderValue("Bearer", session.AccessToken);

        return new KafdeckCliClient(client) { _oidcSession = session, _trustedCa = trustedCa };
    }

    public Task<HttpResponseMessage> GetAsync(
        string relativePath,
        CancellationToken cancellationToken)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(
            relativePath);

        return _httpClient.GetAsync(
            relativePath,
            HttpCompletionOption.ResponseHeadersRead,
            cancellationToken);
    }

    public Task<HttpResponseMessage> PostAsync(string relativePath, object? body, string? idempotencyKey, CancellationToken cancellationToken) =>
        SendMutationAsync(HttpMethod.Post, relativePath, body, idempotencyKey, cancellationToken);

    public Task<HttpResponseMessage> PutAsync(string relativePath, object? body, string? idempotencyKey, CancellationToken cancellationToken) =>
        SendMutationAsync(HttpMethod.Put, relativePath, body, idempotencyKey, cancellationToken);

    private async Task<HttpResponseMessage> SendMutationAsync(HttpMethod method, string relativePath, object? body, string? idempotencyKey, CancellationToken cancellationToken)
    {
        if (_oidcSession is null) throw new CliUsageException("Governed mutations require a memory-only OIDC session.");
        using var request = new HttpRequestMessage(method, relativePath);
        if (body is not null) request.Content = JsonContent.Create(body);
        if (idempotencyKey is not null) request.Headers.Add("Idempotency-Key", idempotencyKey);
        return await _httpClient.SendAsync(request, HttpCompletionOption.ResponseHeadersRead, cancellationToken);
    }

    public void Dispose()
    {
        _httpClient.Dispose();
        _oidcSession?.Dispose();
        _trustedCa?.Dispose();
    }

    internal static async Task<string?> ResolveTokenAsync(
        string? tokenFile,
        CancellationToken cancellationToken)
    {
        var environmentToken =
            Environment.GetEnvironmentVariable(
                "KAFDECK_ACCESS_TOKEN");

        if (tokenFile is not null &&
            string.IsNullOrWhiteSpace(
                tokenFile))
        {
            throw new CliUsageException(
                "The --token-file path cannot be blank.");
        }

        if (!string.IsNullOrEmpty(environmentToken) &&
            tokenFile is not null)
        {
            throw new CliUsageException(
                "Configure only one access-token source: KAFDECK_ACCESS_TOKEN or --token-file.");
        }

        string? token = null;
        if (!string.IsNullOrEmpty(environmentToken))
        {
            token = environmentToken;
        }
        else if (tokenFile is not null)
        {
            var fullPath =
                Path.GetFullPath(tokenFile);
            token = await ReadBoundedTokenFileAsync(
                fullPath,
                cancellationToken);
        }

        if (token is null)
        {
            return null;
        }

        token = token.Trim();
        if (token.Length == 0)
        {
            throw new CliUsageException(
                "The configured access token is empty.");
        }

        if (System.Text.Encoding.UTF8.GetByteCount(token) >
            MaxTokenBytes)
        {
            throw new CliUsageException(
                $"The configured access token exceeds {MaxTokenBytes} UTF-8 bytes.");
        }

        // Never pass control characters, whitespace inside the token or
        // non-ASCII material to TryAddWithoutValidation. A token file may
        // end in a newline, but embedded CR/LF must not become HTTP syntax.
        if (token.Any(ch => ch is < '!' or > '~'))
        {
            throw new CliUsageException(
                "The configured access token contains invalid header characters.");
        }

        return token;
    }

    private static async Task<string> ReadBoundedTokenFileAsync(
        string fullPath,
        CancellationToken cancellationToken)
    {
        await using var stream = new FileStream(
            fullPath,
            FileMode.Open,
            FileAccess.Read,
            FileShare.Read,
            bufferSize: 4096,
            options: FileOptions.Asynchronous | FileOptions.SequentialScan);

        // Bound actual bytes read, not only the file's advertised size:
        // a file can grow between opening and consuming it.
        // A UTF-8 BOM (3 bytes) and one terminal CRLF (2 bytes) were
        // historically accepted even for an exact-limit token. Keep those
        // framing bytes without permitting an unbounded file read; the
        // normalized credential is still capped at MaxTokenBytes below.
        const int maxFramingBytes = 5;
        var bytes = new byte[MaxTokenBytes + maxFramingBytes + 1];
        try
        {
            var count = 0;
            while (count < bytes.Length)
            {
                var read = await stream.ReadAsync(
                    bytes.AsMemory(count),
                    cancellationToken);
                if (read == 0)
                {
                    break;
                }

                count += read;
            }

            if (count > MaxTokenBytes + maxFramingBytes)
            {
                throw new CliUsageException(
                    $"The configured access token file exceeds the bounded {MaxTokenBytes}-byte token budget and optional framing.");
            }

            // Match the former ReadAllTextAsync behavior for an optional
            // UTF-8 BOM, but reject malformed UTF-8 rather than substituting
            // replacement characters in an authentication credential.
            var start = count >= 3 &&
                bytes[0] == 0xEF &&
                bytes[1] == 0xBB &&
                bytes[2] == 0xBF
                    ? 3
                    : 0;

            try
            {
                return new System.Text.UTF8Encoding(
                    encoderShouldEmitUTF8Identifier: false,
                    throwOnInvalidBytes: true)
                    .GetString(bytes, start, count - start);
            }
            catch (System.Text.DecoderFallbackException)
            {
                throw new CliUsageException(
                    "The configured access token file must contain valid UTF-8.");
            }
        }
        finally
        {
            Array.Clear(bytes);
        }
    }
}
