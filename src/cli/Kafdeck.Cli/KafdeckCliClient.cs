using System.Net.Http.Headers;

namespace Kafdeck.Cli;

public sealed class KafdeckCliClient : IDisposable
{
    public const string DeploymentTokenHeader =
        "X-Kafdeck-Access-Token";
    public const int MaxTokenBytes = 8 * 1024;
    public static TimeSpan RequestTimeout { get; } =
        TimeSpan.FromSeconds(30);

    private readonly HttpClient _httpClient;

    private KafdeckCliClient(HttpClient httpClient)
    {
        _httpClient = httpClient;
    }

    public static async Task<KafdeckCliClient> CreateAsync(
        CliInvocation invocation,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(invocation);

        var token = await ResolveTokenAsync(
            invocation.TokenFile,
            cancellationToken);

        var handler = new SocketsHttpHandler
        {
            AllowAutoRedirect = false,
            ConnectTimeout = TimeSpan.FromSeconds(10),
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

        return new KafdeckCliClient(client);
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

    public void Dispose() =>
        _httpClient.Dispose();

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
            token = await File.ReadAllTextAsync(
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

        return token;
    }
}
