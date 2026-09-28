namespace Kafdeck.Cli;

public static class CliApplication
{
    public const int Success = 0;
    public const int UsageError = 2;
    public const int AuthenticationOrAuthorizationError = 3;
    public const int RemoteError = 4;
    public const int TransportError = 5;

    public static async Task<int> RunAsync(
        IReadOnlyList<string> args,
        TextWriter output,
        TextWriter error,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(args);
        ArgumentNullException.ThrowIfNull(output);
        ArgumentNullException.ThrowIfNull(error);

        CliInvocation invocation;
        try
        {
            invocation = CliParser.Parse(args);
        }
        catch (CliUsageException exception)
        {
            await error.WriteLineAsync(exception.Message);
            await error.WriteLineAsync(CliParser.Usage);
            return UsageError;
        }

        if (invocation.ShowHelp)
        {
            await output.WriteLineAsync(CliParser.Usage);
            return Success;
        }

        try
        {
            using var deadline =
                CancellationTokenSource.CreateLinkedTokenSource(
                    cancellationToken);
            deadline.CancelAfter(
                KafdeckCliClient.RequestTimeout);
            var requestToken =
                deadline.Token;

            using var client = await KafdeckCliClient.CreateAsync(
                invocation,
                requestToken);

            using var response = await ExecuteAsync(
                invocation,
                client,
                requestToken);

            var body = await response.Content
                .ReadAsStringAsync(requestToken);

            if (response.IsSuccessStatusCode)
            {
                await output.WriteLineAsync(
                    string.IsNullOrWhiteSpace(body)
                        ? "{}"
                        : body);
                return Success;
            }

            await error.WriteLineAsync(
                string.IsNullOrWhiteSpace(body)
                    ? $"Kafdeck returned HTTP {(int)response.StatusCode}."
                    : body);

            return response.StatusCode is
                System.Net.HttpStatusCode.Unauthorized or
                System.Net.HttpStatusCode.Forbidden
                    ? AuthenticationOrAuthorizationError
                    : RemoteError;
        }
        catch (CliUsageException exception)
        {
            await error.WriteLineAsync(exception.Message);
            await error.WriteLineAsync(CliParser.Usage);
            return UsageError;
        }
        catch (OperationCanceledException)
            when (cancellationToken.IsCancellationRequested)
        {
            await error.WriteLineAsync(
                "Kafdeck CLI request was cancelled.");
            return TransportError;
        }
        catch (OperationCanceledException)
        {
            await error.WriteLineAsync(
                $"Kafdeck CLI request timed out after {KafdeckCliClient.RequestTimeout.TotalSeconds:0} seconds.");
            return TransportError;
        }
        catch (HttpRequestException exception)
        {
            await error.WriteLineAsync(
                $"Kafdeck CLI transport error: {exception.Message}");
            return TransportError;
        }
        catch (UnauthorizedAccessException exception)
        {
            await error.WriteLineAsync(
                $"Kafdeck CLI credential/configuration I/O error: {exception.Message}");
            return TransportError;
        }
        catch (IOException exception)
        {
            await error.WriteLineAsync(
                $"Kafdeck CLI credential/configuration I/O error: {exception.Message}");
            return TransportError;
        }
    }

    private static Task<HttpResponseMessage> ExecuteAsync(
        CliInvocation invocation,
        KafdeckCliClient client,
        CancellationToken cancellationToken) =>
        invocation.Command switch
        {
            CliCommand.SystemInfo =>
                client.GetAsync(
                    "/api/v1/system/info",
                    cancellationToken),
            CliCommand.ClustersList =>
                client.GetAsync(
                    "/api/v1/clusters",
                    cancellationToken),
            CliCommand.ClusterGet =>
                client.GetAsync(
                    $"/api/v1/clusters/{Uri.EscapeDataString(invocation.ResourceId!)}",
                    cancellationToken),
            _ => throw new InvalidOperationException(
                "CLI invocation contains an unsupported command."),
        };
}

public enum CliCommand
{
    None = 0,
    SystemInfo = 1,
    ClustersList = 2,
    ClusterGet = 3,
}

public sealed record CliInvocation(
    Uri BaseUri,
    string? TokenFile,
    CliCommand Command,
    string? ResourceId,
    bool ShowHelp);

public sealed class CliUsageException : Exception
{
    public CliUsageException(string message)
        : base(message)
    {
    }
}

public static class CliParser
{
    private static Uri DefaultBaseUri { get; } =
        new(
            "http://127.0.0.1:8080/",
            UriKind.Absolute);

    public const string Usage =
        """
        Kafdeck CLI

        Usage:
          kafdeck [--url <base-url>] [--token-file <path>] system info
          kafdeck [--url <base-url>] [--token-file <path>] clusters list
          kafdeck [--url <base-url>] [--token-file <path>] clusters get <cluster-id>
          kafdeck --help

        Environment:
          KAFDECK_URL
          KAFDECK_ACCESS_TOKEN

        Security:
          Access tokens are intentionally not accepted as command-line arguments.
          Use KAFDECK_ACCESS_TOKEN or --token-file.
          Mutation status and approval commands are withheld until the CLI has a
          governed non-browser OIDC authentication flow compatible with mutation mode.
        """;

    public static CliInvocation Parse(
        IReadOnlyList<string> args)
    {
        ArgumentNullException.ThrowIfNull(args);

        if (args.Count == 0 ||
            args.Any(value =>
                string.Equals(
                    value,
                    "--help",
                    StringComparison.Ordinal)))
        {
            return new CliInvocation(
                DefaultBaseUri,
                null,
                CliCommand.None,
                null,
                ShowHelp: true);
        }

        string? url = null;
        string? tokenFile = null;
        var positionals = new List<string>();

        for (var index = 0; index < args.Count; index++)
        {
            var value = args[index];

            switch (value)
            {
                case "--url":
                    url = RequireValue(
                        args,
                        ref index,
                        "--url");
                    break;
                case "--token-file":
                    tokenFile = RequireValue(
                        args,
                        ref index,
                        "--token-file");
                    if (string.IsNullOrWhiteSpace(
                            tokenFile))
                    {
                        throw new CliUsageException(
                            "--token-file requires a non-empty path.");
                    }

                    break;
                case "--token":
                    throw new CliUsageException(
                        "Access tokens are not accepted on the command line. Use KAFDECK_ACCESS_TOKEN or --token-file.");
                default:
                    if (value.StartsWith(
                            "--",
                            StringComparison.Ordinal))
                    {
                        throw new CliUsageException(
                            $"Unknown option '{value}'.");
                    }

                    positionals.Add(value);
                    break;
            }
        }

        var baseUri = ResolveBaseUri(url);

        if (positionals.SequenceEqual(
                new[] { "system", "info" },
                StringComparer.Ordinal))
        {
            return Create(
                baseUri,
                tokenFile,
                CliCommand.SystemInfo);
        }

        if (positionals.SequenceEqual(
                new[] { "clusters", "list" },
                StringComparer.Ordinal))
        {
            return Create(
                baseUri,
                tokenFile,
                CliCommand.ClustersList);
        }

        if (positionals.Count == 3 &&
            positionals[0] == "clusters" &&
            positionals[1] == "get")
        {
            ValidateIdentifier(
                positionals[2],
                "cluster ID");
            return Create(
                baseUri,
                tokenFile,
                CliCommand.ClusterGet,
                resourceId: positionals[2]);
        }

        throw new CliUsageException(
            "Unknown, incomplete, or unavailable command.");
    }

    private static CliInvocation Create(
        Uri baseUri,
        string? tokenFile,
        CliCommand command,
        string? resourceId = null) =>
        new(
            baseUri,
            tokenFile,
            command,
            resourceId,
            ShowHelp: false);

    private static string RequireValue(
        IReadOnlyList<string> args,
        ref int index,
        string option)
    {
        if (index + 1 >= args.Count)
        {
            throw new CliUsageException(
                $"{option} requires a value.");
        }

        index++;
        return args[index];
    }

    private static Uri ResolveBaseUri(
        string? commandLineValue)
    {
        var value =
            commandLineValue ??
            Environment.GetEnvironmentVariable(
                "KAFDECK_URL") ??
            "http://127.0.0.1:8080";

        if (!Uri.TryCreate(
                value,
                UriKind.Absolute,
                out var uri) ||
            (uri.Scheme != Uri.UriSchemeHttp &&
             uri.Scheme != Uri.UriSchemeHttps) ||
            !string.IsNullOrEmpty(uri.UserInfo) ||
            !string.IsNullOrEmpty(uri.Query) ||
            !string.IsNullOrEmpty(uri.Fragment) ||
            (uri.AbsolutePath != "/" &&
             !string.IsNullOrEmpty(uri.AbsolutePath)))
        {
            throw new CliUsageException(
                "Kafdeck base URL must be an absolute HTTP/HTTPS origin without credentials, query, fragment or path.");
        }

        return new Uri(
            uri.GetLeftPart(UriPartial.Authority) + "/",
            UriKind.Absolute);
    }

    private static void ValidateIdentifier(
        string value,
        string field)
    {
        if (string.IsNullOrWhiteSpace(value) ||
            value.Length > 256 ||
            value.Any(char.IsControl))
        {
            throw new CliUsageException(
                $"{field} must be non-empty, at most 256 characters, and contain no control characters.");
        }
    }
}
