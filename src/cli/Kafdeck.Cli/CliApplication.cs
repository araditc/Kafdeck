using System.Text.Json;

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

            using var response = await client.GetAsync(
                CliRouteBuilder.Build(invocation),
                requestToken);

            // A non-success response is untrusted remote text and may echo
            // Authorization credentials, connection strings or protected
            // provider details. Never download or print its body to stderr.
            // Status-only errors retain stable CLI script exit categories.
            if (!response.IsSuccessStatusCode)
            {
                await error.WriteLineAsync(
                    $"Kafdeck returned HTTP {(int)response.StatusCode}.");
                return CategorizeHttpFailure(response.StatusCode);
            }

            string body;
            try
            {
                body = await CliResponseBodyReader.ReadAsync(
                    response.Content, requestToken);
            }
            catch (CliResponseTooLargeException)
            {
                await error.WriteLineAsync(
                    $"Kafdeck CLI refused an API response exceeding {CliResponseBodyReader.MaxBodyBytes} bytes.");
                return RemoteError;
            }

            await WriteJsonAsync(output, body, requestToken);
            return Success;
        }
        catch (CliUsageException exception)
        {
            await error.WriteLineAsync(exception.Message);
            await error.WriteLineAsync(CliParser.Usage);
            return UsageError;
        }
        catch (JsonException exception)
        {
            await error.WriteLineAsync(
                $"Kafdeck CLI received invalid JSON from the governed API: {exception.Message}");
            return RemoteError;
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

    public static int CategorizeHttpFailure(System.Net.HttpStatusCode status) =>
        status is System.Net.HttpStatusCode.Unauthorized or
            System.Net.HttpStatusCode.Forbidden
            ? AuthenticationOrAuthorizationError
            : RemoteError;

    private static async Task WriteJsonAsync(
        TextWriter output,
        string body,
        CancellationToken cancellationToken)
    {
        if (string.IsNullOrWhiteSpace(body))
        {
            await output.WriteLineAsync("{}");
            return;
        }

        using var document =
            JsonDocument.Parse(body);
        cancellationToken.ThrowIfCancellationRequested();

        await output.WriteLineAsync(
            document.RootElement.GetRawText());
    }
}

public enum CliCommand
{
    None = 0,
    SystemInfo = 1,
    ClustersList = 2,
    ClusterGet = 3,
    TopicsList = 4,
    TopicGet = 5,
    ConsumerGroupsList = 6,
    ConsumerGroupGet = 7,
    ConsumerGroupLag = 8,
    SchemaSubjectsList = 9,
    SchemaVersionsList = 10,
    ConsumerGroupDiagnostics = 11,
    SchemaVersionGet = 12,
    SchemaCompatibilityGet = 13,
    SchemaDiff = 14,
}

public enum CliOutputFormat
{
    Json = 1,
}

public sealed record CliInvocation(
    Uri BaseUri,
    string? TokenFile,
    CliCommand Command,
    string? ResourceId,
    bool ShowHelp,
    string? SecondaryResourceId = null,
    string? Search = null,
    string? Cursor = null,
    int? Limit = null,
    CliOutputFormat Output = CliOutputFormat.Json,
    int? Version = null,
    int? RightVersion = null);

public static class CliRouteBuilder
{
    public static string Build(
        CliInvocation invocation)
    {
        ArgumentNullException.ThrowIfNull(
            invocation);

        return invocation.Command switch
        {
            CliCommand.SystemInfo =>
                "/api/v1/system/info",
            CliCommand.ClustersList =>
                "/api/v1/clusters",
            CliCommand.ClusterGet =>
                $"/api/v1/clusters/{RequirePrimary(invocation)}",
            CliCommand.TopicsList =>
                BuildTopicsList(
                    RequirePrimary(invocation),
                    invocation),
            CliCommand.TopicGet =>
                $"/api/v1/clusters/{RequirePrimary(invocation)}/topics/{RequireSecondary(invocation)}",
            CliCommand.ConsumerGroupsList =>
                $"/api/v1/clusters/{RequirePrimary(invocation)}/consumer-groups",
            CliCommand.ConsumerGroupGet =>
                $"/api/v1/clusters/{RequirePrimary(invocation)}/consumer-groups/{RequireSecondary(invocation)}",
            CliCommand.ConsumerGroupLag =>
                $"/api/v1/clusters/{RequirePrimary(invocation)}/consumer-groups/{RequireSecondary(invocation)}/lag",
            CliCommand.ConsumerGroupDiagnostics =>
                $"/api/v1/clusters/{RequirePrimary(invocation)}/consumer-groups/{RequireSecondary(invocation)}/diagnostics",
            CliCommand.SchemaSubjectsList =>
                $"/api/v1/clusters/{RequirePrimary(invocation)}/schemas/subjects",
            CliCommand.SchemaVersionsList =>
                $"/api/v1/clusters/{RequirePrimary(invocation)}/schemas/subjects/{RequireSecondary(invocation)}/versions",
            CliCommand.SchemaVersionGet =>
                $"/api/v1/clusters/{RequirePrimary(invocation)}/schemas/subjects/{RequireSecondary(invocation)}/versions/{RequireVersion(invocation)}",
            CliCommand.SchemaCompatibilityGet =>
                $"/api/v1/clusters/{RequirePrimary(invocation)}/schemas/subjects/{RequireSecondary(invocation)}/compatibility",
            CliCommand.SchemaDiff =>
                $"/api/v1/clusters/{RequirePrimary(invocation)}/schemas/subjects/{RequireSecondary(invocation)}/diff?leftVersion={RequireVersion(invocation)}&rightVersion={RequireRightVersion(invocation)}",
            _ => throw new InvalidOperationException(
                "CLI invocation contains an unsupported command."),
        };
    }

    private static string BuildTopicsList(
        string cluster,
        CliInvocation invocation)
    {
        var query =
            new List<string>(3);

        if (invocation.Search is not null)
        {
            query.Add(
                $"q={Uri.EscapeDataString(invocation.Search)}");
        }

        if (invocation.Cursor is not null)
        {
            query.Add(
                $"cursor={Uri.EscapeDataString(invocation.Cursor)}");
        }

        if (invocation.Limit is not null)
        {
            query.Add(
                $"pageSize={invocation.Limit.Value}");
        }

        var path =
            $"/api/v1/clusters/{cluster}/topics";

        return query.Count == 0
            ? path
            : $"{path}?{string.Join("&", query)}";
    }

    private static string RequirePrimary(
        CliInvocation invocation) =>
        EscapeSegment(
            invocation.ResourceId,
            "primary");

    private static string RequireSecondary(
        CliInvocation invocation) =>
        EscapeSegment(
            invocation.SecondaryResourceId,
            "secondary");

    private static int RequireVersion(
        CliInvocation invocation) =>
        invocation.Version is > 0
            ? invocation.Version.Value
            : throw new InvalidOperationException(
                "CLI invocation is missing a valid schema version.");

    private static int RequireRightVersion(
        CliInvocation invocation) =>
        invocation.RightVersion is > 0
            ? invocation.RightVersion.Value
            : throw new InvalidOperationException(
                "CLI invocation is missing a valid right schema version.");

    private static string EscapeSegment(
        string? value,
        string identityKind)
    {
        if (string.IsNullOrEmpty(value))
        {
            throw new InvalidOperationException(
                $"CLI invocation is missing its {identityKind} resource identity.");
        }

        if (value is "." or "..")
        {
            throw new InvalidOperationException(
                $"CLI invocation contains an unsafe {identityKind} resource identity.");
        }

        return Uri.EscapeDataString(
            value);
    }
}

public sealed class CliUsageException : Exception
{
    public CliUsageException(string message)
        : base(message)
    {
    }
}

public static class CliParser
{
    public const int TopicMaximumPageSize = 200;
    public const int MaxCursorLength = 4096;
    public const int MaxSearchLength = 256;

    private static Uri DefaultBaseUri { get; } =
        new(
            "http://127.0.0.1:8080/",
            UriKind.Absolute);

    public const string Usage =
        """
        Kafdeck CLI

        Usage:
          kafdeck [global-options] system info
          kafdeck [global-options] clusters list
          kafdeck [global-options] clusters get <cluster-id>
          kafdeck [global-options] topics list <cluster-id> [--search <text>] [--cursor <opaque>] [--limit <1..200>]
          kafdeck [global-options] topics get <cluster-id> <topic-name>
          kafdeck [global-options] consumer-groups list <cluster-id>
          kafdeck [global-options] consumer-groups get <cluster-id> <group-id>
          kafdeck [global-options] consumer-groups lag <cluster-id> <group-id>
          kafdeck [global-options] consumer-groups diagnostics <cluster-id> <group-id>
          kafdeck [global-options] schemas subjects <cluster-id>
          kafdeck [global-options] schemas versions <cluster-id> <subject>
          kafdeck [global-options] schemas version <cluster-id> <subject> <version>
          kafdeck [global-options] schemas compatibility <cluster-id> <subject>
          kafdeck [global-options] schemas diff <cluster-id> <subject> <left-version> <right-version>
          kafdeck --help

        Global options:
          --url <base-url>
          --token-file <path>
          --output json

        Environment:
          KAFDECK_URL
          KAFDECK_ACCESS_TOKEN

        Security:
          Access tokens are intentionally not accepted as command-line arguments.
          Use KAFDECK_ACCESS_TOKEN or --token-file.
          The CLI exposes only typed governed API routes; there is no arbitrary HTTP path passthrough.
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
        string? search = null;
        string? cursor = null;
        int? limit = null;
        var output =
            CliOutputFormat.Json;
        var positionals =
            new List<string>();

        for (var index = 0;
             index < args.Count;
             index++)
        {
            var value =
                args[index];

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
                case "--output":
                {
                    var requested =
                        RequireValue(
                            args,
                            ref index,
                            "--output");
                    if (!string.Equals(
                            requested,
                            "json",
                            StringComparison.OrdinalIgnoreCase))
                    {
                        throw new CliUsageException(
                            "The only admitted machine output format in this slice is json.");
                    }

                    output =
                        CliOutputFormat.Json;
                    break;
                }
                case "--search":
                    search = ValidateBoundedOption(
                        RequireValue(
                            args,
                            ref index,
                            "--search"),
                        "--search",
                        MaxSearchLength);
                    break;
                case "--cursor":
                    cursor = ValidateBoundedOption(
                        RequireValue(
                            args,
                            ref index,
                            "--cursor"),
                        "--cursor",
                        MaxCursorLength);
                    break;
                case "--limit":
                {
                    var text =
                        RequireValue(
                            args,
                            ref index,
                            "--limit");
                    if (!int.TryParse(
                            text,
                            System.Globalization.NumberStyles.None,
                            System.Globalization.CultureInfo.InvariantCulture,
                            out var parsed) ||
                        parsed is < 1 or >
                            TopicMaximumPageSize)
                    {
                        throw new CliUsageException(
                            $"--limit must be between 1 and {TopicMaximumPageSize}.");
                    }

                    limit =
                        parsed;
                    break;
                }
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

        var baseUri =
            ResolveBaseUri(url);

        var invocation =
            ParseCommand(
                baseUri,
                tokenFile,
                positionals,
                search,
                cursor,
                limit,
                output);

        if (invocation.Command !=
                CliCommand.TopicsList &&
            (search is not null ||
             cursor is not null ||
             limit is not null))
        {
            throw new CliUsageException(
                "--search, --cursor and --limit are admitted only for 'topics list'.");
        }

        return invocation;
    }

    private static CliInvocation ParseCommand(
        Uri baseUri,
        string? tokenFile,
        IReadOnlyList<string> positionals,
        string? search,
        string? cursor,
        int? limit,
        CliOutputFormat output)
    {
        if (positionals.SequenceEqual(
                new[] { "system", "info" },
                StringComparer.Ordinal))
        {
            return Create(
                baseUri,
                tokenFile,
                CliCommand.SystemInfo,
                output: output);
        }

        if (positionals.SequenceEqual(
                new[] { "clusters", "list" },
                StringComparer.Ordinal))
        {
            return Create(
                baseUri,
                tokenFile,
                CliCommand.ClustersList,
                output: output);
        }

        if (positionals.Count == 3 &&
            positionals[0] == "clusters" &&
            positionals[1] == "get")
        {
            return CreateSingle(
                baseUri,
                tokenFile,
                CliCommand.ClusterGet,
                positionals[2],
                "cluster ID",
                output);
        }

        if (positionals.Count == 3 &&
            positionals[0] == "topics" &&
            positionals[1] == "list")
        {
            return CreateSingle(
                baseUri,
                tokenFile,
                CliCommand.TopicsList,
                positionals[2],
                "cluster ID",
                output,
                search,
                cursor,
                limit);
        }

        if (positionals.Count == 4 &&
            positionals[0] == "topics" &&
            positionals[1] == "get")
        {
            return CreateDouble(
                baseUri,
                tokenFile,
                CliCommand.TopicGet,
                positionals[2],
                "cluster ID",
                positionals[3],
                "topic name",
                output);
        }

        if (positionals.Count == 3 &&
            positionals[0] == "consumer-groups" &&
            positionals[1] == "list")
        {
            return CreateSingle(
                baseUri,
                tokenFile,
                CliCommand.ConsumerGroupsList,
                positionals[2],
                "cluster ID",
                output);
        }

        if (positionals.Count == 4 &&
            positionals[0] == "consumer-groups" &&
            positionals[1] is "get" or "lag" or "diagnostics")
        {
            var command =
                positionals[1] switch
                {
                    "get" => CliCommand.ConsumerGroupGet,
                    "lag" => CliCommand.ConsumerGroupLag,
                    _ => CliCommand.ConsumerGroupDiagnostics,
                };

            return CreateDouble(
                baseUri,
                tokenFile,
                command,
                positionals[2],
                "cluster ID",
                positionals[3],
                "consumer group ID",
                output);
        }

        if (positionals.Count == 3 &&
            positionals[0] == "schemas" &&
            positionals[1] == "subjects")
        {
            return CreateSingle(
                baseUri,
                tokenFile,
                CliCommand.SchemaSubjectsList,
                positionals[2],
                "cluster ID",
                output);
        }

        if (positionals.Count == 4 &&
            positionals[0] == "schemas" &&
            positionals[1] == "versions")
        {
            return CreateDouble(
                baseUri,
                tokenFile,
                CliCommand.SchemaVersionsList,
                positionals[2],
                "cluster ID",
                positionals[3],
                "schema subject",
                output);
        }

        if (positionals.Count == 5 &&
            positionals[0] == "schemas" &&
            positionals[1] == "version")
        {
            return CreateSchemaVersion(
                baseUri,
                tokenFile,
                CliCommand.SchemaVersionGet,
                positionals[2],
                positionals[3],
                positionals[4],
                output);
        }

        if (positionals.Count == 4 &&
            positionals[0] == "schemas" &&
            positionals[1] == "compatibility")
        {
            return CreateDouble(
                baseUri,
                tokenFile,
                CliCommand.SchemaCompatibilityGet,
                positionals[2],
                "cluster ID",
                positionals[3],
                "schema subject",
                output);
        }

        if (positionals.Count == 6 &&
            positionals[0] == "schemas" &&
            positionals[1] == "diff")
        {
            return CreateSchemaDiff(
                baseUri,
                tokenFile,
                positionals[2],
                positionals[3],
                positionals[4],
                positionals[5],
                output);
        }

        throw new CliUsageException(
            "Unknown, incomplete, or unavailable command.");
    }

    private static CliInvocation CreateSingle(
        Uri baseUri,
        string? tokenFile,
        CliCommand command,
        string resourceId,
        string field,
        CliOutputFormat output,
        string? search = null,
        string? cursor = null,
        int? limit = null)
    {
        ValidateIdentifier(
            resourceId,
            field);

        return Create(
            baseUri,
            tokenFile,
            command,
            resourceId,
            output: output,
            search: search,
            cursor: cursor,
            limit: limit);
    }

    private static CliInvocation CreateDouble(
        Uri baseUri,
        string? tokenFile,
        CliCommand command,
        string resourceId,
        string resourceField,
        string secondaryResourceId,
        string secondaryField,
        CliOutputFormat output)
    {
        ValidateIdentifier(
            resourceId,
            resourceField);
        ValidateIdentifier(
            secondaryResourceId,
            secondaryField,
            maxLength: 512);

        return Create(
            baseUri,
            tokenFile,
            command,
            resourceId,
            secondaryResourceId,
            output: output);
    }

    private static CliInvocation CreateSchemaVersion(
        Uri baseUri,
        string? tokenFile,
        CliCommand command,
        string clusterId,
        string subject,
        string versionText,
        CliOutputFormat output)
    {
        ValidateIdentifier(
            clusterId,
            "cluster ID");
        ValidateIdentifier(
            subject,
            "schema subject",
            maxLength: 512);

        return Create(
            baseUri,
            tokenFile,
            command,
            clusterId,
            subject,
            output: output,
            version: ParsePositiveVersion(
                versionText,
                "schema version"));
    }

    private static CliInvocation CreateSchemaDiff(
        Uri baseUri,
        string? tokenFile,
        string clusterId,
        string subject,
        string leftVersionText,
        string rightVersionText,
        CliOutputFormat output)
    {
        ValidateIdentifier(
            clusterId,
            "cluster ID");
        ValidateIdentifier(
            subject,
            "schema subject",
            maxLength: 512);

        return Create(
            baseUri,
            tokenFile,
            CliCommand.SchemaDiff,
            clusterId,
            subject,
            output: output,
            version: ParsePositiveVersion(
                leftVersionText,
                "left schema version"),
            rightVersion: ParsePositiveVersion(
                rightVersionText,
                "right schema version"));
    }

    private static int ParsePositiveVersion(
        string value,
        string field)
    {
        if (!int.TryParse(
                value,
                System.Globalization.NumberStyles.None,
                System.Globalization.CultureInfo.InvariantCulture,
                out var parsed) ||
            parsed < 1)
        {
            throw new CliUsageException(
                $"{field} must be a positive integer.");
        }

        return parsed;
    }

    private static CliInvocation Create(
        Uri baseUri,
        string? tokenFile,
        CliCommand command,
        string? resourceId = null,
        string? secondaryResourceId = null,
        CliOutputFormat output = CliOutputFormat.Json,
        string? search = null,
        string? cursor = null,
        int? limit = null,
        int? version = null,
        int? rightVersion = null) =>
        new(
            baseUri,
            tokenFile,
            command,
            resourceId,
            ShowHelp: false,
            secondaryResourceId,
            search,
            cursor,
            limit,
            output,
            version,
            rightVersion);

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

    private static string ValidateBoundedOption(
        string value,
        string option,
        int maxLength)
    {
        if (string.IsNullOrWhiteSpace(value) ||
            value.Length > maxLength ||
            value.Any(char.IsControl))
        {
            throw new CliUsageException(
                $"{option} must be non-empty, at most {maxLength} characters, and contain no control characters.");
        }

        return value;
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
        string field,
        int maxLength = 256)
    {
        if (string.IsNullOrWhiteSpace(value) ||
            value is "." or ".." ||
            value.Contains('/', StringComparison.Ordinal) ||
            value.Length > maxLength ||
            !string.Equals(
                value,
                value.Trim(),
                StringComparison.Ordinal) ||
            value.Any(char.IsControl))
        {
            throw new CliUsageException(
                $"{field} must be exact, non-empty, at most {maxLength} characters, and contain no control characters.");
        }
    }
}
