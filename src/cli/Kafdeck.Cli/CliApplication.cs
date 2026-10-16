using System.Text.Json;

namespace Kafdeck.Cli;

public static class CliApplication
{
    public const int Success = 0;
    public const int UsageError = 2;
    public const int AuthenticationOrAuthorizationError = 3;
    public const int RemoteError = 4;
    public const int TransportError = 5;
    public const int PendingApproval = 6;
    public const int UnknownEffect = 7;
    public const int Conflict = 8;
    public const int Expired = 9;
    public const int PartialOrUnverified = 10;

    public static async Task<int> RunAsync(
        IReadOnlyList<string> args,
        TextWriter output,
        TextWriter error,
        CancellationToken cancellationToken,
        Func<Uri, CancellationToken, Task>? openBrowser = null,
        Action<CliOidcFailureStage>? oidcFailureDiagnostic = null)
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
                invocation.OidcProfileFile is not null ? CliOidcPkceAuthenticator.FlowTimeout + KafdeckCliClient.RequestTimeout :
                    invocation.ServiceProfileFile is not null ? CliServiceOidcAuthenticator.FlowTimeout + KafdeckCliClient.RequestTimeout :
                    KafdeckCliClient.RequestTimeout);
            var requestToken =
                deadline.Token;

            using var client = await KafdeckCliClient.CreateAsync(
                invocation,
                requestToken,
                openBrowser);
            deadline.CancelAfter(KafdeckCliClient.RequestTimeout);

            using var response = invocation.Command switch
            {
                CliCommand.TopicCreatePreview => await client.PostAsync(CliRouteBuilder.Build(invocation),
                    new { partitionCount = invocation.PartitionCount, replicationFactor = invocation.ReplicationFactor,
                        configurations = invocation.TopicConfigurations ?? new Dictionary<string, string?>() }, invocation.IdempotencyKey, requestToken),
                CliCommand.TopicAlterPreview => await client.PostAsync(CliRouteBuilder.Build(invocation),
                    new { changes = invocation.TopicConfigurations }, invocation.IdempotencyKey, requestToken),
                CliCommand.TopicIncreasePartitionsPreview => await client.PostAsync(CliRouteBuilder.Build(invocation),
                    new { newPartitionCount = invocation.PartitionCount }, invocation.IdempotencyKey, requestToken),
                CliCommand.OperationConfirm => await client.PostAsync(CliRouteBuilder.Build(invocation),
                    new { previewHash = invocation.PreviewHash, typedTargetChallenge = invocation.TypedTargetChallenge }, null, requestToken),
                CliCommand.TopicDeletePreview => await client.PostAsync(CliRouteBuilder.Build(invocation), null, invocation.IdempotencyKey, requestToken),
                CliCommand.OperationApprove => await client.PostAsync(CliRouteBuilder.Build(invocation), new { previewHash = invocation.PreviewHash }, null, requestToken),
                CliCommand.OperationCancel or CliCommand.OperationReject => await client.PostAsync(CliRouteBuilder.Build(invocation),
                    new { previewHash = invocation.PreviewHash }, null, requestToken),
                CliCommand.OperationExecute => await client.PostAsync(CliRouteBuilder.Build(invocation), null, null, requestToken),
                _ => await client.GetAsync(CliRouteBuilder.Build(invocation), requestToken),
            };

            // A non-success response is untrusted remote text and may echo
            // Authorization credentials, connection strings or protected
            // provider details. Never download or print its body to stderr.
            // Status-only errors retain stable CLI script exit categories.
            if (!response.IsSuccessStatusCode)
            {
                await error.WriteLineAsync(
                    $"Kafdeck returned HTTP {(int)response.StatusCode}.");
                if (IsMutation(invocation.Command) && response.StatusCode == System.Net.HttpStatusCode.Conflict)
                    return Conflict;
                if (IsMutation(invocation.Command) && response.StatusCode == System.Net.HttpStatusCode.Gone)
                    return Expired;
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
            return IsMutation(invocation.Command) ? MutationOutcomeCategory(body, invocation.Command) : Success;
        }
        catch (CliUsageException exception)
        {
            await error.WriteLineAsync(exception.Message);
            await error.WriteLineAsync(CliParser.Usage);
            return UsageError;
        }
        catch (CliOidcAuthenticationException exception)
        {
            oidcFailureDiagnostic?.Invoke(exception.Stage);
            await error.WriteLineAsync("Kafdeck CLI OIDC sign-in was rejected.");
            return AuthenticationOrAuthorizationError;
        }
        catch (JsonException)
        {
            await error.WriteLineAsync(
                "Kafdeck CLI received invalid JSON from the governed API or public profile.");
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

    public static bool IsMutation(CliCommand command) => command is CliCommand.TopicCreatePreview or CliCommand.OperationStatus or
        CliCommand.OperationConfirm or CliCommand.OperationExecute or CliCommand.TopicDeletePreview or CliCommand.OperationApproval or CliCommand.OperationApprove or
        CliCommand.TopicAlterPreview or CliCommand.TopicIncreasePartitionsPreview or CliCommand.OperationCancel or CliCommand.OperationReject;

    public static int MutationOutcomeCategory(string json, CliCommand command = CliCommand.None)
    {
        using var document = JsonDocument.Parse(json);
        if (!document.RootElement.TryGetProperty("state", out var state) || state.ValueKind != JsonValueKind.String)
            return RemoteError;
        return state.GetString()?.ToLowerInvariant() switch
        {
            "awaitingapproval" => PendingApproval, "executionunknown" or "executing" => UnknownEffect,
            "stalepreview" => Conflict, "expired" => Expired,
            "appliedunverified" or "partiallyapplied" => PartialOrUnverified,
            "cancelled" when command == CliCommand.OperationCancel => Success,
            "rejected" when command == CliCommand.OperationReject => Success,
            "failedbeforedispatch" or "faileddefinitive" or "rejected" or "cancelled" => RemoteError,
            "previewed" or "awaitingconfirmation" or "ready" or "appliedverified" => Success,
            _ => RemoteError,
        };
    }

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
    TopicCreatePreview = 15,
    OperationStatus = 16,
    OperationConfirm = 17,
    OperationExecute = 18,
    TopicDeletePreview = 19,
    OperationApproval = 20,
    OperationApprove = 21,
    TopicAlterPreview = 22,
    TopicIncreasePartitionsPreview = 23,
    OperationCancel = 24,
    OperationReject = 25,
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
    int? RightVersion = null,
    string? OidcProfileFile = null,
    string? IdempotencyKey = null,
    string? PreviewHash = null,
    string? TypedTargetChallenge = null,
    int? PartitionCount = null,
    short? ReplicationFactor = null,
    string? ServiceProfileFile = null,
    IReadOnlyDictionary<string, string?>? TopicConfigurations = null);

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
            CliCommand.TopicCreatePreview => $"/api/v1/automation/clusters/{RequirePrimary(invocation)}/topics/{RequireSecondary(invocation)}/create/preview",
            CliCommand.TopicDeletePreview => $"/api/v1/automation/clusters/{RequirePrimary(invocation)}/topics/{RequireSecondary(invocation)}/delete/preview",
            CliCommand.TopicAlterPreview => $"/api/v1/automation/clusters/{RequirePrimary(invocation)}/topics/{RequireSecondary(invocation)}/alter/preview",
            CliCommand.TopicIncreasePartitionsPreview => $"/api/v1/automation/clusters/{RequirePrimary(invocation)}/topics/{RequireSecondary(invocation)}/increase-partitions/preview",
            CliCommand.OperationStatus => $"/api/v1/automation/mutations/{RequirePrimary(invocation)}/status",
            CliCommand.OperationConfirm => $"/api/v1/automation/mutations/{RequirePrimary(invocation)}/confirm",
            CliCommand.OperationExecute => $"/api/v1/automation/mutations/{RequirePrimary(invocation)}/execute",
            CliCommand.OperationApproval => $"/api/v1/automation/mutations/{RequirePrimary(invocation)}/approval",
            CliCommand.OperationApprove => $"/api/v1/automation/mutations/{RequirePrimary(invocation)}/approve",
            CliCommand.OperationCancel => $"/api/v1/automation/mutations/{RequirePrimary(invocation)}/cancel",
            CliCommand.OperationReject => $"/api/v1/automation/mutations/{RequirePrimary(invocation)}/reject",
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
          kafdeck --oidc-profile <profile> [global-options] topics create-preview <cluster-id> <topic-name> <partitions> <replication-factor> --idempotency-key <key>
          kafdeck --oidc-profile <profile> [global-options] operations status <operation-id>
          kafdeck --oidc-profile <profile> [global-options] operations confirm <operation-id> <preview-hash> [--challenge <typed-target>]
          kafdeck --oidc-profile <profile> [global-options] operations execute <operation-id>
          kafdeck --oidc-profile <profile> [global-options] topics delete-preview <cluster-id> <topic-name> --idempotency-key <key>
          kafdeck --oidc-profile <profile> [global-options] topics alter-preview <cluster-id> <topic-name> --config <key=value> --idempotency-key <key>
          kafdeck --oidc-profile <profile> [global-options] topics increase-partitions-preview <cluster-id> <topic-name> <target-count> --idempotency-key <key>
          kafdeck --oidc-profile <profile> [global-options] operations approval <operation-id>
          kafdeck --oidc-profile <profile> [global-options] operations approve <operation-id> <preview-hash>
          kafdeck --oidc-profile <profile> [global-options] operations reject <operation-id> <preview-hash>
          kafdeck --oidc-profile <profile> [global-options] operations cancel <operation-id> <preview-hash>
          kafdeck --help

        Global options:
          --url <base-url>
          --token-file <path>
          --oidc-profile <public-profile.json>  Sign in using browser PKCE; memory-only bearer for this command.
          --service-profile <public-profile.json>  Sign in with a referenced RSA private key; memory-only service bearer.
          --output json
          --idempotency-key <key>  Required only for a new topic preview.
          --challenge <target>    Admitted only for operation confirmation.
          --config <key=value>   Repeat unique allowed keys for topic create/alter preview.
          --reset-config <key>   Reset a unique allowed topic key; alter preview only.

        Environment:
          KAFDECK_URL
          KAFDECK_ACCESS_TOKEN

        Security:
          Access tokens are intentionally not accepted as command-line arguments.
          Use KAFDECK_ACCESS_TOKEN or --token-file.
          The CLI exposes only typed governed API routes; there is no arbitrary HTTP path passthrough.
          Governed TopicCreate commands require non-browser OIDC and explicit backend admission.
          Approval commands require a separate eligible human. Other mutation families remain unavailable.
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
        string? oidcProfileFile = null;
        string? serviceProfileFile = null;
        string? idempotencyKey = null;
        string? typedChallenge = null;
        var topicConfigurations = new Dictionary<string, string?>(StringComparer.Ordinal);
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
                case "--oidc-profile":
                    oidcProfileFile = ValidateBoundedOption(RequireValue(args, ref index, "--oidc-profile"), "--oidc-profile", 2048);
                    break;
                case "--service-profile":
                    serviceProfileFile = ValidateBoundedOption(RequireValue(args, ref index, "--service-profile"), "--service-profile", 2048);
                    break;
                case "--idempotency-key":
                    idempotencyKey = ValidateBoundedOption(RequireValue(args, ref index, "--idempotency-key"), "--idempotency-key", 256);
                    break;
                case "--challenge":
                    typedChallenge = ValidateBoundedOption(RequireValue(args, ref index, "--challenge"), "--challenge", 512);
                    break;
                case "--config":
                    CliTopicConfiguration.Add(topicConfigurations, RequireValue(args, ref index, "--config"), reset: false);
                    break;
                case "--reset-config":
                    CliTopicConfiguration.Add(topicConfigurations, RequireValue(args, ref index, "--reset-config"), reset: true);
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

        if (oidcProfileFile is not null && serviceProfileFile is not null)
            throw new CliUsageException("Choose one human or service OIDC profile.");
        if ((oidcProfileFile is not null || serviceProfileFile is not null) && (tokenFile is not null || !string.IsNullOrEmpty(Environment.GetEnvironmentVariable("KAFDECK_ACCESS_TOKEN"))))
            throw new CliUsageException("OIDC sign-in cannot be combined with a deployment access-token source.");
        if (CliApplication.IsMutation(invocation.Command) && oidcProfileFile is null && serviceProfileFile is null)
            throw new CliUsageException("Governed operation commands require --oidc-profile or --service-profile; deployment credentials never acquire mutation authority.");
        if (invocation.Command is CliCommand.OperationApproval or CliCommand.OperationApprove or CliCommand.OperationReject && oidcProfileFile is null)
            throw new CliUsageException("Human approval commands require --oidc-profile; a service cannot become a human approver.");
        var isTopicPreview = invocation.Command is CliCommand.TopicCreatePreview or CliCommand.TopicDeletePreview or
            CliCommand.TopicAlterPreview or CliCommand.TopicIncreasePartitionsPreview;
        if (isTopicPreview && idempotencyKey is null)
            throw new CliUsageException("A topic mutation preview requires --idempotency-key.");
        if (!isTopicPreview && idempotencyKey is not null ||
            invocation.Command != CliCommand.OperationConfirm && typedChallenge is not null)
            throw new CliUsageException("Mutation options must match their named command.");
        if (topicConfigurations.Count > 0 && invocation.Command is not (CliCommand.TopicCreatePreview or CliCommand.TopicAlterPreview) ||
            invocation.Command == CliCommand.TopicCreatePreview && topicConfigurations.Values.Any(v => v is null) ||
            invocation.Command == CliCommand.TopicAlterPreview && topicConfigurations.Count == 0)
            throw new CliUsageException("Topic create accepts config values; topic alter requires config values or resets; other commands accept neither.");
        return invocation with { OidcProfileFile = oidcProfileFile, ServiceProfileFile = serviceProfileFile, IdempotencyKey = idempotencyKey, TypedTargetChallenge = typedChallenge,
            TopicConfigurations = new System.Collections.ObjectModel.ReadOnlyDictionary<string, string?>(topicConfigurations) };
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
        if (positionals.Count == 4 && positionals[0] == "topics" && positionals[1] == "alter-preview")
        {
            ValidateIdentifier(positionals[2], "cluster ID"); ValidateIdentifier(positionals[3], "topic name", 249);
            return Create(baseUri, tokenFile, CliCommand.TopicAlterPreview, positionals[2], positionals[3], output: output);
        }
        if (positionals.Count == 5 && positionals[0] == "topics" && positionals[1] == "increase-partitions-preview")
        {
            ValidateIdentifier(positionals[2], "cluster ID"); ValidateIdentifier(positionals[3], "topic name", 249);
            if (!int.TryParse(positionals[4], System.Globalization.NumberStyles.None, System.Globalization.CultureInfo.InvariantCulture,
                out var target) || target is < 1 or > 100000)
                throw new CliUsageException("Partition increase requires a target count between 1 and 100000; the backend validates current state.");
            return Create(baseUri, tokenFile, CliCommand.TopicIncreasePartitionsPreview, positionals[2], positionals[3], output: output) with { PartitionCount = target };
        }
        if (positionals.Count == 4 && positionals[0] == "topics" && positionals[1] == "delete-preview")
        {
            ValidateIdentifier(positionals[2], "cluster ID"); ValidateIdentifier(positionals[3], "topic name", 249);
            return Create(baseUri, tokenFile, CliCommand.TopicDeletePreview, positionals[2], positionals[3], output: output);
        }
        if (positionals.Count == 6 && positionals[0] == "topics" && positionals[1] == "create-preview")
        {
            ValidateIdentifier(positionals[2], "cluster ID"); ValidateIdentifier(positionals[3], "topic name", 249);
            if (!int.TryParse(positionals[4], System.Globalization.NumberStyles.None, System.Globalization.CultureInfo.InvariantCulture, out var partitions) ||
                partitions is < 1 or > 100000 || !short.TryParse(positionals[5], System.Globalization.NumberStyles.None,
                    System.Globalization.CultureInfo.InvariantCulture, out var replicas) || replicas < 1)
                throw new CliUsageException("TopicCreate requires 1..100000 partitions and a positive bounded replication factor.");
            return Create(baseUri, tokenFile, CliCommand.TopicCreatePreview, positionals[2], positionals[3], output: output)
                with { PartitionCount = partitions, ReplicationFactor = replicas };
        }
        if (positionals.Count is 3 or 4 && positionals[0] == "operations" && positionals[1] is "status" or "confirm" or "execute" or "approval" or "approve" or "cancel" or "reject")
        {
            var needsHash = positionals[1] is "confirm" or "approve" or "cancel" or "reject";
            if (!Guid.TryParseExact(positionals[2], "D", out _) || (needsHash ? positionals.Count != 4 : positionals.Count != 3))
                throw new CliUsageException("A canonical operation UUID and the command's exact arguments are required.");
            if (needsHash && (positionals[3].Length != 64 || !positionals[3].All(char.IsAsciiHexDigit)))
                throw new CliUsageException("Confirmation requires the exact 64-character preview hash returned by the backend.");
            var command = positionals[1] switch { "status" => CliCommand.OperationStatus, "confirm" => CliCommand.OperationConfirm,
                "approval" => CliCommand.OperationApproval, "approve" => CliCommand.OperationApprove,
                "cancel" => CliCommand.OperationCancel, "reject" => CliCommand.OperationReject, _ => CliCommand.OperationExecute };
            return Create(baseUri, tokenFile, command, positionals[2], output: output)
                with { PreviewHash = needsHash ? positionals[3] : null };
        }
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
