using Kafdeck.Cli;
using Xunit;

namespace Kafdeck.Architecture.Tests;

public sealed class V08W67CliFoundationTests
{
    [Fact]
    public void Cli_defaults_to_local_kafdeck_origin()
    {
        var invocation =
            CliParser.Parse(
                ["system", "info"]);

        Assert.Equal(
            new Uri("http://127.0.0.1:8080/"),
            invocation.BaseUri);
        Assert.Equal(
            CliCommand.SystemInfo,
            invocation.Command);
    }

    [Fact]
    public void Cli_accepts_explicit_safe_origin_and_cluster_read()
    {
        var invocation =
            CliParser.Parse(
                [
                    "--url",
                    "https://kafdeck.example/",
                    "clusters",
                    "get",
                    "prod-a",
                ]);

        Assert.Equal(
            new Uri("https://kafdeck.example/"),
            invocation.BaseUri);
        Assert.Equal(
            CliCommand.ClusterGet,
            invocation.Command);
        Assert.Equal(
            "prod-a",
            invocation.ResourceId);
    }

    [Theory]
    [InlineData("https://user:pass@kafdeck.example")]
    [InlineData("https://kafdeck.example/api")]
    [InlineData("https://kafdeck.example/?token=secret")]
    [InlineData("file:///tmp/kafdeck")]
    public void Cli_rejects_unsafe_or_non_origin_base_urls(
        string url)
    {
        Assert.Throws<CliUsageException>(
            () => CliParser.Parse(
                [
                    "--url",
                    url,
                    "system",
                    "info",
                ]));
    }

    [Fact]
    public void Cli_never_accepts_access_token_as_process_argument()
    {
        var exception =
            Assert.Throws<CliUsageException>(
                () => CliParser.Parse(
                    [
                        "--token",
                        "super-secret",
                        "clusters",
                        "list",
                    ]));

        Assert.Contains(
            "not accepted on the command line",
            exception.Message,
            StringComparison.Ordinal);
    }

    [Theory]
    [InlineData("operations", "get", "00000000-0000-0000-0000-000000000001")]
    [InlineData("approvals", "list", null)]
    public void Cli_withholds_mutation_reads_until_non_browser_oidc_is_supported(
        string command,
        string action,
        string? identity)
    {
        var args =
            identity is null
                ? new[] { command, action }
                : new[] { command, action, identity };

        var exception =
            Assert.Throws<CliUsageException>(
                () => CliParser.Parse(args));

        Assert.Contains(
            "unavailable command",
            exception.Message,
            StringComparison.OrdinalIgnoreCase);
        Assert.DoesNotContain(
            "operations get",
            CliParser.Usage,
            StringComparison.Ordinal);
        Assert.DoesNotContain(
            "approvals list",
            CliParser.Usage,
            StringComparison.Ordinal);
        Assert.Contains(
            "non-browser OIDC",
            CliParser.Usage,
            StringComparison.Ordinal);
    }

    [Theory]
    [InlineData("system", "info", "/api/v1/system/info")]
    [InlineData("clusters", "list", "/api/v1/clusters")]
    public void Resource_free_routes_do_not_require_cluster_identity(
        string command,
        string action,
        string expected)
    {
        var invocation =
            CliParser.Parse(
                [command, action]);

        Assert.Null(
            invocation.ResourceId);
        Assert.Equal(
            expected,
            CliRouteBuilder.Build(
                invocation));
    }

    [Theory]
    [InlineData("clusters", "get", ".")]
    [InlineData("topics", "list", "..")]
    public void Cli_rejects_dot_only_primary_resource_segments(
        string command,
        string action,
        string resource)
    {
        Assert.Throws<CliUsageException>(
            () => CliParser.Parse(
                [command, action, resource]));
    }

    [Theory]
    [InlineData("topics", "get", "prod-a", ".")]
    [InlineData("consumer-groups", "get", "prod-a", "..")]
    [InlineData("schemas", "versions", "prod-a", ".")]
    public void Cli_rejects_dot_only_secondary_resource_segments(
        string command,
        string action,
        string cluster,
        string resource)
    {
        Assert.Throws<CliUsageException>(
            () => CliParser.Parse(
                [command, action, cluster, resource]));
    }

    [Fact]
    public void Topics_list_builds_bounded_encoded_route()
    {
        var invocation =
            CliParser.Parse(
                [
                    "--output",
                    "json",
                    "topics",
                    "list",
                    "prod-a",
                    "--search",
                    "order events",
                    "--cursor",
                    "a+/=",
                    "--limit",
                    "200",
                ]);

        Assert.Equal(
            CliCommand.TopicsList,
            invocation.Command);
        Assert.Equal(
            200,
            invocation.Limit);
        Assert.Equal(
            CliOutputFormat.Json,
            invocation.Output);
        Assert.Equal(
            "/api/v1/clusters/prod-a/topics?q=order%20events&cursor=a%2B%2F%3D&pageSize=200",
            CliRouteBuilder.Build(
                invocation));
    }

    [Theory]
    [InlineData("0")]
    [InlineData("201")]
    [InlineData("-1")]
    [InlineData("not-a-number")]
    public void Topics_list_rejects_out_of_range_page_size(
        string value)
    {
        Assert.Throws<CliUsageException>(
            () => CliParser.Parse(
                [
                    "topics",
                    "list",
                    "prod-a",
                    "--limit",
                    value,
                ]));
    }

    [Fact]
    public void Topic_pagination_options_are_not_generic_http_query_options()
    {
        Assert.Throws<CliUsageException>(
            () => CliParser.Parse(
                [
                    "clusters",
                    "list",
                    "--limit",
                    "10",
                ]));

        Assert.Throws<CliUsageException>(
            () => CliParser.Parse(
                [
                    "consumer-groups",
                    "list",
                    "prod-a",
                    "--cursor",
                    "opaque",
                ]));
    }

    [Theory]
    [InlineData("topics", "get", "prod-a", "orders.main", "/api/v1/clusters/prod-a/topics/orders.main")]
    [InlineData("consumer-groups", "get", "prod-a", "orders-group", "/api/v1/clusters/prod-a/consumer-groups/orders-group")]
    [InlineData("consumer-groups", "lag", "prod-a", "orders-group", "/api/v1/clusters/prod-a/consumer-groups/orders-group/lag")]
    [InlineData("schemas", "versions", "prod-a", "orders-value", "/api/v1/clusters/prod-a/schemas/subjects/orders-value/versions")]
    public void Read_only_detail_routes_are_typed_and_confined(
        string command,
        string action,
        string cluster,
        string resource,
        string expected)
    {
        var invocation =
            CliParser.Parse(
                [
                    command,
                    action,
                    cluster,
                    resource,
                ]);

        Assert.Equal(
            expected,
            CliRouteBuilder.Build(
                invocation));
    }

    [Theory]
    [InlineData("topics", "get", "prod-a", "orders/main")]
    [InlineData("consumer-groups", "get", "prod-a", "orders/group")]
    [InlineData("consumer-groups", "lag", "prod-a", "orders/group")]
    [InlineData("schemas", "versions", "prod-a", "orders/value")]
    public void Cli_rejects_slash_bearing_route_identities(
        string command,
        string action,
        string cluster,
        string resource)
    {
        Assert.Throws<CliUsageException>(
            () => CliParser.Parse(
                [command, action, cluster, resource]));
    }

    [Theory]
    [InlineData("consumer-groups", "list", "prod-a", "/api/v1/clusters/prod-a/consumer-groups")]
    [InlineData("schemas", "subjects", "prod-a", "/api/v1/clusters/prod-a/schemas/subjects")]
    public void Read_only_collection_routes_are_typed(
        string command,
        string action,
        string cluster,
        string expected)
    {
        var invocation =
            CliParser.Parse(
                [
                    command,
                    action,
                    cluster,
                ]);

        Assert.Equal(
            expected,
            CliRouteBuilder.Build(
                invocation));
    }

    [Theory]
    [InlineData(
        "consumer-groups",
        "diagnostics",
        "prod-a",
        "orders-group",
        "/api/v1/clusters/prod-a/consumer-groups/orders-group/diagnostics")]
    [InlineData(
        "schemas",
        "compatibility",
        "prod-a",
        "orders-value",
        "/api/v1/clusters/prod-a/schemas/subjects/orders-value/compatibility")]
    public void Additional_read_only_routes_are_typed_and_confined(
        string command,
        string action,
        string cluster,
        string resource,
        string expected)
    {
        var invocation =
            CliParser.Parse(
                [
                    command,
                    action,
                    cluster,
                    resource,
                ]);

        Assert.Equal(
            expected,
            CliRouteBuilder.Build(
                invocation));
    }

    [Fact]
    public void Schema_version_detail_route_uses_positive_positional_version()
    {
        var invocation =
            CliParser.Parse(
                [
                    "schemas",
                    "version",
                    "prod-a",
                    "orders-value",
                    "7",
                ]);

        Assert.Equal(
            CliCommand.SchemaVersionGet,
            invocation.Command);
        Assert.Equal(
            7,
            invocation.Version);
        Assert.Equal(
            "/api/v1/clusters/prod-a/schemas/subjects/orders-value/versions/7",
            CliRouteBuilder.Build(
                invocation));
    }

    [Fact]
    public void Schema_diff_route_uses_only_typed_positive_version_query_values()
    {
        var invocation =
            CliParser.Parse(
                [
                    "schemas",
                    "diff",
                    "prod-a",
                    "orders-value",
                    "2",
                    "9",
                ]);

        Assert.Equal(
            CliCommand.SchemaDiff,
            invocation.Command);
        Assert.Equal(
            2,
            invocation.Version);
        Assert.Equal(
            9,
            invocation.RightVersion);
        Assert.Equal(
            "/api/v1/clusters/prod-a/schemas/subjects/orders-value/diff?leftVersion=2&rightVersion=9",
            CliRouteBuilder.Build(
                invocation));
    }

    [Theory]
    [InlineData("0")]
    [InlineData("-1")]
    [InlineData("+1")]
    [InlineData("1.5")]
    [InlineData("not-a-version")]
    public void Schema_version_commands_reject_non_positive_or_non_integer_versions(
        string version)
    {
        Assert.Throws<CliUsageException>(
            () => CliParser.Parse(
                [
                    "schemas",
                    "version",
                    "prod-a",
                    "orders-value",
                    version,
                ]));

        Assert.Throws<CliUsageException>(
            () => CliParser.Parse(
                [
                    "schemas",
                    "diff",
                    "prod-a",
                    "orders-value",
                    "1",
                    version,
                ]));
    }

    [Fact]
    public void Additional_read_only_commands_do_not_admit_generic_query_options()
    {
        Assert.Throws<CliUsageException>(
            () => CliParser.Parse(
                [
                    "consumer-groups",
                    "diagnostics",
                    "prod-a",
                    "orders-group",
                    "--limit",
                    "10",
                ]));

        Assert.Throws<CliUsageException>(
            () => CliParser.Parse(
                [
                    "schemas",
                    "compatibility",
                    "prod-a",
                    "orders-value",
                    "--cursor",
                    "opaque",
                ]));
    }

    [Theory]
    [InlineData("yaml")]
    [InlineData("table")]
    [InlineData("raw")]
    public void Cli_rejects_unadmitted_output_formats(
        string format)
    {
        Assert.Throws<CliUsageException>(
            () => CliParser.Parse(
                [
                    "--output",
                    format,
                    "clusters",
                    "list",
                ]));
    }

    [Fact]
    public async Task Empty_token_file_returns_usage_error_instead_of_throwing()
    {
        var tokenFile = Path.Combine(
            Path.GetTempPath(),
            $"kafdeck-cli-token-{Guid.NewGuid():N}.txt");

        try
        {
            await File.WriteAllTextAsync(
                tokenFile,
                "   ");

            var output = new StringWriter();
            var error = new StringWriter();

            var exitCode =
                await CliApplication.RunAsync(
                    [
                        "--token-file",
                        tokenFile,
                        "system",
                        "info",
                    ],
                    output,
                    error,
                    CancellationToken.None);

            Assert.Equal(
                CliApplication.UsageError,
                exitCode);
            Assert.Contains(
                "access token is empty",
                error.ToString(),
                StringComparison.OrdinalIgnoreCase);
        }
        finally
        {
            File.Delete(tokenFile);
        }
    }

    [Fact]
    public void Cli_help_ignores_invalid_ambient_url()
    {
        var original =
            Environment.GetEnvironmentVariable(
                "KAFDECK_URL");

        try
        {
            Environment.SetEnvironmentVariable(
                "KAFDECK_URL",
                "not-a-valid-origin");

            var invocation =
                CliParser.Parse(
                    ["--help"]);

            Assert.True(
                invocation.ShowHelp);
            Assert.Equal(
                CliCommand.None,
                invocation.Command);
            Assert.Equal(
                new Uri("http://127.0.0.1:8080/"),
                invocation.BaseUri);
        }
        finally
        {
            Environment.SetEnvironmentVariable(
                "KAFDECK_URL",
                original);
        }
    }

    [Fact]
    public void Cli_rejects_blank_token_file_path()
    {
        var exception =
            Assert.Throws<CliUsageException>(
                () => CliParser.Parse(
                    [
                        "--token-file",
                        "   ",
                        "system",
                        "info",
                    ]));

        Assert.Contains(
            "non-empty path",
            exception.Message,
            StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public async Task Directory_token_file_returns_transport_error()
    {
        var tokenDirectory =
            Path.Combine(
                Path.GetTempPath(),
                $"kafdeck-cli-token-dir-{Guid.NewGuid():N}");
        Directory.CreateDirectory(
            tokenDirectory);

        var originalToken =
            Environment.GetEnvironmentVariable(
                "KAFDECK_ACCESS_TOKEN");

        try
        {
            Environment.SetEnvironmentVariable(
                "KAFDECK_ACCESS_TOKEN",
                null);

            var output =
                new StringWriter();
            var error =
                new StringWriter();

            var exitCode =
                await CliApplication.RunAsync(
                    [
                        "--token-file",
                        tokenDirectory,
                        "system",
                        "info",
                    ],
                    output,
                    error,
                    CancellationToken.None);

            Assert.Equal(
                CliApplication.TransportError,
                exitCode);
            Assert.Contains(
                "credential/configuration I/O error",
                error.ToString(),
                StringComparison.OrdinalIgnoreCase);
        }
        finally
        {
            Environment.SetEnvironmentVariable(
                "KAFDECK_ACCESS_TOKEN",
                originalToken);
            Directory.Delete(
                tokenDirectory,
                recursive: true);
        }
    }

    [Fact]
    public void Cli_request_deadline_covers_header_and_body_read()
    {
        var root = FindRepositoryRoot();
        var application = File.ReadAllText(
            Path.Combine(
                root,
                "src",
                "cli",
                "Kafdeck.Cli",
                "CliApplication.cs"));
        var client = File.ReadAllText(
            Path.Combine(
                root,
                "src",
                "cli",
                "Kafdeck.Cli",
                "KafdeckCliClient.cs"));

        Assert.Contains(
            "deadline.CancelAfter",
            application,
            StringComparison.Ordinal);
        Assert.Contains(
            "ReadAsStringAsync(requestToken)",
            application,
            StringComparison.Ordinal);
        Assert.Contains(
            "catch (OperationCanceledException)",
            application,
            StringComparison.Ordinal);
        Assert.Contains(
            "Timeout = Timeout.InfiniteTimeSpan",
            client,
            StringComparison.Ordinal);
    }

    [Fact]
    public void Cli_project_has_no_direct_kafka_or_provider_dependencies()
    {
        var root = FindRepositoryRoot();
        var project = File.ReadAllText(
            Path.Combine(
                root,
                "src",
                "cli",
                "Kafdeck.Cli",
                "Kafdeck.Cli.csproj"));

        Assert.DoesNotContain(
            "Confluent.Kafka",
            project,
            StringComparison.Ordinal);
        Assert.DoesNotContain(
            "Kafdeck.Infrastructure",
            project,
            StringComparison.Ordinal);
        Assert.DoesNotContain(
            "PackageReference",
            project,
            StringComparison.Ordinal);
    }

    private static string FindRepositoryRoot()
    {
        DirectoryInfo? current =
            new(AppContext.BaseDirectory);
        while (current is not null)
        {
            if (File.Exists(
                    Path.Combine(
                        current.FullName,
                        "Kafdeck.slnx")))
            {
                return current.FullName;
            }

            current = current.Parent;
        }

        throw new DirectoryNotFoundException(
            "Unable to locate Kafdeck repository root.");
    }
}
