using System.Net;
using Kafdeck.Cli;
using Xunit;

namespace Kafdeck.Architecture.Tests;

public sealed class V08W67CliPkceLoopbackBoundaryTests
{
    [Theory]
    [InlineData("http://127.0.0.1:49152/oauth/callback")]
    [InlineData("http://[::1]:49152/oauth/callback")]
    public void Explicit_numeric_loopback_uri_and_matching_listener_are_valid(
        string input)
    {
        var uri = new Uri(input);
        var expected = CliOidcLoopbackPolicy.ValidateRedirectUri(uri);
        CliOidcLoopbackPolicy.RequireExactListenerAddress(uri, expected);
    }

    [Theory]
    [InlineData("http://localhost:49152/oauth/callback")]
    [InlineData("http://127.0.0.2:49152/oauth/callback")]
    [InlineData("http://0.0.0.0:49152/oauth/callback")]
    [InlineData("http://[::]:49152/oauth/callback")]
    [InlineData("http://192.168.1.7:49152/oauth/callback")]
    [InlineData("https://127.0.0.1:49152/oauth/callback")]
    [InlineData("http://127.0.0.1/oauth/callback")]
    [InlineData("http://127.0.0.1:80/oauth/callback")]
    [InlineData("http://127.0.0.1:49152/other")]
    [InlineData("http://127.0.0.1:49152/oauth/callback?code=secret")]
    [InlineData("http://127.0.0.1:49152/oauth/callback#token")]
    [InlineData("http://user:pass@127.0.0.1:49152/oauth/callback")]
    [InlineData("http://127.0.0.1:49152/%6fauth/callback")]
    public void Hostname_nonloopback_or_changed_authority_and_path_are_denied(
        string input)
    {
        Assert.Throws<CliUsageException>(
            () => CliOidcLoopbackPolicy.ValidateRedirectUri(new Uri(input)));
    }

    [Theory]
    [InlineData("http://127.0.0.1:49152/oauth/callback", "0.0.0.0")]
    [InlineData("http://127.0.0.1:49152/oauth/callback", "::1")]
    [InlineData("http://127.0.0.1:49152/oauth/callback", "127.0.0.2")]
    [InlineData("http://[::1]:49152/oauth/callback", "::")]
    [InlineData("http://[::1]:49152/oauth/callback", "127.0.0.1")]
    public void Wildcard_cross_family_or_different_listener_address_is_denied(
        string redirect, string listener)
    {
        Assert.Throws<CliUsageException>(() =>
            CliOidcLoopbackPolicy.RequireExactListenerAddress(
                new Uri(redirect), IPAddress.Parse(listener)));
    }

    [Fact]
    public void Missing_listener_address_is_rejected()
    {
        Assert.Throws<ArgumentNullException>(() =>
            CliOidcLoopbackPolicy.RequireExactListenerAddress(
                new Uri("http://127.0.0.1:49152/oauth/callback"), null!));
    }

    [Fact]
    public void Existing_read_only_cli_parser_does_not_accept_pkce_commands()
    {
        Assert.Throws<CliUsageException>(() =>
            CliParser.Parse(["auth", "login"]));
    }
}
