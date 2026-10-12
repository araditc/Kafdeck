using System.Text;
using Kafdeck.Cli;
using Xunit;

namespace Kafdeck.Architecture.Tests;

public sealed class V08W67CliTokenFileSecurityTests
{
    [Fact]
    public async Task Oversized_token_file_is_rejected_before_client_creation()
    {
        var file = TemporaryTokenFile();
        try
        {
            await File.WriteAllBytesAsync(
                file, Enumerable.Repeat((byte)'a', KafdeckCliClient.MaxTokenBytes + 1).ToArray());

            var exception = await Assert.ThrowsAsync<CliUsageException>(
                async () => { using var _ = await CreateClientAsync(file); });

            Assert.Contains("exceeds", exception.Message, StringComparison.OrdinalIgnoreCase);
            Assert.Contains(
                KafdeckCliClient.MaxTokenBytes.ToString(), exception.Message,
                StringComparison.Ordinal);
        }
        finally
        {
            File.Delete(file);
        }
    }

    [Fact]
    public async Task Exact_maximum_utf8_byte_budget_is_accepted()
    {
        var file = TemporaryTokenFile();
        try
        {
            await File.WriteAllTextAsync(
                file, new string('a', KafdeckCliClient.MaxTokenBytes));
            using var client = await CreateClientAsync(file);
            Assert.NotNull(client);
        }
        finally
        {
            File.Delete(file);
        }
    }

    [Theory]
    [InlineData("safe-token\r\nInjected: arbitrary")]
    [InlineData("safe-token\u0000hidden")]
    [InlineData("tokén")]
    [InlineData("two tokens")]
    public async Task Invalid_header_material_is_rejected_before_request(
        string content)
    {
        var file = TemporaryTokenFile();
        try
        {
            await File.WriteAllTextAsync(file, content);
            var exception = await Assert.ThrowsAsync<CliUsageException>(
                async () => { using var _ = await CreateClientAsync(file); });
            Assert.Contains(
                "invalid header characters", exception.Message,
                StringComparison.OrdinalIgnoreCase);
            Assert.DoesNotContain(content, exception.Message, StringComparison.Ordinal);
        }
        finally
        {
            File.Delete(file);
        }
    }

    [Fact]
    public async Task Malformed_utf8_is_rejected_before_authentication()
    {
        var file = TemporaryTokenFile();
        try
        {
            await File.WriteAllBytesAsync(file, [0xC3, 0x28]);
            var exception = await Assert.ThrowsAsync<CliUsageException>(
                async () => { using var _ = await CreateClientAsync(file); });
            Assert.Contains(
                "valid UTF-8", exception.Message, StringComparison.Ordinal);
        }
        finally
        {
            File.Delete(file);
        }
    }

    [Fact]
    public async Task Terminal_newline_and_utf8_bom_remain_compatible()
    {
        var file = TemporaryTokenFile();
        try
        {
            await File.WriteAllTextAsync(file, "safe-token\n", Encoding.UTF8);
            using var client = await CreateClientAsync(file);
            Assert.NotNull(client);
        }
        finally
        {
            File.Delete(file);
        }
    }

    private static string TemporaryTokenFile() =>
        Path.Combine(Path.GetTempPath(), $"kafdeck-w67-token-{Guid.NewGuid():N}.txt");

    private static Task<KafdeckCliClient> CreateClientAsync(string file)
    {
        var invocation = CliParser.Parse(
            ["--token-file", file, "system", "info"]);
        return KafdeckCliClient.CreateAsync(invocation, CancellationToken.None);
    }
}
