using System.Text.Json;
using Kafdeck.Cli;

namespace Kafdeck.Mcp;

public sealed record McpApiRequest(string Path, object? Body = null, string? IdempotencyKey = null, bool Post = false);
public sealed record McpApiResponse(int Status, JsonElement? Data);
public interface IMcpApiBackend { Task<McpApiResponse> SendAsync(McpApiRequest request, CancellationToken cancellationToken); }

/// <summary>One fixed operator-configured origin and identity, never supplied by a tool.</summary>
public sealed class McpApiBackend(CliInvocation invocation) : IMcpApiBackend, IDisposable
{
    private KafdeckCliClient? client;
    public async Task<McpApiResponse> SendAsync(McpApiRequest request, CancellationToken cancellationToken)
    {
        client ??= await KafdeckCliClient.CreateAsync(invocation, cancellationToken);
        using var response = request.Post
            ? await client.PostAsync(request.Path, request.Body, request.IdempotencyKey, cancellationToken)
            : await client.GetAsync(request.Path, cancellationToken);
        if (!response.IsSuccessStatusCode) return new((int)response.StatusCode, null);
        // Both advertised length and actual bytes are bounded. Never print remote error bodies.
        var body = await CliResponseBodyReader.ReadAsync(response.Content, cancellationToken);
        using var document = JsonDocument.Parse(body, new() { MaxDepth = 32 });
        return new((int)response.StatusCode, document.RootElement.Clone());
    }
    public void Dispose() => client?.Dispose();
}
