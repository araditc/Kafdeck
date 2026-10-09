using System.Buffers;
using System.Text;

namespace Kafdeck.Cli;

/// <summary>
/// Fail-closed, bounded response consumption for both JSON and error bodies.
/// A hostile server cannot force unbounded allocations in the read-only CLI.
/// </summary>
public static class CliResponseBodyReader
{
    public const int MaxBodyBytes = 2 * 1024 * 1024;
    private const int ChunkBytes = 16 * 1024;

    public static async Task<string> ReadAsync(
        HttpContent content,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(content);

        if (content.Headers.ContentLength is > MaxBodyBytes)
            throw new CliResponseTooLargeException();

        await using var stream = await content.ReadAsStreamAsync(cancellationToken);
        using var buffer = new MemoryStream();
        var rented = ArrayPool<byte>.Shared.Rent(ChunkBytes);
        try
        {
            while (true)
            {
                cancellationToken.ThrowIfCancellationRequested();
                var read = await stream.ReadAsync(
                    rented.AsMemory(0, Math.Min(ChunkBytes, MaxBodyBytes - (int)buffer.Length + 1)),
                    cancellationToken);
                if (read == 0) break;
                if (buffer.Length + read > MaxBodyBytes)
                    throw new CliResponseTooLargeException();
                buffer.Write(rented, 0, read);
            }
            return Encoding.UTF8.GetString(buffer.GetBuffer(), 0, (int)buffer.Length);
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(rented, clearArray: true);
        }
    }
}

public sealed class CliResponseTooLargeException : Exception
{
}
