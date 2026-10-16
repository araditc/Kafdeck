using System.Globalization;
using System.Text;
using Kafdeck.Core.Notifications;

namespace Kafdeck.Infrastructure.Configuration;

/// <summary>Bounded HTTP/1.1 for confidential Webhook and Telegram URLs. No
/// HttpClient/HTTP DiagnosticSource/EventSource sees this private request URI.</summary>
internal static class NotificationPrivateHttpWire
{
    internal const int MaxBodyBytes = 16 * 1024;
    internal sealed record Response(int Status, byte[] Body);

    internal static async Task<Response> PostAsync(NotificationPinnedHttpHandlerFactory connections,
        NotificationPinnedEndpoint pinned, byte[] body, CancellationToken token,
        bool readBody = true, NotificationCredentialValue? bearer = null, NotificationDestinationProfile? profile = null)
    {
        // HTTP Host is ASCII IDNA, even when the approved URI used a Unicode
        // hostname. Preserve IPv6 brackets and the exact non-default port.
        var authority = pinned.Endpoint.IdnHost;
        if (pinned.Endpoint.HostNameType == UriHostNameType.IPv6)
            authority = "[" + authority.Trim('[', ']') + "]";
        if (!pinned.Endpoint.IsDefaultPort)
            authority += ":" + pinned.Endpoint.Port.ToString(CultureInfo.InvariantCulture);
        var headers = Encoding.ASCII.GetBytes("POST " + pinned.Endpoint.PathAndQuery + " HTTP/1.1\r\nHost: " +
            authority + "\r\nContent-Type: application/json\r\nContent-Length: " +
            body.Length.ToString(CultureInfo.InvariantCulture) + "\r\n" +
            (bearer is null ? string.Empty : "Authorization: Bearer " + bearer.Reveal() + "\r\n") +
            "Connection: close\r\n\r\n");
        if (headers.Length > 8192) throw new IOException("Provider request headers exceed finite budget.");
        await using var tls = await connections.OpenPinnedTlsStreamAsync(pinned, token, profile).ConfigureAwait(false);
        await connections.VerifySendAdmissionAsync(token, profile).ConfigureAwait(false);
        await tls.WriteAsync(headers, token).ConfigureAwait(false);
        await tls.WriteAsync(body, token).ConfigureAwait(false);
        var reader = new BoundedLineReader(tls);
        var statusLine = await reader.ReadAsync(512, token).ConfigureAwait(false);
        if (statusLine.Length < 13 || statusLine[12] != ' ' || !(statusLine.StartsWith("HTTP/1.1 ", StringComparison.Ordinal) ||
            statusLine.StartsWith("HTTP/1.0 ", StringComparison.Ordinal)) ||
            !int.TryParse(statusLine.AsSpan(9, 3), NumberStyles.None, CultureInfo.InvariantCulture, out var status) ||
            status is < 100 or > 599)
            throw new IOException("Invalid private provider HTTP status.");
        int? length = null;
        var chunked = false;
        var headerBytes = 0;
        for (var count = 0; ; count++)
        {
            var line = await reader.ReadAsync(1024, token).ConfigureAwait(false);
            headerBytes += line.Length;
            if (count >= 32 || headerBytes > 16 * 1024) throw new IOException("Provider response headers exceed budget.");
            if (line.Length == 0) break;
            var colon = line.IndexOf(':');
            if (colon <= 0) throw new IOException("Invalid provider header.");
            var name = line[..colon];
            var value = line[(colon + 1)..].Trim();
            if (name.Equals("Content-Length", StringComparison.OrdinalIgnoreCase))
            {
                if (length is not null || chunked || !int.TryParse(value, NumberStyles.None, CultureInfo.InvariantCulture, out var bytes) || bytes < 0 || (readBody && bytes > MaxBodyBytes))
                    throw new IOException("Invalid bounded provider response framing.");
                length = bytes;
            }
            if (name.Equals("Transfer-Encoding", StringComparison.OrdinalIgnoreCase))
            {
                if (length is not null || chunked || !value.Equals("chunked", StringComparison.OrdinalIgnoreCase))
                    throw new IOException("Unsupported provider response framing.");
                chunked = true;
            }
        }
        if (!readBody || status is < 200 or >= 300) return new Response(status, []);
        using var result = new MemoryStream();
        var buffer = new byte[MaxBodyBytes + 1];
        if (chunked)
        {
            for (var count = 0; count < 256; count++)
            {
                var line = await reader.ReadAsync(32, token).ConfigureAwait(false);
                if (!int.TryParse(line, NumberStyles.AllowHexSpecifier, CultureInfo.InvariantCulture, out var bytes) ||
                    bytes < 0 || bytes > MaxBodyBytes - result.Length) throw new IOException("Invalid bounded response chunk.");
                if (bytes == 0)
                {
                    var trailerBytes = 0;
                    for (var trailers = 0; trailers < 16; trailers++)
                    {
                        var trailer = await reader.ReadAsync(1024, token).ConfigureAwait(false);
                        trailerBytes += trailer.Length;
                        if (trailerBytes > 4096) throw new IOException("Response trailer budget exceeded.");
                        if (trailer.Length == 0) return new Response(status, result.ToArray());
                    }
                    throw new IOException("Response trailer count exceeded.");
                }
                await tls.ReadExactlyAsync(buffer.AsMemory(0, bytes), token).ConfigureAwait(false);
                result.Write(buffer, 0, bytes);
                if ((await reader.ReadAsync(2, token).ConfigureAwait(false)).Length != 0)
                    throw new IOException("Invalid response chunk terminator.");
            }
            throw new IOException("Response chunk count exceeded.");
        }
        if (length is not null)
        {
            await tls.ReadExactlyAsync(buffer.AsMemory(0, length.Value), token).ConfigureAwait(false);
            return new Response(status, buffer[..length.Value]);
        }
        while (result.Length <= MaxBodyBytes)
        {
            var read = await tls.ReadAsync(buffer.AsMemory(0, Math.Min(buffer.Length, MaxBodyBytes + 1 - (int)result.Length)), token).ConfigureAwait(false);
            if (read == 0) return new Response(status, result.ToArray());
            result.Write(buffer, 0, read);
        }
        throw new IOException("Response body budget exceeded.");
    }
}

internal sealed class BoundedLineReader(Stream stream)
{
    private readonly byte[] _one = new byte[1];
    internal async Task<string> ReadAsync(int maxBytes, CancellationToken token)
    {
        var bytes = new List<byte>();
        while (bytes.Count < maxBytes)
        {
            if (await stream.ReadAsync(_one, token).ConfigureAwait(false) == 0) throw new IOException("Provider closed before complete response.");
            var next = _one[0];
            if (bytes.Count > 0 && bytes[^1] == 13 && next != 10)
                throw new IOException("Provider response requires CRLF framing.");
            if (next == 10)
            {
                if (bytes.Count == 0 || bytes[^1] != 13) throw new IOException("Provider response requires CRLF framing.");
                bytes.RemoveAt(bytes.Count - 1);
                return Encoding.ASCII.GetString(bytes.ToArray());
            }
            if (next != 13 && (next < 32 || next > 126)) throw new IOException("Invalid provider response line.");
            bytes.Add(next);
        }
        throw new IOException("Provider response line budget exceeded.");
    }
}
