using System.Net;
using System.Net.Http.Headers;
using System.Text;
using System.Text.Json;
using Kafdeck.Core.ReadViews;
using Kafdeck.Infrastructure.Configuration;

namespace Kafdeck.Infrastructure.Ecosystem;

internal sealed record HttpReadRuntime(
    HttpClient Client,
    AuthenticationHeaderValue? Authorization);

internal sealed class ReadViewHttpException : Exception
{
    public ReadViewHttpException(
        ReadViewFailureCategory category,
        string code,
        string safeMessage,
        bool retryable)
    {
        Category = category;
        Code = code;
        SafeMessage = safeMessage;
        Retryable = retryable;
    }

    public ReadViewFailureCategory Category { get; }
    public string Code { get; }
    public string SafeMessage { get; }
    public bool Retryable { get; }
}

internal sealed class ResponseBoundExceededException : Exception;

internal static class ReadOnlyHttpSupport
{
    public static HttpReadRuntime CreateRuntime(
        string url,
        SecretReference? usernameReference,
        SecretReference? passwordReference,
        SecretResolver secretResolver,
        HttpMessageHandler handler)
    {
        var baseUri = new Uri(
            url.EndsWith("/", StringComparison.Ordinal) ? url : url + "/",
            UriKind.Absolute);

        var client = new HttpClient(handler, disposeHandler: true)
        {
            BaseAddress = baseUri,
            Timeout = Timeout.InfiniteTimeSpan,
        };

        AuthenticationHeaderValue? authorization = null;
        if (usernameReference is not null && passwordReference is not null)
        {
            var username = secretResolver.Resolve(usernameReference).Reveal();
            var password = secretResolver.Resolve(passwordReference).Reveal();
            authorization = new AuthenticationHeaderValue(
                "Basic",
                Convert.ToBase64String(Encoding.UTF8.GetBytes($"{username}:{password}")));
        }

        return new HttpReadRuntime(client, authorization);
    }

    public static async Task<JsonElement> GetJsonAsync(
        HttpReadRuntime runtime,
        string relativePath,
        long maxBytes,
        CancellationToken cancellationToken)
    {
        using var request = new HttpRequestMessage(HttpMethod.Get, relativePath);
        request.Headers.Accept.ParseAdd("application/json");
        if (runtime.Authorization is not null)
        {
            request.Headers.Authorization = runtime.Authorization;
        }

        using var response = await runtime.Client.SendAsync(
                request,
                HttpCompletionOption.ResponseHeadersRead,
                cancellationToken)
            .ConfigureAwait(false);

        if ((int)response.StatusCode is >= 300 and < 400)
        {
            throw new ReadViewHttpException(
                ReadViewFailureCategory.InvalidResponse,
                "upstream_redirect_rejected",
                "External read endpoint redirect was rejected.",
                false);
        }

        if (response.StatusCode == HttpStatusCode.NotFound)
        {
            throw new ReadViewHttpException(
                ReadViewFailureCategory.Unavailable,
                "upstream_resource_not_found",
                "External read endpoint did not contain the requested resource.",
                false);
        }

        if (response.StatusCode is HttpStatusCode.Unauthorized or HttpStatusCode.Forbidden)
        {
            throw new ReadViewHttpException(
                ReadViewFailureCategory.Unauthorized,
                "upstream_authorization_denied",
                "External read endpoint denied the requested operation.",
                false);
        }

        if ((int)response.StatusCode >= 500)
        {
            throw new ReadViewHttpException(
                ReadViewFailureCategory.Unavailable,
                "upstream_unavailable",
                "External read endpoint is temporarily unavailable.",
                true);
        }

        if (!response.IsSuccessStatusCode)
        {
            throw new ReadViewHttpException(
                ReadViewFailureCategory.InvalidResponse,
                "upstream_request_failed",
                "External read endpoint rejected the requested operation.",
                false);
        }

        var bytes = await ReadBoundedAsync(response.Content, maxBytes, cancellationToken)
            .ConfigureAwait(false);

        using var document = JsonDocument.Parse(bytes);
        return document.RootElement.Clone();
    }

    private static async Task<byte[]> ReadBoundedAsync(
        HttpContent content,
        long maxBytes,
        CancellationToken cancellationToken)
    {
        if (content.Headers.ContentLength is long declared && declared > maxBytes)
        {
            throw new ResponseBoundExceededException();
        }

        await using var stream = await content.ReadAsStreamAsync(cancellationToken)
            .ConfigureAwait(false);

        using var buffer = new MemoryStream();
        var chunk = new byte[16 * 1024];

        while (true)
        {
            var read = await stream.ReadAsync(chunk.AsMemory(), cancellationToken).ConfigureAwait(false);
            if (read == 0)
            {
                break;
            }

            if (buffer.Length + read > maxBytes)
            {
                throw new ResponseBoundExceededException();
            }

            buffer.Write(chunk, 0, read);
        }

        return buffer.ToArray();
    }
}
