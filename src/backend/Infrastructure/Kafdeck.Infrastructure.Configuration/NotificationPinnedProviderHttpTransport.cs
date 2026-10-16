using System.Net;
using System.Net.Http.Headers;
using System.Net.Sockets;
using System.Security.Authentication;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using Kafdeck.Core.Notifications;

namespace Kafdeck.Infrastructure.Configuration;

/// <summary>
/// Concrete closed Slack, Graph Teams, Telegram and PagerDuty HTTP protocols.
/// All origins/methods/headers are code-owned; profiles own only typed targets.
/// Provider response material never escapes except a one-way receipt digest.
/// </summary>
public sealed class NotificationPinnedProviderHttpTransport
{
    private const int MaxResponseBytes = 16 * 1024;
    private readonly NotificationDestinationProfile _profile;
    private readonly INotificationEndpointResolutionPort _resolver;
    private readonly NotificationPinnedHttpHandlerFactory _handlers;
    private readonly NotificationDeliveryPolicy _policy;

    public NotificationPinnedProviderHttpTransport(NotificationDestinationProfile profile,
        INotificationEndpointResolutionPort resolver, NotificationPinnedHttpHandlerFactory handlers,
        NotificationDeliveryPolicy? policy = null)
    {
        if (!profile.Enabled || profile.CredentialBindingId is null ||
            !NotificationHttpProviderTargetPolicy.IsValid(profile.Provider, profile.ProviderResourceId, profile.ProviderSecondaryResourceId))
            throw new KafdeckConfigurationException("Physical provider requires an enabled revision-bound typed HTTP destination.");
        _profile = profile;
        _resolver = resolver;
        _handlers = handlers;
        _policy = policy ?? new NotificationDeliveryPolicy();
    }

    public async Task<NotificationProviderTransportResult> SendAsync(NotificationBoundTransportRequest request,
        CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        using var deadline = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        deadline.CancelAfter(TimeSpan.FromSeconds(15));
        cancellationToken = deadline.Token;
        if (request.Provider != _profile.Provider || !request.Event.IsApprovedForDurability ||
            !_profile.EnabledEvents.Contains(request.Event.EventClass) ||
            !string.Equals(request.DestinationId, _profile.DestinationId, StringComparison.Ordinal) ||
            !string.Equals(request.ProfileRevisionFingerprint, _profile.RevisionFingerprint, StringComparison.Ordinal))
            throw new KafdeckConfigurationException("Provider request does not match the approved destination revision and event.");

        var credential = request.Credential.Reveal();
        var text = $"[{request.Event.EventClass}] {request.Event.Subject}\n{request.Event.Summary}\n{request.Event.OccurredAtUtc:O}\nEvent {request.Event.EventId:D}";
        var dedup = request.Event.EventId.ToString("D");
        Uri endpoint;
        byte[] body;
        var bearer = false;
        switch (_profile.Provider)
        {
            case NotificationProviderKind.Slack:
                bearer = true;
                endpoint = new Uri("https://slack.com/api/chat.postMessage");
                body = JsonSerializer.SerializeToUtf8Bytes(new { channel = _profile.ProviderResourceId, text,
                    mrkdwn = false, parse = "none", unfurl_links = false, unfurl_media = false,
                    client_msg_id = dedup });
                break;
            case NotificationProviderKind.MicrosoftTeams:
                bearer = true;
                endpoint = new Uri("https://graph.microsoft.com/v1.0/teams/" +
                    Uri.EscapeDataString(_profile.ProviderResourceId!) + "/channels/" +
                    Uri.EscapeDataString(_profile.ProviderSecondaryResourceId!) + "/messages");
                body = JsonSerializer.SerializeToUtf8Bytes(new { body = new { contentType = "text", content = text } });
                break;
            case NotificationProviderKind.Telegram:
                var separator = credential.IndexOf(':');
                if (credential.Length is < 22 or > 200 || separator is < 1 or > 20 ||
                    !credential[..separator].All(char.IsAsciiDigit) ||
                    !credential[(separator + 1)..].All(ch => char.IsAsciiLetterOrDigit(ch) || ch is '-' or '_'))
                    throw new KafdeckConfigurationException("Telegram bot credential is invalid.");
                endpoint = new Uri("https://api.telegram.org/bot" + credential + "/sendMessage");
                body = JsonSerializer.SerializeToUtf8Bytes(new { chat_id = long.Parse(_profile.ProviderResourceId!, System.Globalization.CultureInfo.InvariantCulture), text,
                    link_preview_options = new { is_disabled = true } });
                break;
            case NotificationProviderKind.PagerDuty:
                if (credential.Length != 32 || !credential.All(Uri.IsHexDigit))
                    throw new KafdeckConfigurationException("PagerDuty routing credential is invalid.");
                endpoint = new Uri("https://events.pagerduty.com/v2/enqueue");
                body = JsonSerializer.SerializeToUtf8Bytes(new { routing_key = credential, event_action = "trigger",
                    dedup_key = dedup, client = "Kafdeck", payload = new { summary = text, source = "Kafdeck",
                        severity = "warning", timestamp = request.Event.OccurredAtUtc.ToString("O") } });
                break;
            default:
                throw new KafdeckConfigurationException("Provider HTTP protocol is not admitted.");
        }
        if (bearer && !NotificationWebhookHttpRequestFactory.IsValidBearerCredential(credential))
            throw new KafdeckConfigurationException("Provider Bearer credential is invalid.");
        if (body.Length > _policy.MaxPayloadBytes)
            throw new KafdeckConfigurationException("Provider payload exceeds the admitted delivery budget.");

        var pinned = await _resolver.ResolveForConnectionAsync(endpoint, cancellationToken).ConfigureAwait(false);
        cancellationToken.ThrowIfCancellationRequested();
        if (!string.Equals(pinned.Endpoint.AbsoluteUri, endpoint.AbsoluteUri, StringComparison.Ordinal))
            throw new KafdeckConfigurationException("Provider resolver substituted its fixed destination.");
        if (_profile.Provider == NotificationProviderKind.Telegram)
        {
            try
            {
                var response = await NotificationPrivateHttpWire.PostAsync(_handlers, pinned, body, cancellationToken, profile: _profile).ConfigureAwait(false);
                if (response.Status is 400 or 401 or 403 or 404 or 405 or 410 or 413 or 415 or 422)
                    return Result(NotificationProviderTransportOutcome.PermanentFailure, "provider_definitive_reject");
                if (response.Status is < 200 or >= 300) return Unknown();
                using var document = JsonDocument.Parse(response.Body, new JsonDocumentOptions { MaxDepth = 8 });
                return ParseReceipt(response.Status, document.RootElement, dedup);
            }
            catch (Exception error) when (error is IOException or SocketException or AuthenticationException or OperationCanceledException or JsonException or KafdeckConfigurationException)
            { return Unknown(); }
        }
        using var handler = _handlers.CreateGuardedHandler(pinned, _profile);
        using var client = new HttpClient(handler, disposeHandler: false) { Timeout = TimeSpan.FromSeconds(15) };
        using var message = new HttpRequestMessage(HttpMethod.Post, endpoint)
        {
            Version = HttpVersion.Version11, VersionPolicy = HttpVersionPolicy.RequestVersionExact,
            Content = new ByteArrayContent(body),
        };
        message.Content.Headers.ContentType = new MediaTypeHeaderValue("application/json");
        if (bearer) message.Headers.Authorization = new AuthenticationHeaderValue("Bearer", credential);
        try
        {
            // These HttpClient paths contain no credentials. Telegram bypasses
            // HTTP diagnostics above because its API requires a secret URL.
            // PagerDuty requires routing_key in the private body; never log
            // bodies/headers/exceptions or persist those materials.
            using var response = await client.SendAsync(message, HttpCompletionOption.ResponseHeadersRead, cancellationToken)
                .ConfigureAwait(false);
            var status = (int)response.StatusCode;
            if (status is 400 or 401 or 403 or 404 or 405 or 410 or 413 or 415 or 422)
                return Result(NotificationProviderTransportOutcome.PermanentFailure, "provider_definitive_reject");
            if (status is < 200 or >= 300)
                return Unknown();
            if (response.Content.Headers.ContentLength > MaxResponseBytes) return Unknown();
            await using var stream = await response.Content.ReadAsStreamAsync(cancellationToken).ConfigureAwait(false);
            var buffer = new byte[MaxResponseBytes + 1];
            var count = 0;
            while (count < buffer.Length)
            {
                var read = await stream.ReadAsync(buffer.AsMemory(count), cancellationToken).ConfigureAwait(false);
                if (read == 0) break;
                count += read;
            }
            if (count > MaxResponseBytes) return Unknown();
            using var document = JsonDocument.Parse(buffer.AsMemory(0, count), new JsonDocumentOptions { MaxDepth = 8 });
            return ParseReceipt(status, document.RootElement, dedup);
        }
        catch (Exception error) when (error is HttpRequestException or IOException or OperationCanceledException or JsonException or KafdeckConfigurationException)
        {
            return Unknown();
        }
    }

    private NotificationProviderTransportResult ParseReceipt(int status, JsonElement root, string dedup)
    {
        string? receipt = null;
        switch (_profile.Provider)
        {
            case NotificationProviderKind.Slack when status == 200:
                if (!Property(root, "ok", out var slackOk)) break;
                if (slackOk.ValueKind == JsonValueKind.False && Property(root, "error", out var error) &&
                    error.ValueKind == JsonValueKind.String && error.GetString() is "invalid_auth" or "not_authed" or "token_revoked" or
                        "account_inactive" or "channel_not_found" or "not_in_channel" or "missing_scope")
                    return Result(NotificationProviderTransportOutcome.PermanentFailure, "provider_definitive_reject");
                if (slackOk.ValueKind == JsonValueKind.True && Property(root, "channel", out var channel) &&
                    channel.ValueKind == JsonValueKind.String && channel.GetString() == _profile.ProviderResourceId &&
                    Property(root, "ts", out var timestamp) && SafeString(timestamp, out receipt)) { }
                break;
            case NotificationProviderKind.MicrosoftTeams when status == 201:
                if (Property(root, "id", out var id)) SafeString(id, out receipt);
                break;
            case NotificationProviderKind.Telegram when status == 200:
                if (Property(root, "ok", out var failed) && failed.ValueKind == JsonValueKind.False &&
                    Property(root, "error_code", out var rejected) && rejected.ValueKind == JsonValueKind.Number &&
                    rejected.TryGetInt32(out var code) && code is 400 or 401 or 403 or 404)
                    return Result(NotificationProviderTransportOutcome.PermanentFailure, "provider_definitive_reject");
                if (Property(root, "ok", out var telegramOk) && telegramOk.ValueKind == JsonValueKind.True &&
                    Property(root, "result", out var result) && Property(result, "message_id", out var messageId) &&
                    messageId.ValueKind == JsonValueKind.Number && messageId.TryGetInt64(out var numericId) && numericId > 0 && Property(result, "chat", out var chat) &&
                    Property(chat, "id", out var chatId) && chatId.ValueKind == JsonValueKind.Number && chatId.TryGetInt64(out var numericChat) &&
                    numericChat.ToString(System.Globalization.CultureInfo.InvariantCulture) == _profile.ProviderResourceId)
                    receipt = numericId.ToString(System.Globalization.CultureInfo.InvariantCulture);
                break;
            case NotificationProviderKind.PagerDuty when status == 202:
                if (Property(root, "status", out var state) && state.ValueKind == JsonValueKind.String && state.GetString() == "success" &&
                    Property(root, "dedup_key", out var key) && key.ValueKind == JsonValueKind.String && key.GetString() == dedup)
                    receipt = dedup;
                break;
        }
        return receipt is null ? Unknown() : new NotificationProviderTransportResult(NotificationProviderTransportOutcome.Delivered,
            "provider_receipt_ack", Convert.ToHexString(SHA256.HashData(Encoding.UTF8.GetBytes(
                _profile.Provider + "\n" + receipt))).ToLowerInvariant());
    }

    private static bool Property(JsonElement root, string name, out JsonElement value)
    {
        value = default;
        if (root.ValueKind != JsonValueKind.Object) return false;
        var count = 0;
        foreach (var property in root.EnumerateObject())
            if (property.NameEquals(name)) { value = property.Value; count++; }
        return count == 1;
    }
    private static bool SafeString(JsonElement value, out string? text)
    {
        text = value.ValueKind == JsonValueKind.String ? value.GetString() : null;
        if (text is not { Length: > 0 and <= 256 } || text.Any(char.IsControl)) { text = null; return false; }
        return true;
    }
    private static NotificationProviderTransportResult Result(NotificationProviderTransportOutcome outcome, string code) => new(outcome, code);
    private static NotificationProviderTransportResult Unknown() => Result(NotificationProviderTransportOutcome.UnknownExternalEffect, "provider_effect_ambiguous");
}
