using System.Globalization;
using System.Net.Sockets;
using System.Security.Authentication;
using System.Security.Cryptography;
using System.Text;
using Kafdeck.Core.Notifications;

namespace Kafdeck.Infrastructure.Configuration;

/// <summary>Closed implicit-TLS SMTP submission with mandatory SASL PLAIN.
/// No STARTTLS/cleartext fallback, arbitrary commands, recipients or headers.</summary>
public sealed class NotificationPinnedSmtpTransport(NotificationDestinationProfile profile,
    INotificationEndpointResolutionPort resolver, NotificationPinnedHttpHandlerFactory connections,
    NotificationDeliveryPolicy? policy = null) : INotificationEmailTransport
{
    public async Task<NotificationProviderTransportResult> SendAsync(NotificationEmailTransportRequest request, CancellationToken token)
    {
        token.ThrowIfCancellationRequested();
        var smtp = profile.Smtp;
        var notification = request.Event;
        if (!profile.Enabled || profile.Provider != NotificationProviderKind.Email || smtp is null ||
            profile.CredentialBindingId is null || notification?.IsApprovedForDurability != true ||
            !profile.EnabledEvents.Contains(notification.EventClass) ||
            !string.Equals(request.DestinationId, profile.DestinationId, StringComparison.Ordinal) ||
            !string.Equals(request.ProfileRevisionFingerprint, profile.RevisionFingerprint, StringComparison.Ordinal) ||
            !string.Equals(request.RecipientAddress, profile.EmailRecipientAddress, StringComparison.Ordinal) ||
            !NotificationSmtpProfile.IsMailbox(request.RecipientAddress) ||
            request.Subject != $"[{notification.EventClass}] {notification.Subject}" ||
            request.Body != string.Join("\n", notification.EventType, notification.Summary,
                notification.OccurredAtUtc.ToString("O", CultureInfo.InvariantCulture), notification.EventId.ToString("D")))
            throw new KafdeckConfigurationException("SMTP request does not match its exact approved profile and safe event.");
        var password = request.Credential.Reveal();
        if (Encoding.UTF8.GetByteCount(password) > 256 || password.Any(char.IsControl))
            throw new KafdeckConfigurationException("SMTP credential exceeds its admitted SASL format.");
        var authBytes = Encoding.UTF8.GetBytes("\0" + smtp.Username + "\0" + password);
        var auth = "AUTH PLAIN " + Convert.ToBase64String(authBytes) + "\r\n";
        Array.Clear(authBytes);
        if (auth.Length > 512 || Encoding.UTF8.GetByteCount(request.Subject) > 256)
            throw new KafdeckConfigurationException("SMTP command or subject exceeds its finite budget.");
        var encodedBody = Convert.ToBase64String(Encoding.UTF8.GetBytes(request.Body), Base64FormattingOptions.InsertLineBreaks);
        var message = Encoding.ASCII.GetBytes("From: <" + smtp.SenderAddress + ">\r\nTo: <" + request.RecipientAddress +
            ">\r\nSubject: =?utf-8?B?" + Convert.ToBase64String(Encoding.UTF8.GetBytes(request.Subject)) +
            "?=\r\nDate: " + notification.OccurredAtUtc.ToUniversalTime().ToString("r", CultureInfo.InvariantCulture) +
            "\r\nMessage-ID: <" + notification.EventId.ToString("D") + "@kafdeck.invalid>\r\nMIME-Version: 1.0\r\nContent-Type: text/plain; charset=utf-8\r\nContent-Transfer-Encoding: base64\r\n\r\n" + encodedBody + "\r\n.\r\n");
        if (message.Length > (policy ?? new NotificationDeliveryPolicy()).MaxPayloadBytes)
            throw new KafdeckConfigurationException("SMTP message exceeds the delivery payload budget.");
        using var deadline = CancellationTokenSource.CreateLinkedTokenSource(token);
        deadline.CancelAfter(TimeSpan.FromSeconds(15));
        token = deadline.Token;
        try
        {
            var pin = await resolver.ResolveForConnectionAsync(smtp.Endpoint, token).ConfigureAwait(false);
            token.ThrowIfCancellationRequested();
            if (!string.Equals(pin.Endpoint.AbsoluteUri, smtp.Endpoint.AbsoluteUri, StringComparison.Ordinal))
                throw new KafdeckConfigurationException("SMTP resolver substituted its approved server identity.");
            await using var tls = await connections.OpenPinnedTlsStreamAsync(pin, token, profile).ConfigureAwait(false);
            var reader = new BoundedLineReader(tls);
            var greeting = await ReplyAsync(reader, token).ConfigureAwait(false);
            if (greeting.Code != 220) return Rejection(greeting.Code);
            await CommandAsync(tls, "EHLO kafdeck.invalid\r\n", token).ConfigureAwait(false);
            var ehlo = await ReplyAsync(reader, token).ConfigureAwait(false);
            if (ehlo.Code != 250) return Rejection(ehlo.Code);
            var plain = ehlo.Lines.Any(line => line.StartsWith("AUTH ", StringComparison.OrdinalIgnoreCase) &&
                line.Split(' ', StringSplitOptions.RemoveEmptyEntries).Skip(1).Any(mechanism => mechanism.Equals("PLAIN", StringComparison.OrdinalIgnoreCase)));
            if (!plain) return Rejection(535);
            await connections.VerifySendAdmissionAsync(token, profile).ConfigureAwait(false);
            await CommandAsync(tls, auth, token).ConfigureAwait(false);
            var authenticated = await ReplyAsync(reader, token).ConfigureAwait(false);
            if (authenticated.Code != 235) return Rejection(authenticated.Code);
            await CommandAsync(tls, "MAIL FROM:<" + smtp.SenderAddress + ">\r\n", token).ConfigureAwait(false);
            var sender = await ReplyAsync(reader, token).ConfigureAwait(false);
            if (sender.Code != 250) return Rejection(sender.Code);
            await CommandAsync(tls, "RCPT TO:<" + request.RecipientAddress + ">\r\n", token).ConfigureAwait(false);
            var recipient = await ReplyAsync(reader, token).ConfigureAwait(false);
            if (recipient.Code is not (250 or 251)) return Rejection(recipient.Code);
            await connections.VerifySendAdmissionAsync(token, profile).ConfigureAwait(false);
            await CommandAsync(tls, "DATA\r\n", token).ConfigureAwait(false);
            var data = await ReplyAsync(reader, token).ConfigureAwait(false);
            if (data.Code != 354) return Rejection(data.Code);
            await connections.VerifySendAdmissionAsync(token, profile).ConfigureAwait(false);
            await tls.WriteAsync(message, token).ConfigureAwait(false);
            var receipt = await ReplyAsync(reader, token).ConfigureAwait(false);
            if (receipt.Code != 250) return Rejection(receipt.Code);
            return new NotificationProviderTransportResult(NotificationProviderTransportOutcome.Delivered,
                "smtp_receipt_ack", Convert.ToHexString(SHA256.HashData(Encoding.UTF8.GetBytes(
                    "smtp\n" + string.Join("\n", receipt.Lines)))).ToLowerInvariant());
        }
        catch (Exception error) when (error is IOException or SocketException or AuthenticationException or OperationCanceledException or KafdeckConfigurationException)
        {
            return new NotificationProviderTransportResult(NotificationProviderTransportOutcome.UnknownExternalEffect, "smtp_effect_ambiguous");
        }
    }

    private static Task CommandAsync(Stream stream, string command, CancellationToken token) =>
        stream.WriteAsync(Encoding.ASCII.GetBytes(command), token).AsTask();

    private static async Task<(int Code, IReadOnlyList<string> Lines)> ReplyAsync(BoundedLineReader reader, CancellationToken token)
    {
        var lines = new List<string>();
        var code = 0;
        for (var count = 0; count < 16; count++)
        {
            var line = await reader.ReadAsync(512, token).ConfigureAwait(false);
            if (line.Length < 4 || !int.TryParse(line.AsSpan(0, 3), NumberStyles.None, CultureInfo.InvariantCulture, out var parsed) ||
                parsed is < 200 or > 599 || (count != 0 && parsed != code) || line[3] is not (' ' or '-'))
                throw new IOException("Invalid bounded SMTP reply.");
            code = parsed;
            lines.Add(line[4..]);
            if (line[3] == ' ') return (code, lines);
        }
        throw new IOException("SMTP multiline reply exceeds finite budget.");
    }
    private static NotificationProviderTransportResult Rejection(int code) =>
        new(code is >= 500 and <= 599 ? NotificationProviderTransportOutcome.PermanentFailure :
            NotificationProviderTransportOutcome.UnknownExternalEffect,
            code is >= 500 and <= 599 ? "smtp_definitive_reject" : "smtp_effect_ambiguous");
}
