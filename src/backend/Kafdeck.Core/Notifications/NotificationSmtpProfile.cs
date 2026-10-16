namespace Kafdeck.Core.Notifications;

public sealed record NotificationSmtpProfile
{
    public NotificationSmtpProfile(string host, string senderAddress, string username, int port = 465)
    {
        if (host.Length is < 1 or > 253 || !host.All(ch => char.IsAsciiLetterOrDigit(ch) || ch is '.' or '-') ||
            port is < 1 or > 65535 || !IsMailbox(senderAddress) || username.Length is < 1 or > 128 ||
            !username.All(ch => char.IsAsciiLetterOrDigit(ch) || ch is '@' or '.' or '_' or '-' or '+'))
            throw new ArgumentException("SMTP requires a closed host, port, exact sender and SASL username.");
        Endpoint = NotificationHttpsEndpointPolicy.ValidateAndNormalize(new UriBuilder("https", host, port, "smtp").Uri);
        SenderAddress = senderAddress;
        Username = username;
    }
    public Uri Endpoint { get; }
    public string SenderAddress { get; }
    public string Username { get; }
    public override string ToString() => "[server-owned-smtp-profile]";

    public static bool IsMailbox(string? value)
    {
        if (value is null || value.Length is < 3 or > 254 || value.Count(ch => ch == '@') != 1) return false;
        var pieces = value.Split('@');
        return pieces[0].Length is > 0 and <= 64 && pieces[1].Length > 0 &&
            pieces[0].All(ch => char.IsAsciiLetterOrDigit(ch) || ch is '.' or '_' or '-' or '+') &&
            pieces[1].All(ch => char.IsAsciiLetterOrDigit(ch) || ch is '.' or '-');
    }
}
