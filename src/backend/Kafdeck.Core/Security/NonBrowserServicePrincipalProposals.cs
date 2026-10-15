namespace Kafdeck.Core.Security;

/// <summary>
/// Planning-only, immutable input for a future machine-principal registration.
/// This is NOT a credential, authenticated identity, active grant or authorization
/// decision. No runtime/API/DI integration is admitted by this contract.
/// </summary>
public sealed class NonBrowserServicePrincipalProposal
{
    private static readonly HashSet<AuthorizationAction> InitialReadCandidates =
    [
        AuthorizationAction.SystemRead,
        AuthorizationAction.ClusterRead,
        AuthorizationAction.BrokerRead,
        AuthorizationAction.TopicList,
        AuthorizationAction.TopicRead,
        AuthorizationAction.ConsumerRead,
        AuthorizationAction.SchemaRead,
        AuthorizationAction.ConnectRead,
        AuthorizationAction.CatalogRead,
        AuthorizationAction.KsqlRead,
        AuthorizationAction.DataQualityRead,
        AuthorizationAction.NotificationRead,
    ];

    public NonBrowserServicePrincipalProposal(
        string registrationId,
        string ownerId,
        string issuer,
        string apiAudience,
        string subject,
        string clientId,
        IReadOnlyList<AuthorizationAction> requestedReadActions)
    {
        RegistrationId = RequireIdentifier(registrationId, nameof(registrationId), 128);
        OwnerId = RequireIdentifier(ownerId, nameof(ownerId), 128);
        Issuer = RequireIssuer(issuer);
        ApiAudience = RequireIdentifier(apiAudience, nameof(apiAudience), 256);
        Subject = RequireIdentifier(subject, nameof(subject), 512);
        ClientId = RequireIdentifier(clientId, nameof(clientId), 256);

        ArgumentNullException.ThrowIfNull(requestedReadActions);
        if (requestedReadActions.Count is < 1 or > 16 ||
            requestedReadActions.Any(action => !InitialReadCandidates.Contains(action)) ||
            requestedReadActions.Distinct().Count() != requestedReadActions.Count)
        {
            throw new ArgumentException(
                "Requested actions must be finite, unique and explicitly supported read candidates.",
                nameof(requestedReadActions));
        }

        RequestedReadActions = Array.AsReadOnly(
            requestedReadActions.OrderBy(action => (int)action).ToArray());
    }

    public string RegistrationId { get; }
    public string OwnerId { get; }
    public string Issuer { get; }
    public string ApiAudience { get; }
    public string Subject { get; }
    public string ClientId { get; }
    public IReadOnlyList<AuthorizationAction> RequestedReadActions { get; }

    private static string RequireIdentifier(string? value, string field, int maxLength)
    {
        if (string.IsNullOrWhiteSpace(value) ||
            value.Length > maxLength ||
            !string.Equals(value, value.Trim(), StringComparison.Ordinal) ||
            value.Any(char.IsControl) ||
            value.Contains('*') ||
            value.Contains('?'))
        {
            throw new ArgumentException("Non-browser registration identity must be bounded and exact.", field);
        }

        return value;
    }

    private static string RequireIssuer(string? issuer)
    {
        if (string.IsNullOrWhiteSpace(issuer) || issuer.Length > 2048 ||
            !string.Equals(issuer, issuer.Trim(), StringComparison.Ordinal) ||
            !Uri.TryCreate(issuer, UriKind.Absolute, out var uri) ||
            uri.Scheme != Uri.UriSchemeHttps ||
            string.IsNullOrWhiteSpace(uri.DnsSafeHost) ||
            !string.IsNullOrEmpty(uri.UserInfo) ||
            !string.IsNullOrEmpty(uri.Query) ||
            !string.IsNullOrEmpty(uri.Fragment) ||
            // Uri.AbsoluteUri adds '/' to authority-only URLs. Preserve the
            // exact issuer claim literal without rejecting that valid shape.
            !(string.Equals(uri.AbsoluteUri, issuer, StringComparison.Ordinal) ||
              (string.Equals(uri.AbsolutePath, "/", StringComparison.Ordinal) &&
               string.Equals(uri.AbsoluteUri, issuer + "/", StringComparison.Ordinal))))
        {
            throw new ArgumentException(
                "Reference issuer must be one exact canonical HTTPS issuer without credentials, query or fragment.",
                nameof(issuer));
        }

        return issuer;
    }
}

/// <summary>
/// Finite, checked proposal set only. It intentionally has NO token lookup,
/// authentication, authorization, role assignment or runtime activation API.
/// Each request still needs separately admitted signed-token verification,
/// current registration/authorization, scope, capability and risk approval.
/// </summary>
public sealed class NonBrowserPrincipalProposalSet
{
    public NonBrowserPrincipalProposalSet(
        IReadOnlyList<NonBrowserServicePrincipalProposal> proposals)
    {
        ArgumentNullException.ThrowIfNull(proposals);
        if (proposals.Count > 128)
        {
            throw new ArgumentOutOfRangeException(
                nameof(proposals), "The registration proposal set exceeds its hard finite limit.");
        }

        var identities = new HashSet<(string Issuer, string Subject, string ClientId)>();
        var registrationIds = new HashSet<string>(StringComparer.Ordinal);
        var snapshot = new List<NonBrowserServicePrincipalProposal>(proposals.Count);
        foreach (var proposal in proposals)
        {
            ArgumentNullException.ThrowIfNull(proposal);
            if (!registrationIds.Add(proposal.RegistrationId) ||
                !identities.Add((proposal.Issuer, proposal.Subject, proposal.ClientId)))
            {
                throw new ArgumentException(
                    "Duplicate registration identifier or exact machine principal identity.",
                    nameof(proposals));
            }

            snapshot.Add(proposal);
        }

        Proposals = snapshot.AsReadOnly();
    }

    public IReadOnlyList<NonBrowserServicePrincipalProposal> Proposals { get; }
}
