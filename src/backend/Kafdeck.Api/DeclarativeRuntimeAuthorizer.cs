using System.Security.Claims;
using Kafdeck.Core.Security;
using Kafdeck.Modules.Administration;

namespace Kafdeck.Api;

public sealed class DeclarativeRuntimeOptions
{
    public bool Enabled { get; set; }
    public string DeploymentId { get; set; } = "";
    public Guid DeploymentGeneration { get; set; }
    public string RecoveryAnchorFile { get; set; } = "";
    public string RecoveryAnchorSha256 { get; set; } = "";
    public List<DeclarativeRuntimeRegistration> Registrations { get; set; } = [];
}

/// <summary>Deployment-owned owner/source/controller mapping. Manifest author fields grant no authority.</summary>
public sealed class DeclarativeRuntimeRegistration
{
    public string Id { get; set; } = "";
    public string OwnerId { get; set; } = "";
    public string SourceRef { get; set; } = "";
    public string SourceRevision { get; set; } = "";
    public bool AllowRevisionAdvance { get; set; }
    public string PrincipalId { get; set; } = "";
    public bool Enabled { get; set; }
    public DateTimeOffset ExpiresAtUtc { get; set; }
    public List<string> ResourceIds { get; set; } = [];
}

public enum DeclarativeRuntimeVerb { Read, Plan, Adopt, Apply, SourceManage }

public sealed class DeclarativeRuntimeAuthorizer(IConfiguration configuration, KafdeckAuthorizationService authorization,
    NonBrowserReadRegistry? bearerRegistry = null)
{
    private readonly string _deployment = configuration["Kafdeck:Declarative:DeploymentId"] ?? "";
    private readonly bool _enabled = configuration.GetValue<bool>("Kafdeck:Declarative:Enabled");

    public string PolicyFingerprint => authorization.PolicyFingerprint;
    public string DeploymentId => _deployment;

    /// <summary>Checks an existing immutable attribution against current registrations. It does not authenticate or reconstruct a caller.</summary>
    public bool IsStoredRegistrationCurrent(string principalId, string deploymentId, string ownerId, string sourceRef,
        string sourceRevision, string resourceId, string controllerId, IReadOnlyList<AuthorizationAction>? requiredActions = null)
    {
        try
        {
            var options = configuration.GetSection("Kafdeck:Declarative").Get<DeclarativeRuntimeOptions>() ?? new();
            if (!_enabled || !options.Enabled || deploymentId != _deployment || options.DeploymentId != _deployment ||
                !DeclarativeRecoveryAnchor.Allows(options) || !Hash(sourceRevision) || options.Registrations.Count is < 1 or > 128 ||
                options.Registrations.Any(r => !Valid(r)) || options.Registrations.Select(r => r.Id).Distinct(StringComparer.Ordinal).Count() != options.Registrations.Count)
                return false;
            var registration = options.Registrations.SingleOrDefault(r => r.Id == controllerId && r.PrincipalId == principalId &&
                r.OwnerId == ownerId && r.SourceRef == sourceRef && r.ResourceIds.Contains(resourceId, StringComparer.Ordinal) &&
                (r.SourceRevision == sourceRevision || r.AllowRevisionAdvance));
            if (registration is not { Enabled: true } || registration.ExpiresAtUtc <= DateTimeOffset.UtcNow) return false;
            const string servicePrefix = "oidc-service:";
            if (principalId.StartsWith(servicePrefix, StringComparison.Ordinal) || principalId.StartsWith("oidc-cli-human:", StringComparison.Ordinal))
            {
                var service = principalId.StartsWith(servicePrefix, StringComparison.Ordinal);
                var canonicalHuman = service ? "oidc-cli-human:" + principalId[servicePrefix.Length..] : principalId;
                if (!NonBrowserHumanPrincipalId.TryParse(canonicalHuman, out var parsed)) return false;
                var current = bearerRegistry?.Find(parsed!.HumanKey.Subject, parsed.ClientId, parsed.RegistrationId,
                    service ? NonBrowserPrincipalKind.Service : NonBrowserPrincipalKind.Human);
                if (current is null || current.Issuer != parsed!.HumanKey.Issuer) return false;
                var bearerOptions = configuration.GetSection("Kafdeck:NonBrowserRead").Get<NonBrowserReadOptions>() ?? new();
                var live = (service ? bearerOptions.Registrations : bearerOptions.HumanRegistrations)
                    .SingleOrDefault(r => r.Id == parsed.RegistrationId && r.Subject == parsed.HumanKey.Subject && r.ClientId == parsed.ClientId);
                return live is not null && (requiredActions ?? []).All(action => live.ReadActions.Contains(action) ||
                    bearerRegistry!.Trust.GovernedMutationsEnabled && bearerOptions.GovernedMutationsEnabled && live.MutationActions.Contains(action));
            }
            return principalId.StartsWith("oidc:", StringComparison.Ordinal);
        }
        catch (Exception ex) when (ex is ArgumentException or InvalidOperationException) { return false; }
    }

    public DeclarativeRuntimeRegistration? SourceRegistration(ClaimsPrincipal? principal, string sourceRef, string resourceId)
    {
        try
        {
            if (!MutationActor.TryCreate(principal, out var actor) || actor is null) return null;
            var options = configuration.GetSection("Kafdeck:Declarative").Get<DeclarativeRuntimeOptions>() ?? new();
            var candidate = options.Registrations.SingleOrDefault(r => r.PrincipalId == actor.PrincipalId && r.SourceRef == sourceRef &&
                r.ResourceIds.Contains(resourceId, StringComparer.Ordinal));
            return candidate is null ? null : Registration(principal, _deployment, candidate.OwnerId, sourceRef, candidate.SourceRevision, resourceId);
        }
        catch (Exception ex) when (ex is ArgumentException or InvalidOperationException) { return null; }
    }

    public DeclarativeRuntimeRegistration? Registration(ClaimsPrincipal? principal, string deploymentId, string ownerId,
        string sourceRef, string sourceRevision, string resourceId)
    {
        try
        {
            if (!_enabled || !MutationActor.TryCreate(principal, out var actor) || actor is null) return null;
            if (principal?.Identity is NonBrowserReadIdentity bearer &&
                bearerRegistry?.Find(bearer.Evidence.Subject, bearer.Evidence.ClientId, bearer.RegistrationId, bearer.Kind) is null) return null;
            var options = configuration.GetSection("Kafdeck:Declarative").Get<DeclarativeRuntimeOptions>() ?? new();
            if (!options.Enabled || !Identifier(_deployment) || options.DeploymentId != _deployment || deploymentId != _deployment ||
                !DeclarativeRecoveryAnchor.Allows(options) ||
                options.Registrations.Count is < 1 or > 128 || !Hash(sourceRevision) ||
                options.Registrations.Any(r => !Valid(r)) || options.Registrations.Select(r => r.Id).Distinct(StringComparer.Ordinal).Count() != options.Registrations.Count)
                return null;
            return options.Registrations.SingleOrDefault(r => r.Enabled && r.ExpiresAtUtc > DateTimeOffset.UtcNow &&
                r.PrincipalId == actor.PrincipalId && r.OwnerId == ownerId && r.SourceRef == sourceRef &&
                (r.SourceRevision == sourceRevision || r.AllowRevisionAdvance) && r.ResourceIds.Contains(resourceId, StringComparer.Ordinal));
        }
        catch (Exception ex) when (ex is ArgumentException or InvalidOperationException) { return null; }
    }

    public bool Authorize(ClaimsPrincipal? principal, string deploymentId, string ownerId, string sourceRef,
        string sourceRevision, string resourceId, IEnumerable<string> destinations, DeclarativeRuntimeVerb verb)
    {
        var registration = Registration(principal, deploymentId, ownerId, sourceRef, sourceRevision, resourceId);
        if (registration is null || verb == DeclarativeRuntimeVerb.SourceManage && !registration.AllowRevisionAdvance ||
            !MutationActor.TryCreate(principal, out var actor) || actor is null || !Enum.IsDefined(verb)) return false;
        var resources = destinations.Append(resourceId).Distinct(StringComparer.Ordinal).ToArray();
        if (resources.Length is < 1 or > 3 || resources.Any(r => !Identifier(r))) return false;
        var actions = new List<AuthorizationAction> { verb == DeclarativeRuntimeVerb.SourceManage
            ? AuthorizationAction.DeclarativeSourceManage : AuthorizationAction.DeclarativeRead };
        if (verb is not (DeclarativeRuntimeVerb.Read or DeclarativeRuntimeVerb.SourceManage)) actions.Add(AuthorizationAction.DeclarativePlan);
        if (verb == DeclarativeRuntimeVerb.Adopt) actions.Add(AuthorizationAction.DeclarativeAdopt);
        if (verb == DeclarativeRuntimeVerb.Apply) actions.Add(AuthorizationAction.DeclarativeApply);
        var requests = actions.Select(a => new AuthorizationRequest(a, deploymentId, verb == DeclarativeRuntimeVerb.SourceManage ? sourceRef : resourceId))
            .Concat(resources.Select(r => new AuthorizationRequest(AuthorizationAction.NotificationRead, ResourceName: r)));
        if (verb == DeclarativeRuntimeVerb.Apply)
            requests = requests.Concat(resources.Select(r => new AuthorizationRequest(AuthorizationAction.NotificationManage, ResourceName: r)));
        return requests.All(request => principal?.Identity is NonBrowserReadIdentity
            ? authorization.Authorize(principal, request) == KafdeckAuthorizationOutcome.Allowed
            : authorization.IsDirectSubjectAllowed(actor.PrincipalId, request));
    }

    private static bool Valid(DeclarativeRuntimeRegistration r) => Identifier(r.Id) && Identifier(r.OwnerId) && Identifier(r.SourceRef) &&
        Hash(r.SourceRevision) && !string.IsNullOrWhiteSpace(r.PrincipalId) && r.PrincipalId.Length <= 2048 &&
        !r.PrincipalId.Any(char.IsControl) && r.ResourceIds.Count is >= 1 and <= 128 &&
        r.ResourceIds.All(Identifier) && r.ResourceIds.Distinct(StringComparer.Ordinal).Count() == r.ResourceIds.Count;
    internal static bool Identifier(string value) => value is { Length: >= 1 and <= 128 } &&
        value.All(c => char.IsAsciiLetterOrDigit(c) || c is '.' or '_' or '-');
    internal static bool Hash(string value) => value is { Length: 64 } && value.All(c => c is >= '0' and <= '9' or >= 'a' and <= 'f');
}
