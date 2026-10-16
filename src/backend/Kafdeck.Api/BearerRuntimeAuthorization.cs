using Kafdeck.Core.Security;
using Kafdeck.Modules.Administration;

namespace Kafdeck.Api;

/// <summary>Stored attribution is not authentication. Background effects require current exact registration and direct RBAC.</summary>
internal sealed record RegisteredBearerPrincipal(NonBrowserPrincipalKind Kind, OperatorIdentityKey Subject, string ClientId, string RegistrationId)
{
    public static bool HasBearerPrefix(string value) => value.StartsWith("oidc-cli-human:", StringComparison.Ordinal) || value.StartsWith("oidc-service:", StringComparison.Ordinal);
    public static bool TryParse(string value, out RegisteredBearerPrincipal? principal)
    {
        principal = null;
        var kind = value.StartsWith("oidc-cli-human:", StringComparison.Ordinal) ? NonBrowserPrincipalKind.Human : NonBrowserPrincipalKind.Service;
        var prefix = kind == NonBrowserPrincipalKind.Human ? "oidc-cli-human:" : "oidc-service:";
        if (value.Length > 4096 || !value.StartsWith(prefix, StringComparison.Ordinal)) return false;
        var fields = value[prefix.Length..].Split('|');
        if (fields.Length != 4) return false;
        try
        {
            var decoded = fields.Select(Uri.UnescapeDataString).ToArray();
            if (decoded.Any(v => string.IsNullOrWhiteSpace(v) || v != v.Trim() || v.Any(char.IsControl)) ||
                !fields.SequenceEqual(decoded.Select(Uri.EscapeDataString), StringComparer.Ordinal)) return false;
            principal = new(kind, new(decoded[0], decoded[1]), decoded[2], decoded[3]);
            return true;
        }
        catch (ArgumentException) { return false; }
    }
}

public sealed class BearerRuntimeAuthorization(NonBrowserReadRegistry registry, AuthorizationPolicyEvaluator evaluator, KafdeckAuthorizationService authorization)
{
    public bool IsPrincipalAllowed(string principal, AuthorizationRequest request) => registry.IsCurrentPrincipalAllowed(principal, request, evaluator);
    public bool IsRequesterAllowed(MutationOperationSnapshot operation) =>
        BearerFamilyRoutes.IsKind(operation.OperationKind) && operation.AuthorizationTargets.Count > 0 &&
        operation.PolicyVersion == "v0.8-bearer:" + evaluator.Fingerprint &&
        operation.AuthorizationTargets.All(target => IsPrincipalAllowed(operation.RequesterPrincipalId, new(target.Action, target.ClusterId, target.ResourceName)));

    public bool IsApproverAllowed(MutationOperationSnapshot operation)
    {
        if (!operation.Risk.RequiresIndependentApproval) return true;
        if (string.IsNullOrWhiteSpace(operation.ApprovedByPrincipalId) || operation.ApprovalAuthorizationEvidenceHash is null ||
            operation.ApprovedAtUtc is null || operation.AuthorizationTargets.Count == 0) return false;
        OperatorIdentityKey? human = null;
        if (NonBrowserHumanPrincipalId.TryParse(operation.ApprovedByPrincipalId, out var approver)) human = approver!.HumanKey;
        if (human is not null && MutationActor.IsSameHuman(operation.RequesterPrincipalId, human)) return false;
        return operation.ApprovedByPrincipalId != operation.RequesterPrincipalId && operation.AuthorizationTargets.All(target =>
            authorization.IsCurrentApprovalAllowed(operation.ApprovedByPrincipalId, new(target.Action, target.ClusterId, target.ResourceName)));
    }
}
