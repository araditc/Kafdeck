using System.Text.Json;

namespace Kafdeck.Modules.Administration;

/// <summary>
/// Closed W43 binding between the durable common mutation aggregate and the
/// safe SCRAM plan. It never accepts caller-supplied alternate targets.
/// </summary>
public static class ScramMutationContract
{
    private static readonly JsonSerializerOptions CanonicalJson = new()
    {
        PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
        PropertyNameCaseInsensitive = false,
        WriteIndented = false,
    };

    public static ScramMutationPlan ValidateBoundOperation(
        MutationOperationSnapshot operation,
        bool requireReadyForFinalization = false)
    {
        ArgumentNullException.ThrowIfNull(operation);
        if (operation.OperationKind != MutationOperationKind.ScramAlter)
        {
            throw new MutationStateException(
                "SCRAM contract requires the exact ScramAlter operation kind.");
        }

        ScramMutationPlan plan;
        try
        {
            plan = JsonSerializer.Deserialize<ScramMutationPlan>(
                       operation.CanonicalIntent,
                       CanonicalJson) ??
                   throw Invalid();
        }
        catch (JsonException)
        {
            throw Invalid();
        }

        if (!Enum.IsDefined(plan.Mode))
        {
            throw Invalid();
        }

        var preview = ScramMutationPlanner.NormalizePreviewBinding(
            plan.PreviewBinding,
            requireDigestKey: plan.Mode == ScramMutationMode.Upsert);
        var credential = ScramCredentialMaterialBinding.Normalize(
            plan.Credential);

        if (preview.OperationId != operation.OperationId ||
            !string.Equals(
                preview.RequesterPrincipalId,
                operation.RequesterPrincipalId,
                StringComparison.Ordinal) ||
            !string.Equals(
                preview.PolicyVersion,
                operation.PolicyVersion,
                StringComparison.Ordinal) ||
            !string.Equals(
                credential.ClusterId,
                operation.ClusterId,
                StringComparison.Ordinal))
        {
            throw new MutationStateException(
                "SCRAM preview binding does not match the durable mutation identity.");
        }

        if (operation.Risk.RiskClass != MutationRiskClass.Critical ||
            !operation.Risk.RequiresIndependentApproval ||
            operation.Risk.ConfirmationMode != MutationConfirmationMode.TypedTarget)
        {
            throw new MutationStateException(
                "SCRAM mutations require the server-owned CRITICAL risk contract.");
        }

        var resource = ConflictResource(credential);
        if (operation.ResourceKeys.Count != 1 ||
            !string.Equals(
                operation.ResourceKeys[0],
                resource,
                StringComparison.Ordinal))
        {
            throw new MutationStateException(
                "SCRAM durable resource identity does not match the exact credential target.");
        }

        var requirements = FleetMutationAuthorization.NormalizeRequirements(
            MutationOperationKind.ScramAlter,
            new[]
            {
                new MutationAuthorizationTarget(
                    AuthorizationAction.ScramRead,
                    credential.ClusterId,
                    resource),
                new MutationAuthorizationTarget(
                    AuthorizationAction.ScramAlter,
                    credential.ClusterId,
                    resource),
            });

        if (!operation.AuthorizationTargets.SequenceEqual(requirements))
        {
            throw new MutationStateException(
                "SCRAM durable authorization conjunction does not match the exact credential target.");
        }

        var metadataPreconditions = operation.Preconditions
            .Where(item =>
                string.Equals(
                    item.Key,
                    "scram.metadata",
                    StringComparison.Ordinal))
            .Take(2)
            .ToArray();
        if (metadataPreconditions.Length != 1 ||
            !IsSha256(plan.ObservedMetadataFingerprint) ||
            !string.Equals(
                metadataPreconditions[0].Fingerprint,
                plan.ObservedMetadataFingerprint,
                StringComparison.Ordinal))
        {
            throw new MutationStateException(
                "SCRAM metadata precondition is missing or does not match the admitted plan.");
        }

        switch (plan.Mode)
        {
            case ScramMutationMode.Upsert:
                if (!string.Equals(
                        plan.MaterialName,
                        ScramMutationPlanner.MaterialName,
                        StringComparison.Ordinal) ||
                    operation.MaterialDigests.Count != 1 ||
                    !string.Equals(
                        operation.MaterialDigests[0].Name,
                        ScramMutationPlanner.MaterialName,
                        StringComparison.Ordinal) ||
                    string.IsNullOrWhiteSpace(preview.DigestKeyId))
                {
                    throw new MutationStateException(
                        "SCRAM upsert requires exactly one bound ephemeral material digest.");
                }
                break;

            case ScramMutationMode.Delete:
                if (plan.MaterialName is not null ||
                    operation.MaterialDigests.Count != 0)
                {
                    throw new MutationStateException(
                        "SCRAM delete must not persist or request credential material.");
                }
                break;

            default:
                throw Invalid();
        }

        if (requireReadyForFinalization)
        {
            RequireReadyIndependentApproval(operation);
        }

        return plan with
        {
            PreviewBinding = preview,
            Credential = credential,
        };
    }

    public static ScramCredentialMaterialBindingContext
        BuildUpsertMaterialContext(
            MutationOperationSnapshot operation,
            ScramMutationPlan plan)
    {
        ArgumentNullException.ThrowIfNull(operation);
        ArgumentNullException.ThrowIfNull(plan);
        if (plan.Mode != ScramMutationMode.Upsert ||
            string.IsNullOrWhiteSpace(plan.PreviewBinding.DigestKeyId))
        {
            throw new MutationStateException(
                "SCRAM execution material is available only for an admitted upsert.");
        }

        return ScramCredentialMaterialBinding.Normalize(
            new ScramCredentialMaterialBindingContext(
                operation.OperationId,
                operation.RequesterPrincipalId,
                operation.PolicyVersion,
                plan.PreviewBinding.DigestKeyId!,
                plan.Credential));
    }

    public static string ConflictResource(
        ScramCredentialBindingDescriptor credential)
    {
        var normalized = ScramCredentialMaterialBinding.Normalize(credential);
        return FleetConflictKeyCodec.Encode(
            new FleetConflictTarget(
                FleetConflictTargetKind.ScramCredential,
                normalized.ClusterId,
                normalized.User,
                ((int)normalized.Mechanism).ToString(
                    System.Globalization.CultureInfo.InvariantCulture)));
    }

    private static void RequireReadyIndependentApproval(
        MutationOperationSnapshot operation)
    {
        if (operation.State != MutationOperationState.Ready ||
            operation.ConfirmedAtUtc is null ||
            !string.Equals(
                operation.ConfirmedByPrincipalId,
                operation.RequesterPrincipalId,
                StringComparison.Ordinal) ||
            operation.ApprovedAtUtc is null ||
            string.IsNullOrWhiteSpace(operation.ApprovedByPrincipalId) ||
            string.IsNullOrWhiteSpace(
                operation.ApprovalAuthorizationEvidenceHash) ||
            string.Equals(
                operation.ApprovedByPrincipalId,
                operation.RequesterPrincipalId,
                StringComparison.Ordinal))
        {
            throw new MutationStateException(
                "SCRAM finalization requires requester confirmation and a distinct durable independent approval.");
        }
    }

    private static bool IsSha256(string value) =>
        value.Length == 64 &&
        value.All(char.IsAsciiHexDigit);

    private static MutationStateException Invalid() =>
        new("SCRAM canonical intent is invalid or unsupported.");
}
