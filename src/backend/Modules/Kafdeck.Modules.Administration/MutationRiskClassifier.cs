namespace Kafdeck.Modules.Administration;

public sealed record MutationRiskInput(
    MutationOperationKind OperationKind,
    int TargetCount = 1,
    bool PermanentDelete = false,
    bool DurabilitySensitiveChange = false);

public static class MutationRiskClassifier
{
    public static MutationRiskDecision Classify(MutationRiskInput input)
    {
        ArgumentNullException.ThrowIfNull(input);
        if (input.TargetCount <= 0 || input.TargetCount > MutationLimits.MaxResourceKeys)
        {
            throw new ArgumentOutOfRangeException(
                nameof(input),
                $"Mutation target count must be between 1 and {MutationLimits.MaxResourceKeys}.");
        }

        var risk = BaseRisk(input.OperationKind);
        var reasons = new List<string> { $"operation_floor:{risk.ToString().ToLowerInvariant()}" };

        if (input.OperationKind == MutationOperationKind.SchemaDelete && input.PermanentDelete)
        {
            risk = MutationRiskClass.Critical;
            reasons.Add("permanent_delete");
        }

        if (input.DurabilitySensitiveChange && (int)risk < (int)MutationRiskClass.High)
        {
            risk = MutationRiskClass.High;
            reasons.Add("durability_sensitive_change");
        }

        if (input.TargetCount > 1 && (int)risk < (int)MutationRiskClass.Critical)
        {
            risk = RaiseOne(risk);
            reasons.Add("multiple_targets");
        }

        var confirmation = risk >= MutationRiskClass.High
            ? MutationConfirmationMode.TypedTarget
            : MutationConfirmationMode.Explicit;

        return new MutationRiskDecision(
            risk,
            Array.AsReadOnly(reasons.ToArray()),
            confirmation,
            risk == MutationRiskClass.Critical);
    }

    public static MutationRiskDecision EnforceBuiltInFloor(
        MutationRiskInput input,
        MutationRiskDecision proposed)
    {
        ArgumentNullException.ThrowIfNull(proposed);
        ArgumentNullException.ThrowIfNull(proposed.Reasons);

        if (!Enum.IsDefined(proposed.RiskClass) ||
            !Enum.IsDefined(proposed.ConfirmationMode))
        {
            throw new ArgumentOutOfRangeException(
                nameof(proposed),
                "Mutation risk decisions must use supported risk and confirmation values.");
        }

        if (proposed.Reasons.Count > 32)
        {
            throw new ArgumentOutOfRangeException(
                nameof(proposed),
                "Mutation risk decisions must not contain more than 32 reasons.");
        }

        ArgumentNullException.ThrowIfNull(input);
        var floor = Classify(input);
        var effectiveRisk = (MutationRiskClass)Math.Max(
            (int)floor.RiskClass,
            (int)proposed.RiskClass);

        var effectiveConfirmation =
            (int)effectiveRisk >= (int)MutationRiskClass.High ||
            proposed.ConfirmationMode == MutationConfirmationMode.TypedTarget
                ? MutationConfirmationMode.TypedTarget
                : MutationConfirmationMode.Explicit;

        var reasons = floor.Reasons
            .Concat(proposed.Reasons)
            .Where(reason => !string.IsNullOrWhiteSpace(reason))
            .Select(reason => reason.Trim())
            .Distinct(StringComparer.Ordinal)
            .OrderBy(reason => reason, StringComparer.Ordinal)
            .ToArray();

        if (reasons.Any(reason => reason.Length > 512 || reason.Any(char.IsControl)))
        {
            throw new ArgumentOutOfRangeException(
                nameof(proposed),
                "Mutation risk reasons must be at most 512 characters and contain no control characters.");
        }

        return new MutationRiskDecision(
            effectiveRisk,
            Array.AsReadOnly(reasons),
            effectiveConfirmation,
            effectiveRisk == MutationRiskClass.Critical ||
            floor.RequiresIndependentApproval ||
            proposed.RequiresIndependentApproval);
    }

    private static MutationRiskClass BaseRisk(MutationOperationKind kind) => kind switch
    {
        MutationOperationKind.TopicCreate => MutationRiskClass.Low,
        MutationOperationKind.TopicAlter => MutationRiskClass.Moderate,
        MutationOperationKind.TopicIncreasePartitions => MutationRiskClass.High,
        MutationOperationKind.TopicDelete => MutationRiskClass.Critical,
        MutationOperationKind.RecordProduce => MutationRiskClass.Moderate,
        MutationOperationKind.ConsumerOffsetAlter => MutationRiskClass.High,
        MutationOperationKind.ConsumerDelete => MutationRiskClass.High,
        MutationOperationKind.SchemaCreate => MutationRiskClass.Moderate,
        MutationOperationKind.SchemaAlter => MutationRiskClass.High,
        MutationOperationKind.SchemaDelete => MutationRiskClass.High,
        MutationOperationKind.ConnectCreate => MutationRiskClass.Moderate,
        MutationOperationKind.ConnectAlter => MutationRiskClass.Moderate,
        MutationOperationKind.ConnectDelete => MutationRiskClass.High,
        MutationOperationKind.RecordsPurge => MutationRiskClass.Critical,
        MutationOperationKind.AclAlter => MutationRiskClass.High,
        MutationOperationKind.ScramAlter => MutationRiskClass.Critical,
        _ => throw new ArgumentOutOfRangeException(nameof(kind), kind, "Unsupported mutation operation kind."),
    };

    private static MutationRiskClass RaiseOne(MutationRiskClass value) => value switch
    {
        MutationRiskClass.Low => MutationRiskClass.Moderate,
        MutationRiskClass.Moderate => MutationRiskClass.High,
        MutationRiskClass.High => MutationRiskClass.Critical,
        MutationRiskClass.Critical => MutationRiskClass.Critical,
        _ => throw new ArgumentOutOfRangeException(nameof(value), value, "Unsupported mutation risk class."),
    };
}
