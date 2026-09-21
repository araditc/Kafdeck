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
        MutationOperationKind operationKind,
        int targetCount,
        MutationRiskDecision proposed)
    {
        ArgumentNullException.ThrowIfNull(proposed);

        var floor = Classify(new MutationRiskInput(operationKind, targetCount));
        var effectiveRisk = (MutationRiskClass)Math.Max(
            (int)floor.RiskClass,
            (int)proposed.RiskClass);

        var effectiveConfirmation = effectiveRisk >= MutationRiskClass.High
            ? MutationConfirmationMode.TypedTarget
            : MutationConfirmationMode.Explicit;

        var reasons = floor.Reasons
            .Concat(proposed.Reasons ?? Array.Empty<string>())
            .Where(reason => !string.IsNullOrWhiteSpace(reason))
            .Select(reason => reason.Trim())
            .Distinct(StringComparer.Ordinal)
            .OrderBy(reason => reason, StringComparer.Ordinal)
            .ToArray();

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
