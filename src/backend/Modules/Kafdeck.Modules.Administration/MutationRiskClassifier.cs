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
        if (input.TargetCount <= 0)
        {
            throw new ArgumentOutOfRangeException(nameof(input), "Mutation target count must be positive.");
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
