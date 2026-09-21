using System.Globalization;
using Kafdeck.Modules.Administration;

namespace Kafdeck.Modules.Records;

public sealed class RecordProductionExecutionService
{
    private readonly IRecordProduceMutationPort _producer;

    public RecordProductionExecutionService(IRecordProduceMutationPort producer)
    {
        _producer = producer ?? throw new ArgumentNullException(nameof(producer));
    }

    public async Task<MutationProviderResult> ExecuteAsync(
        MutationExecutionContext context,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(context);
        if (context.Operation.OperationKind != MutationOperationKind.RecordProduce)
            return Unknown("record_produce_operation_mismatch", 0, 0, 0);

        RecordProductionCanonicalIntent canonical;
        try
        {
            canonical = RecordProductionPlanner.DeserializeCanonical(
                context.Operation.CanonicalIntent);
        }
        catch
        {
            return Unknown("record_produce_canonical_intent_invalid", 0, 0, 0);
        }

        if (canonical.Records.Count is < 1 or > RecordProductionPolicy.HardMaxRecords ||
            context.Operation.MaterialDigests.Count != canonical.Records.Count)
        {
            return Unknown("record_produce_canonical_intent_invalid", canonical.Records.Count, 0, 0);
        }

        var acknowledged = 0;
        for (var ordinal = 0; ordinal < canonical.Records.Count; ordinal++)
        {
            cancellationToken.ThrowIfCancellationRequested();
            var expected = canonical.Records[ordinal];
            if (expected.Ordinal != ordinal ||
                !string.Equals(expected.MaterialName, $"record/{ordinal:D4}", StringComparison.Ordinal))
            {
                return Unknown(
                    "record_produce_canonical_intent_invalid",
                    canonical.Records.Count,
                    acknowledged,
                    ordinal);
            }

            RecordProduceMutation mutation;
            try
            {
                var material = context.Material.GetRequired(expected.MaterialName);
                mutation = RecordProductionMaterialCodec.Decode(
                    canonical.ClusterId,
                    canonical.TopicName,
                    material,
                    expected);
            }
            catch
            {
                return Unknown(
                    "record_produce_material_invalid",
                    canonical.Records.Count,
                    acknowledged,
                    ordinal);
            }

            var result = await _producer
                .ProduceAsync(mutation, cancellationToken)
                .ConfigureAwait(false);

            if (result.ResultKind == MutationExecutionResultKind.AppliedVerified)
            {
                acknowledged++;
                if (ordinal == canonical.Records.Count - 1)
                {
                    var evidence = BaseEvidence(canonical.Records.Count, acknowledged);
                    if (canonical.Records.Count == 1 && result.SafeEvidence is not null)
                    {
                        CopyIfPresent(result.SafeEvidence, evidence, "partition");
                        CopyIfPresent(result.SafeEvidence, evidence, "offset");
                    }

                    return new MutationProviderResult(
                        MutationExecutionResultKind.AppliedVerified,
                        canonical.Records.Count == 1
                            ? "record_produce_acknowledged"
                            : "record_batch_acknowledged",
                        evidence);
                }

                continue;
            }

            if (result.ResultKind == MutationExecutionResultKind.FailedDefinitive)
            {
                if (acknowledged == 0)
                    return new MutationProviderResult(
                        MutationExecutionResultKind.FailedDefinitive,
                        "record_produce_rejected",
                        FailureEvidence(canonical.Records.Count, acknowledged, ordinal));

                return new MutationProviderResult(
                    MutationExecutionResultKind.PartiallyApplied,
                    "record_produce_partially_applied",
                    FailureEvidence(canonical.Records.Count, acknowledged, ordinal));
            }

            return Unknown(
                "record_produce_execution_unknown",
                canonical.Records.Count,
                acknowledged,
                ordinal);
        }

        return Unknown("record_produce_execution_unknown", canonical.Records.Count, acknowledged, acknowledged);
    }

    private static MutationProviderResult Unknown(
        string code,
        int recordCount,
        int acknowledged,
        int failureOrdinal) =>
        new(
            MutationExecutionResultKind.ExecutionUnknown,
            code,
            FailureEvidence(recordCount, acknowledged, failureOrdinal));

    private static Dictionary<string, string> BaseEvidence(
        int recordCount,
        int acknowledged) =>
        new(StringComparer.Ordinal)
        {
            ["record.count"] = recordCount.ToString(CultureInfo.InvariantCulture),
            ["acknowledged.count"] = acknowledged.ToString(CultureInfo.InvariantCulture),
            ["provider.accepted"] = (recordCount > 0 && acknowledged == recordCount)
                .ToString()
                .ToLowerInvariant(),
            ["verification.state"] = acknowledged == recordCount && recordCount > 0
                ? "observed"
                : "inconclusive",
        };

    private static Dictionary<string, string> FailureEvidence(
        int recordCount,
        int acknowledged,
        int ordinal)
    {
        var evidence = BaseEvidence(recordCount, acknowledged);
        evidence["failure.ordinal"] = Math.Max(0, ordinal)
            .ToString(CultureInfo.InvariantCulture);
        return evidence;
    }

    private static void CopyIfPresent(
        IReadOnlyDictionary<string, string> source,
        IDictionary<string, string> destination,
        string key)
    {
        if (source.TryGetValue(key, out var value))
            destination[key] = value;
    }
}

public sealed class RecordProduceExecutionHandler : IMutationExecutionHandler
{
    private readonly RecordProductionExecutionService _service;

    public RecordProduceExecutionHandler(RecordProductionExecutionService service)
    {
        _service = service ?? throw new ArgumentNullException(nameof(service));
    }

    public MutationOperationKind OperationKind => MutationOperationKind.RecordProduce;

    public Task<MutationProviderResult> ExecuteAsync(
        MutationExecutionContext context,
        CancellationToken cancellationToken = default) =>
        _service.ExecuteAsync(context, cancellationToken);
}
