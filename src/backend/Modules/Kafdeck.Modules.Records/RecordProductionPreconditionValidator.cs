using System.Security.Cryptography;
using System.Text;
using Kafdeck.Modules.Administration;

namespace Kafdeck.Modules.Records;

/// <summary>
/// Rebuilds request-scoped record-production execution material from bytes
/// re-submitted after preview/confirmation. The resulting envelope shape is
/// checked against the safe canonical preview; the W32 executor independently
/// enforces the persisted HMAC material digests before any provider dispatch.
/// </summary>
public static class RecordProductionExecutionMaterialBuilder
{
    public static RecordProductionExecutionMaterial Build(
        MutationOperationSnapshot operation,
        IReadOnlyList<RecordProductionRecordInput> records)
    {
        ArgumentNullException.ThrowIfNull(operation);
        if (operation.OperationKind != MutationOperationKind.RecordProduce)
        {
            throw Invalid();
        }

        RecordProductionCanonicalIntent canonical;
        try
        {
            canonical = RecordProductionPlanner.DeserializeCanonical(
                operation.CanonicalIntent);
        }
        catch
        {
            throw Invalid();
        }

        return Build(canonical, records);
    }

    public static RecordProductionExecutionMaterial Build(
        RecordProductionCanonicalIntent canonical,
        IReadOnlyList<RecordProductionRecordInput> records)
    {
        ArgumentNullException.ThrowIfNull(canonical);
        ArgumentNullException.ThrowIfNull(records);

        if (canonical.Records is null ||
            canonical.Records.Count is < 1 or > RecordProductionPolicy.HardMaxRecords ||
            records.Count != canonical.Records.Count)
        {
            throw Invalid();
        }

        var items = new Dictionary<string, byte[]>(records.Count, StringComparer.Ordinal);
        long totalKeyBytes = 0;
        long totalValueBytes = 0;
        long totalHeaderBytes = 0;
        long totalExecutionMaterialBytes = 0;

        try
        {
            for (var ordinal = 0; ordinal < records.Count; ordinal++)
            {
                var input = records[ordinal] ?? throw Invalid();
                var expected = canonical.Records[ordinal] ?? throw Invalid();
                if (expected.Ordinal != ordinal ||
                    !string.Equals(
                        expected.MaterialName,
                        $"record/{ordinal:D4}",
                        StringComparison.Ordinal) ||
                    input.Headers is null ||
                    input.Headers.Count != expected.Headers.Count ||
                    input.Headers.Count > RecordProductionPolicy.HardMaxHeadersPerRecord)
                {
                    throw Invalid();
                }

                if ((input.Key is null) != !expected.KeyBytes.HasValue ||
                    (input.Key?.Length ?? 0) != (expected.KeyBytes ?? 0) ||
                    (input.Key?.Length ?? 0) > RecordProductionPolicy.HardMaxKeyBytes ||
                    input.Value.Length != expected.ValueBytes ||
                    input.Value.Length > RecordProductionPolicy.HardMaxValueBytes)
                {
                    throw Invalid();
                }

                totalKeyBytes = checked(totalKeyBytes + (input.Key?.Length ?? 0));
                totalValueBytes = checked(totalValueBytes + input.Value.Length);

                var normalizedHeaders = new List<KeyValuePair<string, ReadOnlyMemory<byte>>>(
                    input.Headers.Count);
                var names = new HashSet<string>(StringComparer.Ordinal);
                foreach (var header in input.Headers)
                {
                    if (header is null ||
                        header.Value.Length > RecordProductionPolicy.HardMaxHeaderValueBytes)
                    {
                        throw Invalid();
                    }

                    var name = RecordProductionValidation.RequireIdentifier(
                        header.Name,
                        "Kafka header name",
                        RecordProductionPolicy.HardMaxHeaderNameCharacters);
                    if (!names.Add(name))
                    {
                        throw Invalid();
                    }

                    normalizedHeaders.Add(
                        new KeyValuePair<string, ReadOnlyMemory<byte>>(
                            name,
                            header.Value));
                }

                normalizedHeaders.Sort(
                    static (left, right) =>
                        StringComparer.Ordinal.Compare(left.Key, right.Key));

                for (var index = 0; index < normalizedHeaders.Count; index++)
                {
                    var actual = normalizedHeaders[index];
                    var expectedHeader = expected.Headers[index];
                    if (!string.Equals(
                            actual.Key,
                            expectedHeader.Name,
                            StringComparison.Ordinal) ||
                        actual.Value.Length != expectedHeader.ValueBytes)
                    {
                        throw Invalid();
                    }

                    totalHeaderBytes = checked(
                        totalHeaderBytes +
                        Encoding.UTF8.GetByteCount(actual.Key) +
                        actual.Value.Length);
                }

                var envelope = RecordProductionMaterialCodec.Encode(
                    input.Key,
                    input.Value,
                    normalizedHeaders);
                totalExecutionMaterialBytes = checked(
                    totalExecutionMaterialBytes + envelope.LongLength);
                if (totalExecutionMaterialBytes >
                    RecordProductionPolicy.HardMaxExecutionMaterialBytes)
                {
                    CryptographicOperations.ZeroMemory(envelope);
                    throw Invalid();
                }

                if (!items.TryAdd(expected.MaterialName, envelope))
                {
                    CryptographicOperations.ZeroMemory(envelope);
                    throw Invalid();
                }
            }

            var totalBytes = checked(totalKeyBytes + totalValueBytes + totalHeaderBytes);
            if (totalBytes != canonical.TotalBytes)
            {
                throw Invalid();
            }

            return new RecordProductionExecutionMaterial(items);
        }
        catch
        {
            foreach (var value in items.Values)
            {
                CryptographicOperations.ZeroMemory(value);
            }

            items.Clear();
            throw;
        }
    }

    private static MutationStateException Invalid() =>
        new("Record production execution material does not match the admitted preview shape.");
}
