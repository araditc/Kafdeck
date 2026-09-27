using System.Text;
using System.Text.Json;
using Kafdeck.Core.Records;
using Kafdeck.Modules.Administration;
using Kafdeck.Modules.Schemas;

namespace Kafdeck.Modules.Generator;

public sealed record DataGeneratorMaterializedRecord(
    long RecordIndex,
    ReadOnlyMemory<byte>? Key,
    ReadOnlyMemory<byte> Value,
    IReadOnlyList<KafkaRecordHeader> Headers);

public sealed class DataGeneratorMaterializer
{
    private readonly SchemaDeveloperService _schemaTooling;

    public DataGeneratorMaterializer(
        SchemaDeveloperService schemaTooling)
    {
        _schemaTooling =
            schemaTooling ??
            throw new ArgumentNullException(
                nameof(schemaTooling));
    }

    public DataGeneratorMaterializedRecord Materialize(
        DataGeneratorPlan plan,
        long recordIndex,
        RecordSchemaDocument? schemaDocument = null,
        CancellationToken cancellationToken = default)
    {
        DataGeneratorPolicy.ValidatePlan(plan);

        if (recordIndex < 0 ||
            recordIndex >= plan.RecordCount)
        {
            throw new ArgumentOutOfRangeException(
                nameof(recordIndex));
        }

        byte[] value =
            plan.Source.Kind switch
            {
                DataGeneratorSourceKind.Schema =>
                    MaterializeSchema(
                        plan,
                        recordIndex,
                        schemaDocument ??
                        throw new MutationStateException(
                            "Schema-backed generation requires the revalidated schema document."),
                        cancellationToken),

                DataGeneratorSourceKind.BuiltInTemplate =>
                    MaterializeTemplate(
                        plan,
                        recordIndex),

                _ => throw new MutationStateException(
                    "Generator source kind is unsupported."),
            };

        if (value.LongLength >
            plan.Budget.MaxBatchBytes)
        {
            throw new MutationStateException(
                "Generated record exceeds the immutable batch-byte budget.");
        }

        return new DataGeneratorMaterializedRecord(
            recordIndex,
            Key: null,
            Value: value,
            Headers:
                Array.Empty<KafkaRecordHeader>());
    }

    public static int SeedForRecord(
        int seed,
        long recordIndex)
    {
        if (recordIndex < 0)
        {
            throw new ArgumentOutOfRangeException(
                nameof(recordIndex));
        }

        unchecked
        {
            var low =
                (int)(recordIndex & 0xffffffffL);
            var high =
                (int)((recordIndex >> 32) &
                      0xffffffffL);

            return
                ((seed * 397) ^ low) * 397 ^
                high;
        }
    }

    private byte[] MaterializeSchema(
        DataGeneratorPlan plan,
        long recordIndex,
        RecordSchemaDocument schema,
        CancellationToken cancellationToken)
    {
        var source =
            plan.Source.Schema ??
            throw new MutationStateException(
                "Generator schema source identity is missing.");

        if (schema.Id != source.SchemaId ||
            schema.Format != source.Format ||
            !string.Equals(
                DataGeneratorPolicy.FingerprintSchema(
                    schema),
                source.SchemaFingerprint,
                StringComparison.Ordinal))
        {
            throw new MutationStateException(
                "Revalidated schema document does not match the immutable generator plan.");
        }

        var result =
            _schemaTooling.GenerateMockFromDocument(
                source.Subject,
                source.Version,
                schema,
                count: 1,
                seed:
                    SeedForRecord(
                        plan.Seed,
                        recordIndex),
                cancellationToken);

        if (!result.IsSuccess ||
            result.Value is null ||
            result.Value.Examples.Count != 1)
        {
            throw new MutationStateException(
                $"Schema-backed generation is unavailable: {result.Failure?.Code ?? "unknown"}.");
        }

        return Encoding.UTF8.GetBytes(
            result.Value.Examples[0].Json);
    }

    private static byte[] MaterializeTemplate(
        DataGeneratorPlan plan,
        long recordIndex)
    {
        var source =
            plan.Source.Template ??
            throw new MutationStateException(
                "Generator template source identity is missing.");

        if (source.Template !=
                DataGeneratorBuiltInTemplate
                    .BasicJsonV1 ||
            source.Version != 1 ||
            !string.Equals(
                source.TemplateFingerprint,
                DataGeneratorPolicy
                    .BuiltInTemplateFingerprint(
                        source.Template,
                        source.Version),
                StringComparison.Ordinal))
        {
            throw new MutationStateException(
                "Generator template source is unsupported.");
        }

        var recordSeed =
            SeedForRecord(
                plan.Seed,
                recordIndex);

        return JsonSerializer.SerializeToUtf8Bytes(
            new
            {
                sequence = recordIndex,
                seed = recordSeed,
                value =
                    $"kafdeck-{recordSeed:x8}-{recordIndex}",
            });
    }
}
