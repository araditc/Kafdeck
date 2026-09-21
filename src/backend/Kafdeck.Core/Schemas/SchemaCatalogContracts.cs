using Kafdeck.Core.ReadViews;
using Kafdeck.Core.Records;

namespace Kafdeck.Core.Schemas;

public enum SchemaCompatibilityMode
{
    Unknown = 0,
    None = 1,
    Backward = 2,
    BackwardTransitive = 3,
    Forward = 4,
    ForwardTransitive = 5,
    Full = 6,
    FullTransitive = 7,
}

public sealed record SchemaSubjectSummary(string Subject);

public sealed record SchemaVersionSummary(
    string Subject,
    int Version,
    int SchemaId,
    RecordSchemaFormat Format,
    IReadOnlyList<RecordSchemaReference> References);

public sealed record SchemaVersionDetail(
    string Subject,
    int Version,
    RecordSchemaDocument Schema);

public sealed record SchemaCompatibilityObservation(
    string Subject,
    SchemaCompatibilityMode Mode,
    bool IsInherited);

public interface ISchemaCatalogReadPort
{
    Task<ReadViewResult<IReadOnlyList<SchemaSubjectSummary>>> ListSubjectsAsync(
        string clusterId,
        ReadViewOperationContext operation,
        CancellationToken cancellationToken);

    Task<ReadViewResult<IReadOnlyList<SchemaVersionSummary>>> ListVersionsAsync(
        string clusterId,
        string subject,
        ReadViewOperationContext operation,
        CancellationToken cancellationToken);

    Task<ReadViewResult<SchemaVersionDetail>> GetVersionAsync(
        string clusterId,
        string subject,
        int version,
        ReadViewOperationContext operation,
        CancellationToken cancellationToken);

    Task<ReadViewResult<SchemaCompatibilityObservation>> GetCompatibilityAsync(
        string clusterId,
        string subject,
        ReadViewOperationContext operation,
        CancellationToken cancellationToken);
}
