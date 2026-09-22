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

public sealed record SchemaGlobalCompatibilityObservation(
    SchemaCompatibilityMode Mode);

public sealed record SchemaMutationCapabilities(
    bool SupportsCompatibilityValidation,
    bool SupportsRegistration,
    bool SupportsCompatibilityMutation,
    bool SupportsSoftDelete,
    bool SupportsPermanentDelete);

public enum SchemaMutationObservationFailureCategory
{
    NotConfigured = 1,
    Unauthorized = 2,
    Unsupported = 3,
    Unavailable = 4,
    Timeout = 5,
    Cancelled = 6,
    InvalidRequest = 7,
    InvalidResponse = 8,
}

public sealed record SchemaMutationObservationFailure(
    SchemaMutationObservationFailureCategory Category,
    string Code,
    string SafeMessage,
    bool IsRetryable);

public sealed record SchemaCompatibilityCheckRequest(
    string Subject,
    RecordSchemaFormat Format,
    string Schema,
    IReadOnlyList<RecordSchemaReference> References);

public sealed record SchemaCompatibilityCheckObservation(
    bool IsCompatible,
    string ResultCode);

public sealed record SchemaDeleteObservationRequest(
    string Subject,
    int? Version);

public sealed record SchemaDeleteTargetObservation(
    bool ExistsActive,
    bool ExistsIncludingDeleted,
    bool IsSoftDeleted,
    IReadOnlyList<int> ActiveVersions,
    IReadOnlyList<int> VersionsIncludingDeleted);

public sealed record SchemaMutationObservationResult<T>
{
    private SchemaMutationObservationResult(
        T? value,
        SchemaMutationObservationFailure? failure)
    {
        Value = value;
        Failure = failure;
    }

    public T? Value { get; }
    public SchemaMutationObservationFailure? Failure { get; }
    public bool IsSuccess => Failure is null;

    public static SchemaMutationObservationResult<T> Success(T value) =>
        new(value, null);

    public static SchemaMutationObservationResult<T> Failed(
        SchemaMutationObservationFailure failure) =>
        new(default, failure ?? throw new ArgumentNullException(nameof(failure)));
}

public interface ISchemaMutationObservationPort
{
    Task<SchemaMutationObservationResult<SchemaMutationCapabilities>>
        GetCapabilitiesAsync(
            string clusterId,
            ReadViewOperationContext operation,
            CancellationToken cancellationToken);

    Task<SchemaMutationObservationResult<SchemaCompatibilityCheckObservation>>
        TestCompatibilityAsync(
            string clusterId,
            SchemaCompatibilityCheckRequest request,
            ReadViewOperationContext operation,
            CancellationToken cancellationToken);

    Task<SchemaMutationObservationResult<SchemaDeleteTargetObservation>>
        ObserveDeleteTargetAsync(
            string clusterId,
            SchemaDeleteObservationRequest request,
            ReadViewOperationContext operation,
            CancellationToken cancellationToken);
}

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

    Task<ReadViewResult<SchemaGlobalCompatibilityObservation>>
        GetGlobalCompatibilityAsync(
            string clusterId,
            ReadViewOperationContext operation,
            CancellationToken cancellationToken);
}
