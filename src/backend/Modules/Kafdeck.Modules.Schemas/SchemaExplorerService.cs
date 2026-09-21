using Kafdeck.Core.ReadViews;
using Kafdeck.Core.Schemas;

namespace Kafdeck.Modules.Schemas;

public sealed record SchemaExplorerPolicy(
    TimeSpan OperationTimeout,
    int MaxItems,
    long MaxResponseBytes)
{
    public static SchemaExplorerPolicy Default { get; } =
        new(TimeSpan.FromSeconds(10), 500, 4 * 1024 * 1024);
}

public sealed class SchemaExplorerService
{
    private readonly ISchemaCatalogReadPort _schemas;
    private readonly SchemaDiffService _diff;
    private readonly SchemaExplorerPolicy _policy;
    private readonly TimeProvider _timeProvider;

    public SchemaExplorerService(
        ISchemaCatalogReadPort schemas,
        SchemaDiffService diff,
        SchemaExplorerPolicy? policy = null,
        TimeProvider? timeProvider = null)
    {
        _schemas = schemas ?? throw new ArgumentNullException(nameof(schemas));
        _diff = diff ?? throw new ArgumentNullException(nameof(diff));
        _policy = policy ?? SchemaExplorerPolicy.Default;
        _timeProvider = timeProvider ?? TimeProvider.System;

        _ = new ReadViewOperationContext(
            _timeProvider.GetUtcNow().Add(_policy.OperationTimeout),
            _policy.MaxItems,
            _policy.MaxResponseBytes);
    }

    public Task<ReadViewResult<IReadOnlyList<SchemaSubjectSummary>>> ListSubjectsAsync(
        string clusterId,
        CancellationToken cancellationToken = default) =>
        _schemas.ListSubjectsAsync(clusterId, Operation(), cancellationToken);

    public Task<ReadViewResult<IReadOnlyList<SchemaVersionSummary>>> ListVersionsAsync(
        string clusterId,
        string subject,
        CancellationToken cancellationToken = default) =>
        _schemas.ListVersionsAsync(clusterId, subject, Operation(), cancellationToken);

    public Task<ReadViewResult<SchemaVersionDetail>> GetVersionAsync(
        string clusterId,
        string subject,
        int version,
        CancellationToken cancellationToken = default) =>
        _schemas.GetVersionAsync(clusterId, subject, version, Operation(), cancellationToken);

    public Task<ReadViewResult<SchemaCompatibilityObservation>> GetCompatibilityAsync(
        string clusterId,
        string subject,
        CancellationToken cancellationToken = default) =>
        _schemas.GetCompatibilityAsync(clusterId, subject, Operation(), cancellationToken);

    public async Task<ReadViewResult<SchemaDiffResult>> DiffAsync(
        string clusterId,
        string subject,
        int leftVersion,
        int rightVersion,
        CancellationToken cancellationToken = default)
    {
        var left = await GetVersionAsync(clusterId, subject, leftVersion, cancellationToken)
            .ConfigureAwait(false);
        if (!left.IsSuccess || left.Value is null)
        {
            return ReadViewResult<SchemaDiffResult>.Failed(left.Failure!);
        }

        var right = await GetVersionAsync(clusterId, subject, rightVersion, cancellationToken)
            .ConfigureAwait(false);
        if (!right.IsSuccess || right.Value is null)
        {
            return ReadViewResult<SchemaDiffResult>.Failed(right.Failure!);
        }

        try
        {
            return ReadViewResult<SchemaDiffResult>.Success(
                _diff.Compare(left.Value.Schema, right.Value.Schema));
        }
        catch (ArgumentOutOfRangeException)
        {
            return ReadViewResult<SchemaDiffResult>.Failed(
                new ReadViewFailure(
                    ReadViewFailureCategory.ResponseTooLarge,
                    "schema_diff_bound_exceeded",
                    "Schema diff input exceeded the configured bound.",
                    false));
        }
    }

    private ReadViewOperationContext Operation() =>
        new(
            _timeProvider.GetUtcNow().Add(_policy.OperationTimeout),
            _policy.MaxItems,
            _policy.MaxResponseBytes);
}
